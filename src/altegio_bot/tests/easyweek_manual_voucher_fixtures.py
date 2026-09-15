"""Synthetic payloads and fakes for the §37.2 manual-basis canary tests.

Every identity here is obviously fabricated, and the voucher code is a sentinel
whose whole job is to be searched for: no production customer, phone number,
order, message id or artifact value belongs in this repository.
"""

from __future__ import annotations

import itertools
import uuid as uuid_module
from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy import select

from altegio_bot.campaigns.easyweek_manual_voucher.identity import (
    KARLSRUHE_COMPANY_ID,
    NEW_CLIENT_CAMPAIGN_CODE,
    VOUCHER_TEMPLATE_CODE,
)
from altegio_bot.campaigns.easyweek_manual_voucher.runner import ManualCanaryRequest
from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract
from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import (
    DELIVERY_ACCEPTED,
    DELIVERY_REJECTED,
    DELIVERY_UNKNOWN,
    DeliveryOutcome,
)
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    SUPPORTED_VOUCHER_PRICE_MINOR,
)
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_MANUAL,
    CampaignRecipient,
    CampaignRun,
    Client,
    MessageTemplate,
    WhatsAppSender,
)
from altegio_bot.settings import settings

COMPANY_ID = KARLSRUHE_COMPANY_ID
EW_CUSTOMER_UUID = uuid_module.UUID("a1a1a1a1-b2b2-4c3c-8d4d-e5e5e5e5e5e5")
OTHER_CUSTOMER_UUID = uuid_module.UUID("f6f6f6f6-b2b2-4c3c-8d4d-e5e5e5e5e5e5")
ORDER_UUID = uuid_module.UUID("0d0d0d0d-1111-4222-8333-444444444444")
OTHER_ORDER_UUID = uuid_module.UUID("0e0e0e0e-1111-4222-8333-444444444444")
STAFFER_UUID = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"
ACCOUNT_UUID = "cccccccc-3333-4333-8333-cccccccccccc"
PHONE = "+4915112345678"
CUSTOMER_NAME = "Synthetic Fixture"
_LOCAL_CLIENT_IDS = itertools.count(900042)


def _next_local_client_id() -> int:
    """A fresh synthetic local id per NEW client row."""
    return next(_LOCAL_CLIENT_IDS)


SENDER_PHONE_NUMBER_ID = "SYNTHETIC_PHONE_NUMBER_ID"
PROVIDER_MESSAGE_ID = "wamid.SYNTHETIC0000000042"

# The one string every secrecy test hunts for.
VOUCHER_CODE_SENTINEL = "SENTINEL-MANUAL-CODE-qqq777"

# The August wave: over, in the past, and the same whenever the suite runs.
NOW = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)
PERIOD_START = datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 8, 31, 23, 59, 59, tzinfo=timezone.utc)


@pytest.fixture
def manual_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    """The fence open and both identities configured — the acting case."""
    monkeypatch.setattr(settings, "easyweek_manual_voucher_canary_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_manual_voucher_staffer_uuid", STAFFER_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_manual_voucher_account_uuid", ACCOUNT_UUID, raising=False)


def manual_request(*, run_id: int, recipient_id: int) -> ManualCanaryRequest:
    return ManualCanaryRequest(
        preview_run_id=run_id,
        campaign_recipient_id=recipient_id,
        sender_code="default",
        staffer_uuid=STAFFER_UUID,
        payment_account_uuid=ACCOUNT_UUID,
    )


def customer_payload(**changes: Any) -> dict[str, Any]:
    """What ``GET /customers/{uuid}`` answers for our synthetic person."""
    payload: dict[str, Any] = {
        "uuid": str(EW_CUSTOMER_UUID),
        "phone": PHONE,
        "first_name": CUSTOMER_NAME,
    }
    payload.update(changes)
    return payload


def template_payload(*, services: int = 42, all_services: int = 42, **changes: Any) -> dict[str, Any]:
    """The voucher template as the §37.2 baseline expects to read it."""
    payload: dict[str, Any] = {
        "uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID,
        "is_enabled": True,
        "is_online": False,
        "is_single_charge": True,
        "cost": SUPPORTED_VOUCHER_PRICE_MINOR,
        "value": SUPPORTED_VOUCHER_PRICE_MINOR,
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


def issued_voucher(*, code: str = VOUCHER_CODE_SENTINEL, **changes: Any) -> dict[str, Any]:
    """The singleton artifact shape production actually returned: no quantity."""
    voucher: dict[str, Any] = {
        "code": code,
        "voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID,
        "value": SUPPORTED_VOUCHER_PRICE_MINOR,
        "price": SUPPORTED_VOUCHER_PRICE_MINOR,
    }
    voucher.update(changes)
    return voucher


def voucher_order(
    *,
    marker: str,
    status: str = "open",
    order_uuid: uuid_module.UUID = ORDER_UUID,
    vouchers: list[dict[str, Any]] | None = None,
    **changes: Any,
) -> dict[str, Any]:
    order: dict[str, Any] = {
        "uuid": str(order_uuid),
        "comment": marker,
        "customer": {"uuid": str(EW_CUSTOMER_UUID)},
        "status": status,
        "is_paid": status in ("paid", "refunded"),
        "is_reverted": status == "refunded",
        "total": SUPPORTED_VOUCHER_PRICE_MINOR,
        "subtotal": SUPPORTED_VOUCHER_PRICE_MINOR,
        "services": [],
        "goods": [],
        "vouchers": [issued_voucher()] if vouchers is None else vouchers,
        "created_at": NOW.isoformat(),
    }
    order.update(changes)
    return order


class FakeReader:
    """The EasyWeek read surface this canary uses, and nothing else.

    ``get_order`` and ``get_voucher_template`` answer from dictionaries the test
    controls, so an out-of-order or failing read is expressed by what the test
    puts in them rather than by patching the runner.
    """

    def __init__(
        self,
        *,
        customer: dict[str, Any] | Exception | None = None,
        orders: dict[str, Any] | None = None,
        template: dict[str, Any] | Exception | None = None,
    ) -> None:
        self.customer = customer if customer is not None else customer_payload()
        self.orders: dict[str, Any] = orders or {}
        self.template = template if template is not None else template_payload()
        self.customer_calls: list[str] = []
        self.order_calls: list[str] = []

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        self.customer_calls.append(customer_uuid)
        if isinstance(self.customer, Exception):
            raise self.customer
        return self.customer

    async def get_order(self, order_uuid: str) -> dict[str, Any]:
        self.order_calls.append(order_uuid)
        answer = self.orders.get(order_uuid)
        if isinstance(answer, Exception):
            raise answer
        if answer is None:
            raise KeyError(order_uuid)
        return answer

    async def get_voucher_template(self, template_uuid: str) -> dict[str, Any]:
        if isinstance(self.template, Exception):
            raise self.template
        return self.template

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:  # pragma: no cover - unused here
        raise AssertionError("the manual canary never reads a booking")

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 100) -> dict[str, Any]:
        # A manual basis proves no history, so nothing may walk one.
        raise AssertionError("the manual canary never walks a customer's history")


class FakeMutator:
    """The three EasyWeek writes, recorded rather than performed."""

    def __init__(
        self,
        *,
        create: Any = None,
        pay: Any = None,
        refund: Any = None,
    ) -> None:
        self.create = create
        self.pay = pay
        self.refund = refund
        self.calls: list[str] = []

    async def create_voucher_order(self, **kwargs: Any) -> Any:
        self.calls.append("create")
        if isinstance(self.create, Exception):
            raise self.create
        return self.create

    async def pay_voucher_order(self, **kwargs: Any) -> Any:
        self.calls.append("pay")
        if isinstance(self.pay, Exception):
            raise self.pay
        return self.pay

    async def refund_voucher_order(self, **kwargs: Any) -> Any:
        self.calls.append("refund")
        if isinstance(self.refund, Exception):
            raise self.refund
        return self.refund


class FakeSender:
    """The one Meta call, recorded. Records that params ARRIVED, never stores them."""

    def __init__(self, outcome: DeliveryOutcome | None = None) -> None:
        self.outcome = outcome or DeliveryOutcome(outcome=DELIVERY_ACCEPTED, provider_message_id=PROVIDER_MESSAGE_ID)
        self.calls = 0
        self.saw_code = False

    async def send_voucher_template(self, *, params: list[str], **kwargs: Any) -> DeliveryOutcome:
        self.calls += 1
        self.saw_code = VOUCHER_CODE_SENTINEL in params
        return self.outcome


def unknown_outcome(reason: str = "timeout") -> DeliveryOutcome:
    return DeliveryOutcome(outcome=DELIVERY_UNKNOWN, reason=reason)


def rejected_outcome(reason: str = "invalid_parameter") -> DeliveryOutcome:
    return DeliveryOutcome(outcome=DELIVERY_REJECTED, reason=reason, http_status=400)


async def seed_manual_recipient(
    session_maker,
    *,
    company_id: int = COMPANY_ID,
    campaign_code: str = NEW_CLIENT_CAMPAIGN_CODE,
    run_status: str = "completed",
    run_mode: str = "preview",
    recipient_status: str = "candidate",
    recipient_basis: str = RECIPIENT_BASIS_MANUAL,
    customer_uuid: uuid_module.UUID | None = EW_CUSTOMER_UUID,
    phone: str = PHONE,
    client_phone: str | None = None,
    opted_out: bool = False,
    period_start: datetime = PERIOD_START,
    period_end: datetime = PERIOD_END,
) -> tuple[int, int]:
    """One completed preview with one manually selected candidate. Returns ids."""
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                provider=PROVIDER_EASYWEEK,
                campaign_code=campaign_code,
                mode=run_mode,
                company_ids=[company_id],
                period_start=period_start,
                period_end=period_end,
                status=run_status,
            )
            session.add(run)
            # The same human being can appear in two previews — that is exactly
            # the mistake the entitlement rule exists to catch — so an existing
            # local client is REUSED rather than duplicated, as production does.
            number = client_phone if client_phone is not None else phone
            client = (
                await session.execute(
                    select(Client)
                    .where(Client.provider == PROVIDER_EASYWEEK)
                    .where(Client.company_id == company_id)
                    .where(Client.phone_e164 == number)
                )
            ).scalar_one_or_none()
            if client is None:
                client = Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=company_id,
                    # Not-null in the shared table. An EasyWeek client carries a
                    # synthetic local id here; the canary never reads it.
                    altegio_client_id=_next_local_client_id(),
                    phone_e164=number,
                    display_name=CUSTOMER_NAME,
                    raw={},
                    wa_opted_out=opted_out,
                )
                session.add(client)
            else:
                client.wa_opted_out = opted_out
            await session.flush()
            recipient = CampaignRecipient(
                campaign_run_id=run.id,
                provider=PROVIDER_EASYWEEK,
                company_id=company_id,
                client_id=client.id,
                phone_e164=phone,
                display_name=CUSTOMER_NAME,
                status=recipient_status,
                recipient_basis=recipient_basis,
                easyweek_customer_uuid=customer_uuid,
                is_opted_out=opted_out,
            )
            session.add(recipient)
            await session.flush()
            return run.id, recipient.id


async def seed_template_and_sender(session_maker, *, company_id: int = COMPANY_ID) -> None:
    """The approved Meta template row and an active sender line."""
    async with session_maker() as session:
        async with session.begin():
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


__all__ = [
    "ACCOUNT_UUID",
    "COMPANY_ID",
    "EW_CUSTOMER_UUID",
    "NOW",
    "ORDER_UUID",
    "OTHER_CUSTOMER_UUID",
    "PERIOD_END",
    "PERIOD_START",
    "PHONE",
    "PROVIDER_MESSAGE_ID",
    "STAFFER_UUID",
    "VOUCHER_CODE_SENTINEL",
    "FakeMutator",
    "FakeReader",
    "FakeSender",
    "manual_configuration",
    "customer_payload",
    "issued_voucher",
    "manual_request",
    "rejected_outcome",
    "seed_manual_recipient",
    "seed_template_and_sender",
    "template_payload",
    "unknown_outcome",
    "voucher_order",
]
