"""Synthetic payloads and fakes for the §36 voucher delivery canary tests.

Every identity here is obviously fabricated, and the voucher code is a sentinel
whose whole job is to be searched for: no production customer, phone number,
order, message id or artifact value belongs in this repository.
"""

from __future__ import annotations

import json
import uuid as uuid_module
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract
from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import (
    DELIVERY_ACCEPTED,
    DELIVERY_UNKNOWN,
    DeliveryOutcome,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.identity import (
    NEW_CLIENT_CAMPAIGN_CODE,
    VOUCHER_TEMPLATE_CODE,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.runner import CanaryRequest
from altegio_bot.easyweek_locations import EasyWeekLocation, EasyWeekLocationRegistry
from altegio_bot.easyweek_service_category import (
    record_raw_with_service_category,
    record_raw_with_services_count,
)
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    KARLSRUHE_LOCATION_UUID,
    SUPPORTED_VOUCHER_PRICE_MINOR,
)
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    CampaignRecipient,
    CampaignRun,
    Client,
    EasyWeekEvent,
    MessageTemplate,
    Record,
    WhatsAppSender,
)
from altegio_bot.settings import settings

# -- synthetic identity ------------------------------------------------------
COMPANY_ID = 322579
BOOKING_ID = 900001
CUSTOMER_ID = 900002
BOOKING_UUID = uuid_module.UUID("11111111-2222-4333-8444-555555555555")
EW_CUSTOMER_UUID = uuid_module.UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
ORDER_UUID = uuid_module.UUID("dddddddd-4444-4444-8444-dddddddddddd")
OTHER_UUID = uuid_module.UUID("99999999-8888-4777-8666-555555555555")
STAFFER_UUID = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"
ACCOUNT_UUID = "cccccccc-3333-4333-8333-cccccccccccc"
PHONE = "+4915112345678"
SENDER_PHONE_NUMBER_ID = "SYNTHETIC_PHONE_NUMBER_ID"
PROVIDER_MESSAGE_ID = "wamid.SYNTHETIC0000000001"
BOOKING_LINK = "https://karlsruhe.example.invalid/"

# The one string every secrecy test hunts for.
VOUCHER_CODE_SENTINEL = "SENTINEL-VOUCHER-CODE-zzz999"

# The fixed business context: a first visit that is finished, inside a campaign
# period that is over. All of it sits in the past and stays there, so it means
# the same thing whenever the suite runs.
#
# It is NOT a substitute for the create window. That window is anchored on the
# real ``utcnow()`` at claim time, so a listing row that has to fall inside it
# must be built from the window the ledger actually recorded — see
# ``marker_order`` — and never from this constant.
NOW = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)
PERIOD_START = NOW - timedelta(days=40)
PERIOD_END = NOW - timedelta(days=1)
VISIT_START = NOW - timedelta(days=20)


def registry() -> EasyWeekLocationRegistry:
    return EasyWeekLocationRegistry(
        configured=True,
        valid=True,
        locations={
            COMPANY_ID: EasyWeekLocation(
                name="karlsruhe",
                location_id=COMPANY_ID,
                location_uuid=KARLSRUHE_LOCATION_UUID,
                meta_template_prefix="ka",
                booking_page_url=BOOKING_LINK,
            )
        },
    )


@pytest.fixture
def configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    """A fully configured canary environment, fence OFF by default."""
    raw = {
        location.name: {
            "location_id": location.location_id,
            "location_uuid": location.location_uuid,
            "meta_template_prefix": location.meta_template_prefix,
            "booking_page_url": location.booking_page_url,
        }
        for location in registry().locations.values()
    }
    monkeypatch.setattr(settings, "easyweek_location_map", json.dumps(raw), raising=False)
    monkeypatch.setattr(
        settings,
        "easyweek_allowed_service_categories",
        json.dumps(["Wimpernverlängerung"]),
        raising=False,
    )
    monkeypatch.setattr(settings, "easyweek_voucher_canary_staffer_uuid", STAFFER_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_canary_account_uuid", ACCOUNT_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_canary_enabled", False, raising=False)


@pytest.fixture
def binding_key(monkeypatch: pytest.MonkeyPatch) -> None:
    from pydantic import SecretStr

    monkeypatch.setattr(
        settings,
        "easyweek_voucher_delivery_hmac_key",
        SecretStr("k" * 48),
        raising=False,
    )
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key_id", "test-key-1", raising=False)


def canary_request(*, run_id: int, recipient_id: int) -> CanaryRequest:
    return CanaryRequest(
        preview_run_id=run_id,
        campaign_recipient_id=recipient_id,
        company_id=COMPANY_ID,
        sender_code="default",
        staffer_uuid=STAFFER_UUID,
        payment_account_uuid=ACCOUNT_UUID,
    )


# ---------------------------------------------------------------------------
# EasyWeek payloads
# ---------------------------------------------------------------------------


def booking_payload(**changes: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "uuid": str(BOOKING_UUID),
        "location_uuid": KARLSRUHE_LOCATION_UUID,
        "customer": {"uuid": str(EW_CUSTOMER_UUID)},
        "is_canceled": False,
        "is_completed": True,
        "start_time": VISIT_START.isoformat(),
        "ordered_services": [{}],
    }
    value.update(changes)
    return value


def history_page(rows: list[dict[str, Any]], *, current: int = 1, last: int = 1) -> dict[str, Any]:
    return {
        "data": rows,
        "meta": {"current_page": current, "last_page": last, "per_page": 100, "total": len(rows)},
    }


def issued_voucher(*, code: str = VOUCHER_CODE_SENTINEL, **changes: Any) -> dict[str, Any]:
    """One issued voucher in the shape production actually returned."""
    value = {
        "code": code,
        "voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID,
        "value": SUPPORTED_VOUCHER_PRICE_MINOR,
        "price": SUPPORTED_VOUCHER_PRICE_MINOR,
    }
    value.update(changes)
    return value


def voucher_order(*, marker: str, status: str = "open", **changes: Any) -> dict[str, Any]:
    order: dict[str, Any] = {
        "uuid": str(ORDER_UUID),
        "status": status,
        "is_paid": status == "paid",
        "is_reverted": status == "refunded",
        "created_at": NOW.isoformat(),
        "comment": marker,
        "customer": {"uuid": str(EW_CUSTOMER_UUID)},
        "vouchers": [issued_voucher()],
        "goods": [],
        "services": [],
        "subtotal": SUPPORTED_VOUCHER_PRICE_MINOR,
    }
    if status == "paid":
        order["invoice"] = {"total": SUPPORTED_VOUCHER_PRICE_MINOR, "amount_due": 0, "amount_paid": 1500}
    order.update(changes)
    return order


async def create_window(session_maker: Any) -> tuple[datetime, datetime]:
    """The create window this canary actually proved when it claimed its create.

    Read back from the ledger rather than recomputed: the production code
    anchors the window on the real ``utcnow()`` of the claim, and any test that
    guesses the moment instead of asking is a time bomb waiting for the clock.
    """
    from altegio_bot.campaigns.easyweek_voucher_delivery import ledger as ledger_module

    snapshot = await ledger_module.load(session_maker)
    start, end = snapshot.create_window_start, snapshot.create_window_end
    if start is None or end is None:
        raise AssertionError("the ledger has no proven create window to place a listing row in")
    return start, end


async def marker_order(
    session_maker: Any,
    *,
    marker: str,
    status: str = "open",
    at: datetime | None = None,
    **changes: Any,
) -> dict[str, Any]:
    """A listing row the marker walk can see, built from the proven window.

    ``at`` places the row at an exact moment — that is how the boundary tests
    put an order just outside the window. By default the row lands in the middle
    of the window, far from either edge, so the walk recognises it no matter
    what the wall clock says today.
    """
    start, end = await create_window(session_maker)
    moment = at if at is not None else start + (end - start) / 2
    return voucher_order(marker=marker, status=status, created_at=moment.isoformat(), **changes)


def orders_page(rows: list[dict[str, Any]], *, page: int = 1, last_page: int = 1) -> dict[str, Any]:
    return {
        "data": rows,
        "meta": {"current_page": page, "last_page": last_page, "per_page": 100, "total": len(rows)},
    }


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeEasyWeekReader:
    """Booking history plus the POS reads, all synthetic."""

    def __init__(self, *, order: dict[str, Any] | None = None, marker: str = "") -> None:
        self.order = order
        self.marker = marker
        self.booking = booking_payload()
        self.customer = {"uuid": str(EW_CUSTOMER_UUID)}
        self.pages = {1: history_page([booking_payload()])}
        self.order_pages: list[dict[str, Any]] = [orders_page([])]
        self.calls: list[str] = []
        self.order_error: Exception | None = None

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        self.calls.append("get_booking")
        if isinstance(self.booking, Exception):
            raise self.booking
        return self.booking

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        self.calls.append("get_customer")
        return self.customer

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 100) -> dict[str, Any]:
        self.calls.append(f"history:{page}")
        return self.pages[page]

    async def get_order(self, order_uuid: str) -> dict[str, Any]:
        self.calls.append("get_order")
        if self.order_error is not None:
            raise self.order_error
        if self.order is None:
            from altegio_bot.easyweek_client import EasyWeekNotFoundError

            raise EasyWeekNotFoundError("missing", status_code=404)
        return self.order

    async def list_location_customer_orders(
        self, *, location_uuid: str, customer_uuid: str, page: int, per_page: int = 100
    ) -> dict[str, Any]:
        self.calls.append(f"list_orders:{page}")
        index = page - 1
        return self.order_pages[index] if index < len(self.order_pages) else orders_page([])


class FakeMutator:
    """Records every mutation and applies the effect a real API would."""

    def __init__(
        self,
        reader: FakeEasyWeekReader,
        *,
        marker: str = "",
        create_error: Exception | None = None,
        pay_error: Exception | None = None,
        refund_error: Exception | None = None,
        create_effect: bool = True,
    ) -> None:
        self.reader = reader
        self.marker = marker
        self.create_error = create_error
        self.pay_error = pay_error
        self.refund_error = refund_error
        self.create_effect = create_effect
        self.calls: list[str] = []
        self.create_kwargs: list[dict[str, Any]] = []
        self.pay_kwargs: list[dict[str, Any]] = []

    async def create_voucher_order(self, **kwargs: Any):
        from altegio_bot.easyweek_voucher_mutation import VoucherMutationResponse

        self.calls.append("create")
        self.create_kwargs.append(dict(kwargs))
        if self.create_error is not None:
            raise self.create_error
        body = voucher_order(marker=kwargs["marker"])
        if self.create_effect:
            self.reader.order = body
        return VoucherMutationResponse(http_status=200, envelope=body)

    async def pay_voucher_order(self, **kwargs: Any):
        from altegio_bot.easyweek_voucher_mutation import VoucherMutationResponse

        self.calls.append("pay")
        self.pay_kwargs.append(dict(kwargs))
        if self.pay_error is not None:
            raise self.pay_error
        body = voucher_order(marker=self.marker, status="paid")
        self.reader.order = body
        return VoucherMutationResponse(http_status=200, envelope=body)

    async def refund_voucher_order(self, **kwargs: Any):
        from altegio_bot.easyweek_voucher_mutation import VoucherMutationResponse

        self.calls.append("refund")
        if self.refund_error is not None:
            raise self.refund_error
        body = voucher_order(marker=self.marker, status="refunded")
        self.reader.order = body
        return VoucherMutationResponse(http_status=200, envelope=body)


class FakeSender:
    """Captures the one Meta call, including the parameters, for inspection."""

    def __init__(self, *, outcome: DeliveryOutcome | None = None) -> None:
        self.outcome = outcome or DeliveryOutcome(
            outcome=DELIVERY_ACCEPTED, provider_message_id=PROVIDER_MESSAGE_ID, http_status=200
        )
        self.calls: list[dict[str, Any]] = []

    async def send_voucher_template(self, **kwargs: Any) -> DeliveryOutcome:
        self.calls.append(dict(kwargs))
        return self.outcome


class RefusingSender:
    async def send_voucher_template(self, **kwargs: Any) -> DeliveryOutcome:  # pragma: no cover
        raise AssertionError("no message may be sent")


class RefusingMutator:
    async def create_voucher_order(self, **kwargs: Any):  # pragma: no cover
        raise AssertionError("no mutation may be sent")

    async def pay_voucher_order(self, **kwargs: Any):  # pragma: no cover
        raise AssertionError("no mutation may be sent")

    async def refund_voucher_order(self, **kwargs: Any):  # pragma: no cover
        raise AssertionError("no mutation may be sent")


UNKNOWN_OUTCOME = DeliveryOutcome(outcome=DELIVERY_UNKNOWN, reason="meta_timeout")


# ---------------------------------------------------------------------------
# Database seeding
# ---------------------------------------------------------------------------


async def seed_recipient(
    session_maker,
    *,
    provider: str = PROVIDER_EASYWEEK,
    company_id: int = COMPANY_ID,
    campaign_code: str = NEW_CLIENT_CAMPAIGN_CODE,
    run_status: str = "completed",
    run_mode: str = "preview",
    recipient_status: str = "candidate",
    phone: str | None = PHONE,
    client_phone: str | None = None,
    opted_out: bool = False,
    booking_uuid: uuid_module.UUID = BOOKING_UUID,
) -> tuple[int, int]:
    """One completed preview run with one eligible candidate. Returns ids."""
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                provider=provider,
                campaign_code=campaign_code,
                mode=run_mode,
                company_ids=[company_id],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status=run_status,
            )
            session.add(run)
            client = Client(
                provider=PROVIDER_EASYWEEK,
                company_id=company_id,
                altegio_client_id=CUSTOMER_ID,
                phone_e164=client_phone if client_phone is not None else phone,
                display_name="Synthetic Fixture",
                raw={},
                wa_opted_out=opted_out,
                easyweek_visits_total=1,
                easyweek_visits_total_updated_at=NOW - timedelta(days=1),
            )
            session.add(client)
            await session.flush()
            record = Record(
                provider=PROVIDER_EASYWEEK,
                company_id=company_id,
                altegio_record_id=BOOKING_ID,
                easyweek_booking_uuid=booking_uuid,
                client_id=client.id,
                altegio_client_id=CUSTOMER_ID,
                starts_at=VISIT_START,
                is_deleted=False,
                raw=record_raw_with_services_count(record_raw_with_service_category({}, "Wimpernverlängerung"), 1),
            )
            session.add(record)
            event = EasyWeekEvent(
                status="processed",
                event_hint="booking-succeeded",
                body_truncated=False,
                payload={
                    "uid": str(booking_uuid),
                    "id": BOOKING_ID,
                    "location_id": company_id,
                    "location_uuid": KARLSRUHE_LOCATION_UUID,
                    "customer_id": CUSTOMER_ID,
                    "visits_total": 1,
                },
                payload_hash=f"event-{uuid_module.uuid4()}",
            )
            session.add(event)
            await session.flush()
            # A non-EasyWeek recipient may not carry EasyWeek source proof: the
            # §33 CHECK constraint forbids that mixture outright, so a
            # cross-provider row is seeded the only way it can exist.
            proof: dict[str, Any] = (
                {
                    "source_easyweek_event_id": event.id,
                    "source_record_id": record.id,
                    "source_booking_uuid": booking_uuid,
                    "source_visits_total": 1,
                    "source_visits_total_updated_at": client.easyweek_visits_total_updated_at,
                }
                if provider == PROVIDER_EASYWEEK
                else {}
            )
            recipient = CampaignRecipient(
                provider=provider,
                campaign_run_id=run.id,
                company_id=company_id,
                client_id=client.id,
                phone_e164=phone,
                display_name="Synthetic Fixture",
                status=recipient_status,
                is_opted_out=opted_out,
                **proof,
            )
            session.add(recipient)
            await session.flush()
            return run.id, recipient.id


async def seed_template_and_sender(
    session_maker,
    *,
    company_id: int = COMPANY_ID,
    body: str | None = None,
    meta_name: str | None = None,
    language: str | None = None,
    code: str = VOUCHER_TEMPLATE_CODE,
    is_active: bool = True,
    with_sender: bool = True,
) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(
                MessageTemplate(
                    provider=PROVIDER_EASYWEEK,
                    company_id=company_id,
                    code=code,
                    language=language or template_contract.VOUCHER_TEMPLATE_LANGUAGE,
                    body=body if body is not None else template_contract.VOUCHER_TEMPLATE_BODY,
                    meta_template_name=meta_name
                    if meta_name is not None
                    else template_contract.VOUCHER_META_TEMPLATE_NAME,
                    is_active=is_active,
                )
            )
            if with_sender:
                session.add(
                    WhatsAppSender(
                        provider=PROVIDER_EASYWEEK,
                        company_id=company_id,
                        sender_code="default",
                        phone_number_id=SENDER_PHONE_NUMBER_ID,
                        is_active=True,
                    )
                )


def meta_template(**changes: Any) -> dict[str, Any]:
    """One Meta template row as the Graph API returns it."""
    value: dict[str, Any] = {
        "name": template_contract.VOUCHER_META_TEMPLATE_NAME,
        "language": template_contract.VOUCHER_TEMPLATE_LANGUAGE,
        "status": "APPROVED",
        "category": "MARKETING",
        "parameter_format": "POSITIONAL",
        "components": [{"type": "BODY", "text": template_contract.positional_body()}],
    }
    value.update(changes)
    return value
