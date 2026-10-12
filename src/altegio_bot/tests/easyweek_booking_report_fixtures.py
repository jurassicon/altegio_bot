"""Synthetic bookings, branches and delivery states for the booking report.

What is real here
-----------------
The freeze, the create, the pay and the deliver stages are the real ones, run
through the real runner against the real database, so a report is read off
state the application actually produced. The capture rows are real
``easyweek_events`` rows, written the way the webhook writes them — including
the canonical ``booking_uuid`` the capture derives — so the report reads the
same shape production has.

What is synthetic
-----------------
EasyWeek, Meta, the issuer pin, every identity, and the clock. Delivery
instants are moved afterwards with a narrow helper rather than by freezing
time: the attribution window is a property of stored instants, and a test about
its boundaries has to be able to place a send at a chosen moment without
pretending the whole process ran then.

The branch registry here holds THREE branches, because "any branch of the same
workspace" cannot be tested against a registry with one.
"""

from __future__ import annotations

import json
import uuid as uuid_module
from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy import select, update

from altegio_bot.campaigns.easyweek_manual_batch import MANUAL_POLICY
from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production import runner as runner_module
from altegio_bot.campaigns.easyweek_voucher_production.composition import BatchApproval
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_FREEZE,
    STAGE_PAY,
)
from altegio_bot.easyweek_normalizer import canonical_booking_uuid
from altegio_bot.easyweek_voucher_identity import KARLSRUHE_LOCATION_UUID
from altegio_bot.easyweek_voucher_mutation import VoucherMutationResponse
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    CampaignRecipient,
    CampaignRun,
    Client,
    EasyWeekEvent,
    EasyWeekVoucherProductionBatchItem,
)
from altegio_bot.settings import settings
from altegio_bot.tests import easyweek_voucher_delivery_fixtures as earned_fixtures
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    BOOKING_LINK,
    COMPANY_ID,
    CUSTOMER_UUIDS,
    ORDER_UUIDS,
    PHONES,
    FakeMutator,
    FakeReader,
    FakeSender,
    approval_for,
    marker_orders,
    production_request,
    seed_production_preview,
    seed_template_and_sender,
)
from altegio_bot.utils import utcnow

# Two more branches of the SAME workspace, deliberately fabricated. Their ids
# are not production ids and never were.
DURLACH_COMPANY_ID = 322580
DURLACH_LOCATION_UUID = "8395fab6-7ee8-4702-88d9-fd78f92539c2"
ETTLINGEN_COMPANY_ID = 322581
ETTLINGEN_LOCATION_UUID = "8395fab6-7ee8-4702-88d9-fd78f92539c3"

# A location that is NOT in the registry: another workspace, or a branch nobody
# configured. Its deliveries must never reach a report.
FOREIGN_COMPANY_ID = 999777
FOREIGN_LOCATION_UUID = "8395fab6-7ee8-4702-88d9-fd78f925aaaa"

BRANCH_NAMES = {
    COMPANY_ID: "karlsruhe",
    DURLACH_COMPANY_ID: "durlach",
    ETTLINGEN_COMPANY_ID: "ettlingen",
}

_LOCATION_UUIDS = {
    COMPANY_ID: KARLSRUHE_LOCATION_UUID,
    DURLACH_COMPANY_ID: DURLACH_LOCATION_UUID,
    ETTLINGEN_COMPANY_ID: ETTLINGEN_LOCATION_UUID,
    FOREIGN_COMPANY_ID: FOREIGN_LOCATION_UUID,
}


def workspace_location_map() -> str:
    """The registry string, with all three branches of the workspace."""
    return json.dumps(
        {
            "karlsruhe": {
                "location_id": COMPANY_ID,
                "location_uuid": KARLSRUHE_LOCATION_UUID,
                "meta_template_prefix": "ka",
                "booking_page_url": BOOKING_LINK,
            },
            "durlach": {
                "location_id": DURLACH_COMPANY_ID,
                "location_uuid": DURLACH_LOCATION_UUID,
                "meta_template_prefix": "du",
                "booking_page_url": "https://durlach-production.example.invalid/",
            },
            "ettlingen": {
                "location_id": ETTLINGEN_COMPANY_ID,
                "location_uuid": ETTLINGEN_LOCATION_UUID,
                "meta_template_prefix": "et",
                "booking_page_url": "https://ettlingen-production.example.invalid/",
            },
        }
    )


@pytest.fixture
def workspace_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every branch of the workspace configured, not just the mailing's own.

    Requested AFTER ``production_configuration`` so it widens the registry that
    fixture installed: the analytics scope is the workspace, while the mailing
    keeps its own single-branch limits.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", workspace_location_map(), raising=False)


# ---------------------------------------------------------------------------
# Capture rows
# ---------------------------------------------------------------------------


def booking_payload(
    *,
    booking_uuid: str,
    customer_id: int | None,
    created_at: str | None,
    company_id: int = COMPANY_ID,
    location_uuid: str | None = None,
    service_name: str = "Lash Extension",
    service_category: str | None = "Lashes",
    **changes: Any,
) -> dict[str, Any]:
    """A flat EasyWeek booking payload, in the confirmed root-level shape.

    Only the fields this report proves are meaningful here; the rest are
    present so the payload is not a stripped-down shape no webhook would send.
    """
    payload: dict[str, Any] = {
        "uid": booking_uuid,
        "id": 4_200_000 + (customer_id or 0) % 1000,
        "customer_id": customer_id,
        "location_id": company_id,
        "location_uuid": location_uuid or _LOCATION_UUIDS[company_id],
        "booking_created_at": created_at,
        "booking_date_start": "2026-11-20T10:00:00+0000",
        "booking_date_end": "2026-11-20T11:00:00+0000",
        "timezone": "Europe/Berlin",
        "booking_duration": 60,
        "customer_full_name": "Synthetic Booking Person",
        "customer_phone": "+49000000000",
        "service_id": 5_100_003,
        "service_name": service_name,
        "service_category": service_category,
        "services_count": 1,
        "booking_price": "3500",
        "booking_price_currency": "EUR",
    }
    if customer_id is None:
        payload.pop("customer_id")
    if created_at is None:
        payload.pop("booking_created_at")
    payload.update(changes)
    return payload


async def capture(
    session_maker,
    *,
    payload: dict[str, Any],
    event_hint: str = "booking-created",
    received_at: datetime,
    status: str = "processed",
    body_truncated: bool = False,
) -> int:
    """One authenticated delivery, stored the way capture stores it.

    ``status`` defaults to ``processed`` only because that is the ordinary case;
    every test that cares proves the report does not read it.
    """
    async with session_maker() as session:
        async with session.begin():
            event = EasyWeekEvent(
                received_at=received_at,
                status=status,
                event_hint=event_hint,
                auth_via="query",
                payload=payload,
                query={},
                headers={},
                body_truncated=body_truncated,
                booking_uuid=canonical_booking_uuid(payload),
                processed_at=received_at if status in ("processed", "failed") else None,
            )
            session.add(event)
            await session.flush()
            return int(event.id)


# ---------------------------------------------------------------------------
# A real mailing, driven through the real stages
# ---------------------------------------------------------------------------


def _ok(index: int) -> VoucherMutationResponse:
    return VoucherMutationResponse(http_status=200, envelope={"uuid": ORDER_UUIDS[index]})


_RUNNERS = {
    STAGE_FREEZE: runner_module.run_freeze,
    STAGE_CREATE: runner_module.run_create,
    STAGE_PAY: runner_module.run_pay,
    STAGE_DELIVER: runner_module.run_deliver,
}


async def _plan(session_maker, reader, *, stage, request, approval=None):
    async with session_maker() as session:
        plan, *_ = await runner_module.build_stage_plan(
            session,
            session_maker,
            stage=stage,
            request=request,
            reader=reader,
            order_reader=reader,
            approval=approval,
        )
    return plan


async def _apply(session_maker, reader, *, stage, request, approval=None, **extra):
    plan = await _plan(session_maker, reader, stage=stage, request=request, approval=approval)
    assert plan.ready, plan.reasons
    common = {
        "request": request,
        "reader": reader,
        "order_reader": reader,
        "apply": True,
        "supplied_digest": plan.digest,
        "supplied_issued_at": plan.issued_at,
        "supplied_phrase": plan.confirmation_phrase,
    }
    if stage == STAGE_FREEZE:
        common["approval"] = approval if approval is not None else BatchApproval()
    async with session_maker() as session:
        return await _RUNNERS[stage](session, session_maker, **common, **extra)


async def frozen_batch(session_maker, reader, *, count: int, run_id: int | None = None, offset: int = 0, **seed: Any):
    """A frozen batch and nothing more: no create, no pay, no send."""
    if run_id is None:
        run_id, _ = await seed_production_preview(session_maker, count=count, offset=offset, **seed)
    await seed_template_and_sender(session_maker)
    report = await _apply(
        session_maker,
        reader,
        stage=STAGE_FREEZE,
        request=production_request(run_id=run_id),
        approval=approval_for(count),
    )
    batch_id = int(report.batch["batch_id"])
    return run_id, batch_id, production_request(run_id=run_id, batch_id=batch_id)


async def delivered_batch(
    session_maker, reader, *, count: int, run_id: int | None = None, offset: int = 0, **seed: Any
):
    """Frozen, created, paid and sent — every slot accepted by Meta."""
    run_id, batch_id, request = await frozen_batch(
        session_maker, reader, count=count, run_id=run_id, offset=offset, **seed
    )
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id, offset=offset))
    await _apply(
        session_maker,
        reader,
        stage=STAGE_CREATE,
        request=request,
        mutator=FakeMutator(create_sequence=[_ok(offset + index) for index in range(count)]),
    )
    paid = await marker_orders(session_maker, batch_id=batch_id, offset=offset, status="paid")
    await _apply(
        session_maker,
        reader,
        stage=STAGE_PAY,
        request=request,
        mutator=FakeMutator(pay_sequence=[_ok(offset + index) for index in range(count)], reader=reader, settles=paid),
    )
    sender = FakeSender(first_index=offset)
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    return run_id, batch_id, request


class MixedReader(FakeReader):
    """A reader that can answer both proofs of a mixed audience.

    Copied in shape from the §44 mixed-audience suite rather than imported from
    it: that module is a test module, and a fixture importing a test module
    would collect its tests twice.
    """

    def __init__(self) -> None:
        super().__init__(count=2)
        self.booking = earned_fixtures.booking_payload(customer={"uuid": CUSTOMER_UUIDS[0]})
        self.histories: dict[str, list[Any]] = {CUSTOMER_UUIDS[0]: [self.booking], CUSTOMER_UUIDS[1]: []}

    async def get_booking(self, booking_uuid: str) -> Any:
        assert booking_uuid == str(earned_fixtures.BOOKING_UUID)
        return self.booking

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 100) -> Any:
        assert page == 1 and per_page == 100
        return earned_fixtures.history_page(self.histories[customer_uuid])


# The numeric EasyWeek customer id of the earned recipient's local card. It is
# the fixture's own constant, and it is what makes that card numeric-only: the
# earned shape in production has no UUID column filled in.
EARNED_NUMERIC = earned_fixtures.CUSTOMER_ID


async def mixed_delivered_batch(session_maker, monkeypatch):
    """One earned recipient and one manual one, both frozen, paid and sent.

    The earned member is proven live from its own source booking and history,
    exactly as a freeze proves it, so the report is read off a genuinely mixed
    batch rather than off a manual batch with a relabelled column.
    """
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", '["Wimpernverlängerung"]')
    run_id, earned_id = await earned_fixtures.seed_recipient(session_maker, phone=PHONES[0])
    _run, manual_ids = await seed_production_preview(session_maker, count=1, offset=1)
    async with session_maker() as session:
        async with session.begin():
            recipient = await session.get(CampaignRecipient, manual_ids[0])
            recipient.campaign_run_id = run_id
            recipient.manual_policy = MANUAL_POLICY
            recipient.manual_policy_checked_at = utcnow()
            recipient.manual_operator_attested_at = utcnow()
    reader = MixedReader()
    run_id, batch_id, request = await delivered_batch(session_maker, reader, count=2, run_id=run_id)
    return run_id, batch_id, request, [earned_id, manual_ids[0]]


async def set_send_times(
    session_maker,
    *,
    batch_id: int,
    slot: int,
    attempted_at: datetime | None = None,
    accepted_at: datetime | None = ...,  # type: ignore[assignment]
    delivered_at: datetime | None = ...,  # type: ignore[assignment]
    read_at: datetime | None = ...,  # type: ignore[assignment]
) -> None:
    """Move one slot's delivery instants, leaving every other column alone.

    The schema's ladder is untouched: acceptance still needs an attempt and a
    provider message id, delivery still needs acceptance and reading still
    needs delivery, so a combination the database refuses is refused here too.
    """
    values: dict[str, Any] = {}
    if attempted_at is not None:
        values["send_attempted_at"] = attempted_at
    if accepted_at is not ...:
        values["provider_accepted_at"] = accepted_at
    if delivered_at is not ...:
        values["delivered_at"] = delivered_at
    if read_at is not ...:
        values["read_at"] = read_at
    if not values:
        return
    async with session_maker() as session:
        async with session.begin():
            await session.execute(
                update(EasyWeekVoucherProductionBatchItem)
                .where(
                    EasyWeekVoucherProductionBatchItem.batch_id == batch_id,
                    EasyWeekVoucherProductionBatchItem.slot == slot,
                )
                .values(**values)
            )


async def set_run_window(session_maker, *, run_id: int, days: int) -> None:
    """Give one campaign its own attribution window.

    Written directly because the column is set when a run is CREATED and
    nothing in the project updates it afterwards — which is exactly the
    property the report relies on. A test that needs a different window is
    therefore describing a differently created campaign.
    """
    async with session_maker() as session:
        async with session.begin():
            await session.execute(
                update(CampaignRun).where(CampaignRun.id == run_id).values(attribution_window_days=days)
            )


async def slot_customer_uuid(session_maker, *, batch_id: int, slot: int) -> str:
    async with session_maker() as session:
        found = await session.scalar(
            select(EasyWeekVoucherProductionBatchItem.easyweek_customer_uuid).where(
                EasyWeekVoucherProductionBatchItem.batch_id == batch_id,
                EasyWeekVoucherProductionBatchItem.slot == slot,
            )
        )
    return str(found)


async def bind_numeric(session_maker, *, batch_id: int, slot: int, customer_id: int | None) -> int:
    """Give the slot's own frozen local card an EasyWeek numeric id.

    This is the bridge an earned recipient has in production: a numeric-only
    card the freeze re-proved, with no UUID column filled in.
    """
    async with session_maker() as session:
        async with session.begin():
            recipient_id = await session.scalar(
                select(EasyWeekVoucherProductionBatchItem.campaign_recipient_id).where(
                    EasyWeekVoucherProductionBatchItem.batch_id == batch_id,
                    EasyWeekVoucherProductionBatchItem.slot == slot,
                )
            )
            client_id = await session.scalar(
                select(CampaignRecipient.client_id).where(CampaignRecipient.id == recipient_id)
            )
            await session.execute(update(Client).where(Client.id == client_id).values(altegio_client_id=customer_id))
            return int(client_id)


async def add_branch_card(
    session_maker,
    *,
    company_id: int,
    customer_uuid: str | None,
    customer_id: int | None,
    phone: str,
    display_name: str = "Synthetic Branch Card",
) -> int:
    """A second EasyWeek card for the same person, in another branch.

    Written directly, because a card in another branch is established by a real
    booking in that branch and this report must never create one itself.
    """
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                provider=PROVIDER_EASYWEEK,
                company_id=company_id,
                altegio_client_id=customer_id,
                easyweek_customer_uuid=uuid_module.UUID(customer_uuid) if customer_uuid else None,
                phone_e164=phone,
                display_name=display_name,
                raw={},
            )
            session.add(client)
            await session.flush()
            return int(client.id)


async def set_card_uuid(session_maker, *, client_id: int, customer_uuid: str | None) -> None:
    async with session_maker() as session:
        async with session.begin():
            await session.execute(
                update(Client)
                .where(Client.id == client_id)
                .values(easyweek_customer_uuid=uuid_module.UUID(customer_uuid) if customer_uuid else None)
            )


async def item_rows(session_maker, *, batch_id: int) -> list[EasyWeekVoucherProductionBatchItem]:
    async with session_maker() as session:
        return list(
            (
                await session.scalars(
                    select(EasyWeekVoucherProductionBatchItem)
                    .where(EasyWeekVoucherProductionBatchItem.batch_id == batch_id)
                    .order_by(EasyWeekVoucherProductionBatchItem.slot.asc())
                )
            ).all()
        )


async def provider_message_id(session_maker, *, batch_id: int, slot: int) -> str:
    async with session_maker() as session:
        found = await session.scalar(
            select(EasyWeekVoucherProductionBatchItem.provider_message_id).where(
                EasyWeekVoucherProductionBatchItem.batch_id == batch_id,
                EasyWeekVoucherProductionBatchItem.slot == slot,
            )
        )
    return str(found)


def utc(year: int, month: int, day: int, hour: int = 12, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def iso(value: datetime) -> str:
    """The compact offset form EasyWeek actually sends for UTC."""
    return value.strftime("%Y-%m-%dT%H:%M:%S%z")


async def batch_snapshot(session_maker, *, batch_id: int):
    return await ledger_module.load(session_maker, batch_id=batch_id)


__all__ = [
    "BRANCH_NAMES",
    "DURLACH_COMPANY_ID",
    "DURLACH_LOCATION_UUID",
    "ETTLINGEN_COMPANY_ID",
    "FOREIGN_COMPANY_ID",
    "FOREIGN_LOCATION_UUID",
    "EARNED_NUMERIC",
    "MixedReader",
    "add_branch_card",
    "batch_snapshot",
    "bind_numeric",
    "booking_payload",
    "capture",
    "delivered_batch",
    "frozen_batch",
    "iso",
    "item_rows",
    "mixed_delivered_batch",
    "provider_message_id",
    "set_card_uuid",
    "set_run_window",
    "set_send_times",
    "slot_customer_uuid",
    "utc",
    "workspace_branches",
    "workspace_location_map",
]
