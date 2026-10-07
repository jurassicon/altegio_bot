"""Synthetic earned source plus booking-free manual contacts for Ops acceptance."""

from __future__ import annotations

from sqlalchemy import select

from altegio_bot.models.models import CampaignRecipient, Client
from altegio_bot.tests.easyweek_voucher_delivery_fixtures import (
    BOOKING_UUID,
    booking_payload,
    history_page,
    seed_recipient,
)
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    CUSTOMER_NAMES,
    CUSTOMER_UUIDS,
    PHONES,
    FakeReader,
    seed_template_and_sender,
)


class MixedReader(FakeReader):
    def __init__(self, *, count: int = 3) -> None:
        super().__init__(count=count)
        self.booking = booking_payload(customer={"uuid": CUSTOMER_UUIDS[0]})
        self.history = {customer: history_page([]) for customer in CUSTOMER_UUIDS[:count]}
        self.history[CUSTOMER_UUIDS[0]] = history_page([self.booking])
        self.history_calls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def get_booking(self, booking_uuid: str):
        assert booking_uuid == str(BOOKING_UUID)
        return self.booking

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 100):
        assert page == 1 and per_page == 100
        self.history_calls.append(customer_uuid)
        return self.history[customer_uuid]


async def seed_mixed_editor(session_maker) -> tuple[int, int]:
    run_id, recipient_id = await seed_recipient(session_maker, phone=PHONES[0])
    async with session_maker() as session, session.begin():
        recipient = await session.get(CampaignRecipient, recipient_id)
        recipient.display_name = CUSTOMER_NAMES[0]
        client = await session.scalar(select(Client).where(Client.id == recipient.client_id))
        client.display_name = CUSTOMER_NAMES[0]
    await seed_template_and_sender(session_maker)
    return run_id, recipient_id
