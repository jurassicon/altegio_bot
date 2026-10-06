"""Attach the first real webhook to a UUID-only operator-created Client.

The ordinary worker transaction asks for proof by raising a typed signal. Only
AFTER that transaction rolls back do these GETs run. No numeric customer API
field is assumed: the id comes from the validated webhook of this exact booking.
"""

from __future__ import annotations

import hashlib
import uuid

from sqlalchemy import or_, select, text

from altegio_bot.campaigns.easyweek_manual_identity import KARLSRUHE_COMPANY_ID, lock_identity
from altegio_bot.campaigns.easyweek_manual_recipient import prove_customer
from altegio_bot.campaigns.easyweek_voucher_delivery.eligibility import _booking_customer_uuid
from altegio_bot.easyweek_client import EasyWeekClient
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_migration.customer_api import read_customer_card
from altegio_bot.easyweek_normalizer import NormalizedBooking
from altegio_bot.models.models import Client, EasyWeekEvent


class UUIDIdentityResolutionRequired(Exception):
    def __init__(self, booking: NormalizedBooking):
        super().__init__("easyweek_uuid_identity_proof_required")
        self.booking = booking


async def lock_customer_phone(session, phone: str | None) -> None:
    if phone is not None:
        key = int.from_bytes(
            hashlib.sha256(f"easyweek-identity-phone:{phone}".encode()).digest()[:8], "big", signed=True
        )
        await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})


async def needs_uuid_resolution(session, booking: NormalizedBooking) -> bool:
    # Both creation paths must share a proven UUID lock, including when the
    # webhook omitted its phone and no operator identity existed at first read.
    if booking.company_id == KARLSRUHE_COMPANY_ID:
        return True
    conditions = (Client.company_id == booking.company_id) & Client.altegio_client_id.is_(None)
    if booking.phone_e164:
        conditions = or_(conditions, Client.phone_e164 == booking.phone_e164)
    return bool(
        await session.scalar(
            select(Client.id)
            .where(
                Client.provider == "easyweek",
                Client.easyweek_customer_uuid.is_not(None),
                conditions,
            )
            .limit(1)
        )
    )


async def resolve_webhook_identity(session_factory, *, event_id: int, booking: NormalizedBooking, reader=None) -> bool:
    """Read externally, then adopt in a fresh transaction; failures leave no row."""
    registry = configured_easyweek_locations()
    location = registry.locations.get(booking.company_id) if registry.ready else None
    if location is None or booking.customer_id is None:
        return False
    # An explicitly cleared/invalid webhook phone cannot be silently replaced
    # by a GET phone on every retry; that would create an immediate proof loop.
    if booking.carries("phone_e164") and booking.phone_e164 is None:
        return False
    owned_reader = reader is None
    try:
        if owned_reader:
            reader = EasyWeekClient()
        payload = await reader.get_booking(str(booking.booking_uuid))
        if not isinstance(payload, dict):
            return False
        body = payload.get("data", payload)
        if not isinstance(body, dict) or body.get("location_uuid") != location.location_uuid:
            return False
        customer_uuid = _booking_customer_uuid(body, expected_booking_uuid=booking.booking_uuid)
        if customer_uuid is None:
            return False
        phone = booking.phone_e164
        if phone is None:
            card = read_customer_card(await reader.get_customer(customer_uuid))
            phone = card.phone
        if not phone:
            return False
        proof, reason = await prove_customer(reader, phone=phone, require_name=False)
        if reason is not None or proof is None or proof.uuid != customer_uuid:
            return False
    except Exception:
        return False
    finally:
        if owned_reader and reader is not None:
            await reader.aclose()

    identity = uuid.UUID(customer_uuid)
    async with session_factory() as session, session.begin():
        event = await session.scalar(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id).with_for_update())
        if event is None or event.status != "captured" or event.booking_uuid != booking.booking_uuid:
            return False
        await lock_identity(session, phone=phone, customer_uuid=customer_uuid)
        rows = list(
            (
                await session.scalars(
                    select(Client)
                    .where(
                        Client.provider == "easyweek",
                        or_(
                            Client.easyweek_customer_uuid == identity,
                            Client.phone_e164 == phone,
                            (Client.company_id == booking.company_id)
                            & (Client.altegio_client_id == booking.customer_id),
                        ),
                    )
                    .order_by(Client.id)
                    .with_for_update()
                )
            ).all()
        )
        if len(rows) > 1:
            return False
        if rows:
            client = rows[0]
            if (
                client.company_id != booking.company_id
                or client.phone_e164 != phone
                or client.easyweek_customer_uuid not in (None, identity)
                or client.altegio_client_id not in (None, booking.customer_id)
            ):
                return False
            # Never change opt-out or visit counters when attaching identity.
            client.easyweek_customer_uuid = identity
            client.altegio_client_id = booking.customer_id
        else:
            # A real captured booking named a different person; it may use the
            # ordinary numeric path on the next pass without another GET.
            session.add(
                Client(
                    provider="easyweek",
                    company_id=booking.company_id,
                    altegio_client_id=booking.customer_id,
                    easyweek_customer_uuid=identity,
                    phone_e164=phone,
                    display_name=proof.first_name or booking.display_name,
                    raw={},
                )
            )
        return True
