"""Prove an optional UUID bridge without redefining numeric booking ingestion.

A current booking is mutable. Adoption needs its captured phone or an
independently stored numeric/UUID binding. Never borrow missing identity facts
from the booking's current customer to interpret an older event.
"""

from __future__ import annotations

import uuid

from sqlalchemy import or_, select

from altegio_bot.campaigns.easyweek_manual_identity import (
    KARLSRUHE_COMPANY_ID as KARLSRUHE_COMPANY_ID,
)
from altegio_bot.campaigns.easyweek_manual_identity import (
    lock_workspace_identity,
)
from altegio_bot.campaigns.easyweek_manual_recipient import prove_customer
from altegio_bot.campaigns.easyweek_voucher_delivery.eligibility import _booking_customer_uuid
from altegio_bot.easyweek_client import EasyWeekClient
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_normalizer import NormalizedBooking
from altegio_bot.models.models import Client, EasyWeekEvent


class UUIDIdentityResolutionRequired(Exception):
    def __init__(self, booking: NormalizedBooking):
        super().__init__("easyweek_uuid_identity_proof_required")
        self.booking = booking


async def lock_customer_phone(session, phone: str | None) -> None:
    # Missing phones must also serialize against simultaneous UI Add.
    await lock_workspace_identity(session)


async def needs_uuid_resolution(session, booking: NormalizedBooking) -> bool:
    # No unconditional Karlsruhe/voucher prerequisite for ordinary ingestion.
    conditions = or_(
        Client.altegio_client_id == booking.customer_id,
        (Client.company_id == booking.company_id) & Client.altegio_client_id.is_(None),
    )
    if booking.phone_e164:
        conditions = or_(conditions, Client.phone_e164 == booking.phone_e164)
    return bool(
        await session.scalar(
            select(Client.id)
            .where(Client.provider == "easyweek", Client.easyweek_customer_uuid.is_not(None), conditions)
            .limit(1)
        )
    )


async def _known_binding(session, customer_id: int) -> tuple[uuid.UUID | None, bool]:
    values = set(
        (
            await session.scalars(
                select(Client.easyweek_customer_uuid).where(
                    Client.provider == "easyweek",
                    Client.altegio_client_id == customer_id,
                    Client.easyweek_customer_uuid.is_not(None),
                )
            )
        ).all()
    )
    return (next(iter(values)) if len(values) == 1 else None, len(values) > 1)


def _customer_body(payload):
    if not isinstance(payload, dict):
        return None
    body = payload.get("data", payload)
    return body if isinstance(body, dict) else None


async def resolve_webhook_identity(session_factory, *, event_id: int, booking: NormalizedBooking, reader=None) -> bool:
    """Read independently, then recheck the bridge under identity/event locks."""
    registry = configured_easyweek_locations()
    location = registry.locations.get(booking.company_id) if registry.ready else None
    if location is None or booking.customer_id is None:
        return False
    async with session_factory() as session:
        known_uuid, conflict = await _known_binding(session, booking.customer_id)
    if conflict:
        return False
    captured_phone = booking.phone_e164 if booking.carries("phone_e164") else None
    if captured_phone is None and known_uuid is None:
        return False

    owned_reader = reader is None
    try:
        if owned_reader:
            reader = EasyWeekClient()
        body = _customer_body(await reader.get_booking(str(booking.booking_uuid)))
        if body is None or body.get("location_uuid") != location.location_uuid:
            return False
        customer_uuid = _booking_customer_uuid(body, expected_booking_uuid=booking.booking_uuid)
        if customer_uuid is None:
            return False
        identity = uuid.UUID(customer_uuid)
        if known_uuid is not None and known_uuid != identity:
            return False
        if captured_phone is not None:
            proof, reason = await prove_customer(reader, phone=captured_phone, require_name=False)
            if reason is not None or proof is None or proof.uuid != customer_uuid:
                return False
        else:
            card = _customer_body(await reader.get_customer(customer_uuid))
            if card is None or str(uuid.UUID(card.get("uuid", ""))) != customer_uuid:
                return False
            if booking.carries("phone_e164") and (
                "phone" not in card
                or card["phone"] is not None
                and (not isinstance(card["phone"], str) or card["phone"].strip() != "")
            ):
                return False
    except Exception:
        return False
    finally:
        if owned_reader and reader is not None:
            await reader.aclose()

    async with session_factory() as session, session.begin():
        event = await session.scalar(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id).with_for_update())
        if event is None or event.status != "captured" or event.booking_uuid != booking.booking_uuid:
            return False
        await lock_workspace_identity(session)
        current_uuid, conflict = await _known_binding(session, booking.customer_id)
        if conflict or current_uuid not in (None, identity) or known_uuid is not None and current_uuid != known_uuid:
            return False
        selectors = [Client.easyweek_customer_uuid == identity, Client.altegio_client_id == booking.customer_id]
        if captured_phone is not None:
            selectors.append(Client.phone_e164 == captured_phone)
        rows = list(
            (
                await session.scalars(
                    select(Client)
                    .where(Client.provider == "easyweek", or_(*selectors))
                    .order_by(Client.id)
                    .with_for_update()
                )
            ).all()
        )
        for row in rows:
            if row.easyweek_customer_uuid == identity and row.altegio_client_id not in (None, booking.customer_id):
                return False
            if (
                captured_phone is not None
                and row.phone_e164 == captured_phone
                and (
                    row.easyweek_customer_uuid not in (None, identity)
                    or row.altegio_client_id not in (None, booking.customer_id)
                )
            ):
                return False
        branch_rows = [row for row in rows if row.company_id == booking.company_id]
        if len(branch_rows) > 1:
            return False
        if branch_rows:
            client = branch_rows[0]
            if client.easyweek_customer_uuid not in (None, identity) or client.altegio_client_id not in (
                None,
                booking.customer_id,
            ):
                return False
            client.easyweek_customer_uuid = identity
            client.altegio_client_id = booking.customer_id
            if booking.carries("phone_e164"):
                client.phone_e164 = booking.phone_e164
            opted_out = next((row for row in rows if row.wa_opted_out), None)
            if opted_out is not None and not client.wa_opted_out:
                client.wa_opted_out = True
                client.wa_opted_out_at = opted_out.wa_opted_out_at
                client.wa_opt_out_reason = opted_out.wa_opt_out_reason
            # Opt-out, its audit, visits and every other branch stay intact.
        else:
            session.add(
                Client(
                    provider="easyweek",
                    company_id=booking.company_id,
                    altegio_client_id=booking.customer_id,
                    easyweek_customer_uuid=identity,
                    phone_e164=captured_phone,
                    display_name=booking.display_name,
                    raw={},
                    wa_opted_out=any(row.wa_opted_out for row in rows),
                    wa_opted_out_at=next((row.wa_opted_out_at for row in rows if row.wa_opted_out), None),
                    wa_opt_out_reason=next((row.wa_opt_out_reason for row in rows if row.wa_opted_out), None),
                )
            )
        return True
