"""Synthetic seeds for the §46.2 follow-up preview tests.

Every identity here is obviously fabricated. No production phone number,
customer uuid, order, message id, voucher code or artifact value belongs in
this repository, and the preview never reads one anyway: what it reads is the
ledger's own evidence, which is what these helpers build.

The seed goes in through the ORM rather than through the §45 runner on purpose.
The preview is a READER of a finished mailing, and the states it has to tell
apart — accepted and silent, delivered, read, locally refused, refused by Meta,
never attempted — are states a successful run does not produce all at once. A
test that had to drive a real send to reach "accepted and then nothing" would be
testing the runner, not the audience rules.
"""

from __future__ import annotations

import json
import uuid as uuid_module
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_voucher_production.dispatch import SENDER_CODE
from altegio_bot.easyweek_voucher_identity import KARLSRUHE_LOCATION_UUID
from altegio_bot.easyweek_voucher_production_contract import production_contract
from altegio_bot.models.models import (
    INBOUND_AUTHENTICITY_SIGNED,
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_EARNED,
    RECIPIENT_BASIS_MANUAL,
    VOUCHER_PRODUCTION_COMPLETED,
    VOUCHER_PRODUCTION_ITEM_DELIVERED,
    VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED,
    VOUCHER_PRODUCTION_ITEM_READ,
    VOUCHER_PRODUCTION_SCOPE,
    CampaignRecipient,
    CampaignRun,
    Client,
    EasyWeekEvent,
    EasyWeekVoucherProductionBatch,
    EasyWeekVoucherProductionBatchAttempt,
    EasyWeekVoucherProductionBatchItem,
    EasyWeekVoucherProductionDeliveryReceipt,
    MessageTemplate,
    Record,
    WhatsAppSender,
)
from altegio_bot.whatsapp_consent import recipient_route_digest, sender_route_digest

COMPANY_ID = 322579
CAMPAIGN_CODE = "new_clients_monthly"
SCHEMA_VERSION = "4"

PERIOD_START = datetime(2026, 9, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 10, 1, tzinfo=timezone.utc)
FROZEN_AT = datetime(2026, 9, 2, 9, 0, tzinfo=timezone.utc)
CREATED_AT = datetime(2026, 9, 2, 9, 30, tzinfo=timezone.utc)
PAID_AT = datetime(2026, 9, 2, 9, 45, tzinfo=timezone.utc)
ATTEMPTED_AT = datetime(2026, 9, 2, 10, 0, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc)

# The synthetic Meta sender of the Karlsruhe line.
PHONE_NUMBER_ID = "111111111111111"
SENDER_DIGEST = sender_route_digest(PHONE_NUMBER_ID)

STAFFER_UUID = "dddddddd-4444-4444-8444-dddddddddddd"
ACCOUNT_UUID = "eeeeeeee-5555-4555-8555-eeeeeeeeeeee"
TEMPLATE_UUID = "0ffb0346-57b8-475e-9c22-152dd23e25ca"


def person(index: int) -> dict[str, Any]:
    """One obviously fake person: a uuid, a number, a booking and a numeric id.

    The numeric id is the bridge EasyWeek's own booking webhooks use — they name
    a customer by number, never by uuid — so a fixture without one cannot
    exercise the booking CREATION capture at all.
    """
    return {
        "customer_uuid": f"{index:08d}-1111-4111-8111-111111111111",
        "phone": f"+4915100{index:05d}",
        "booking_uuid": f"{index:08d}-2222-4222-8222-222222222222",
        "numeric_id": 7700000 + index,
    }


def order_uuid_for(batch_id: int, slot: int) -> str:
    """The synthetic order uuid of one slot. Unique across batches, as in production."""
    return f"{int(batch_id):04d}{int(slot):04d}-3333-4333-8333-333333333333"


def voucher_code_for(index: int) -> str:
    """One obviously fabricated code, whose whole job is to be searched for."""
    return f"FOLLOWUP-TEST-CODE-{index:04d}"


def stored_mac(
    *,
    batch_id: int,
    slot: int,
    schema_version: str,
    frozen_digest: str,
    recipient_basis: str,
    source_proof_digest: str | None,
    customer_uuid: str,
    target_order_uuid: str,
    code: str,
) -> tuple[str, str]:
    """``(key_id, mac)`` for one slot, bound the way a real claim binds it.

    Computed through the production phase's own material and domain, so a test
    that seeds this and a preview that verifies it cannot disagree about what
    the binding covers.
    """
    from altegio_bot.campaigns.easyweek_voucher_delivery.binding import voucher_code_mac
    from altegio_bot.campaigns.easyweek_voucher_production.identity import binding_material
    from altegio_bot.campaigns.easyweek_voucher_production.ledger import VOUCHER_PRODUCTION_DOMAIN

    contract = production_contract(schema_version)
    material = binding_material(
        batch_id=int(batch_id),
        slot=int(slot),
        schema_version=schema_version,
        product_contract_version=contract.version,
        message_contract_code=contract.message_code,
        frozen_digest=frozen_digest,
        recipient_basis=recipient_basis,
        manual_policy=None,
        source_proof_digest=source_proof_digest,
        customer_uuid=customer_uuid,
    )
    return voucher_code_mac(
        voucher_code=code,
        ledger_uuid=material,
        target_order_uuid=target_order_uuid,
        voucher_template_uuid=contract.template_uuid,
        domain=VOUCHER_PRODUCTION_DOMAIN,
    )


def voucher_order(
    *,
    order_uuid: str,
    marker: str,
    customer_uuid: str,
    code: str,
    schema_version: str = SCHEMA_VERSION,
    status: str = "paid",
    vouchers: list[dict[str, Any]] | None = None,
    **changes: Any,
) -> dict[str, Any]:
    """One EasyWeek order as a read-back describes it, in safe synthetic form.

    The voucher line uses the singleton shape production actually returns —
    ``voucher_template_uuid``, ``value``, ``price`` and no quantity — because
    the §35 proof the artifact check reuses is written against that shape, and
    a fixture with a different one would be testing the fixture.
    """
    contract = production_contract(schema_version)
    order: dict[str, Any] = {
        "uuid": order_uuid,
        "comment": marker,
        "customer": {"uuid": customer_uuid},
        "status": status,
        "is_paid": status in ("paid", "refunded"),
        "is_reverted": status == "refunded",
        "total": contract.issue_price_minor,
        "subtotal": contract.issue_price_minor,
        "services": [],
        "goods": [],
        "vouchers": (
            [
                {
                    "code": code,
                    "voucher_template_uuid": contract.template_uuid,
                    "value": contract.face_value_minor,
                    "price": contract.issue_price_minor,
                }
            ]
            if vouchers is None
            else vouchers
        ),
        "created_at": FROZEN_AT.isoformat(),
    }
    order.update(changes)
    return order


def _digest_of(phone: str) -> str:
    value = recipient_route_digest(phone)
    assert value is not None
    return value


# A SECOND configured branch of the same workspace, deliberately fabricated:
# these ids are not production ids and never were. One numeric customer id is
# unique per branch rather than per workspace, so two cards claiming the same
# number against two different proven uuids need two branches to exist at all.
SECOND_COMPANY_ID = 322581
SECOND_LOCATION_UUID = "8395fab6-7ee8-4702-88d9-fd78f92539c3"

# And a branch NOBODY configured: another workspace, or one left out of the
# registry. Nothing it holds may speak about this mailing's bookings.
UNCONFIGURED_COMPANY_ID = 999777


def location_map(*, booking_link: str = "https://example.test/ka", second_branch: bool = False) -> str:
    """The configured branch registry, as the settings string the loader reads.

    Needed by the booking CREATION capture (§46.5): a creation delivery is
    accepted only for a configured branch of this workspace, so a test with no
    registry would see every creation rejected as foreign — which is the honest
    answer for an unconfigured deployment and the wrong starting point for a
    test about creation times.

    ``second_branch`` widens it to two branches of the SAME workspace, leaving
    the mailing's own branch exactly as it was. Needed by the tests about the
    reverse numeric-id lookup: the uniqueness that makes an ambiguous bridge
    possible is per branch.
    """
    branches = {
        "karlsruhe": {
            "location_id": COMPANY_ID,
            "location_uuid": KARLSRUHE_LOCATION_UUID,
            "meta_template_prefix": "ka",
            "booking_page_url": booking_link,
        }
    }
    if second_branch:
        branches["ettlingen"] = {
            "location_id": SECOND_COMPANY_ID,
            "location_uuid": SECOND_LOCATION_UUID,
            "meta_template_prefix": "et",
            "booking_page_url": "https://example.test/et",
        }
    return json.dumps(branches)


async def seed_sender_and_template(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    company_id: int = COMPANY_ID,
    with_followup_template: bool = False,
    phone_number_id: str = PHONE_NUMBER_ID,
) -> None:
    """The branch's active sender, and optionally the follow-up template row.

    ``with_followup_template`` is off by default because that is the real state
    of every deployment today: the follow-up template does not exist, and a test
    that silently assumed it did would be testing a configuration nobody has.
    """
    from altegio_bot.campaigns.easyweek_voucher_followup import followup_template_contract as message

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    provider=PROVIDER_EASYWEEK,
                    company_id=company_id,
                    sender_code=SENDER_CODE,
                    phone_number_id=phone_number_id,
                    display_phone="+49 721 000000",
                    is_active=True,
                )
            )
            if with_followup_template:
                session.add(
                    MessageTemplate(
                        provider=PROVIDER_EASYWEEK,
                        company_id=company_id,
                        code=message.FOLLOWUP_TEMPLATE_CODE,
                        language=message.FOLLOWUP_TEMPLATE_LANGUAGE,
                        body=message.FOLLOWUP_TEMPLATE_BODY,
                        meta_template_name=message.FOLLOWUP_META_TEMPLATE_NAME,
                        is_active=True,
                    )
                )


def approved_meta_templates() -> list[dict[str, Any]]:
    """A Meta listing in which the follow-up template really is approved."""
    from altegio_bot.campaigns.easyweek_voucher_followup import followup_template_contract as message

    return [
        {
            "name": message.FOLLOWUP_META_TEMPLATE_NAME,
            "language": message.FOLLOWUP_TEMPLATE_LANGUAGE,
            "status": "APPROVED",
            "category": message.FOLLOWUP_TEMPLATE_CATEGORY,
            "parameter_format": message.FOLLOWUP_TEMPLATE_PARAMETER_FORMAT,
            "components": [{"type": "BODY", "text": message.positional_body()}],
        }
    ]


class TemplateReader:
    """The live Meta listing seam, answered from a fixture."""

    def __init__(self, templates: list[dict[str, Any]] | None = None) -> None:
        self.templates = templates if templates is not None else approved_meta_templates()
        self.calls = 0

    async def list_meta_templates(self) -> list[dict[str, Any]]:
        self.calls += 1
        return list(self.templates)


async def seed_source_batch(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    slots: list[dict[str, Any]],
    company_id: int = COMPANY_ID,
    recipient_basis: str | None = None,
    schema_version: str = SCHEMA_VERSION,
    status: str = VOUCHER_PRODUCTION_COMPLETED,
    frozen_digest: str = "f" * 64,
    hmac_key_id: str = "test-key-1",
) -> tuple[int, list[int]]:
    """One finished production batch whose slots are in the states a test wants.

    Each entry of ``slots`` describes one person and one outcome. The defaults
    describe the ordinary good case — a delivered message from the branch's own
    sender, over a proven route — so a test only states what it is changing::

        {"state": "read"}                      # a read receipt arrived
        {"state": "delivered"}                 # delivered, no read
        {"state": "accepted"}                  # accepted by Meta, then silence
        {"attempted": False}                   # never attempted
        {"sender_digest": "other"}             # went out from another line
        {"phone": "+4915100099999"}            # the card's number moved
        {"status": "refunded"}                 # the entitlement is gone

    Returns the batch id and the campaign recipient ids in slot order.
    """
    count = len(slots)
    # Each slot may name its own basis; the header carries the one they share,
    # or ``mixed`` when they do not — exactly what the §42 composition read
    # writes. A caller may still pin the header explicitly, which is how a test
    # seeds a header that disagrees with its slots.
    slot_bases = [str(spec.get("basis", RECIPIENT_BASIS_EARNED)) for spec in slots]
    header_basis = recipient_basis or (slot_bases[0] if len(set(slot_bases)) == 1 else "mixed")
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                provider=PROVIDER_EASYWEEK,
                campaign_code=CAMPAIGN_CODE,
                mode="preview",
                company_ids=[company_id],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
            )
            session.add(run)
            await session.flush()

            contract = production_contract(schema_version)
            batch = EasyWeekVoucherProductionBatch(
                batch_scope=VOUCHER_PRODUCTION_SCOPE,
                request_schema_version=schema_version,
                provider=PROVIDER_EASYWEEK,
                product_contract_version=contract.version,
                message_contract_code=contract.message_code,
                baseline_version=contract.baseline_version,
                company_id=company_id,
                campaign_code=CAMPAIGN_CODE,
                recipient_basis=header_basis,
                campaign_run_id=int(run.id),
                campaign_period_start=PERIOD_START,
                campaign_period_end=PERIOD_END,
                location_uuid=uuid_module.UUID(KARLSRUHE_LOCATION_UUID),
                staffer_uuid=uuid_module.UUID(STAFFER_UUID),
                payment_account_uuid=uuid_module.UUID(ACCOUNT_UUID),
                voucher_template_uuid=uuid_module.UUID(contract.template_uuid),
                frozen_digest=frozen_digest,
                recipient_count=count,
                voucher_unit_price_minor=contract.face_value_minor,
                total_exposure_minor=contract.face_value_minor * count,
                voucher_issue_price_minor=contract.issue_price_minor,
                total_issue_price_minor=contract.issue_price_minor * count,
                approved_recipient_count=count,
                approved_exposure_minor=contract.face_value_minor * count,
                status=status,
                frozen_at=FROZEN_AT,
            )
            session.add(batch)
            await session.flush()

            recipient_ids: list[int] = []
            for position, spec in enumerate(slots, start=1):
                basis = str(spec.get("basis", RECIPIENT_BASIS_EARNED))
                who = person(spec.get("index", position))
                phone = spec.get("phone", who["phone"])
                card_phone = spec.get("card_phone", phone)
                customer_uuid = uuid_module.UUID(spec.get("customer_uuid", who["customer_uuid"]))
                booking_uuid = spec.get("booking_uuid", who["booking_uuid"])

                event = EasyWeekEvent(query={}, headers={}, payload={"synthetic": True})
                session.add(event)
                await session.flush()
                card = Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=company_id,
                    easyweek_customer_uuid=customer_uuid,
                    altegio_client_id=spec.get("numeric_id", who["numeric_id"]),
                    display_name=f"Person {position}",
                    phone_e164=card_phone,
                    raw={},
                    wa_opted_out=bool(spec.get("card_opted_out", False)),
                )
                session.add(card)
                await session.flush()
                record = Record(
                    company_id=company_id,
                    # Unique across batches: the marker, the order uuid and
                    # this id are all table-wide unique in production, so a
                    # test that seeds two batches must not reuse them.
                    altegio_record_id=900000 + int(batch.id) * 100 + position,
                    client_id=int(card.id),
                    is_deleted=False,
                    raw={},
                )
                session.add(record)
                await session.flush()

                recipient = CampaignRecipient(
                    provider=PROVIDER_EASYWEEK,
                    campaign_run_id=int(run.id),
                    company_id=company_id,
                    client_id=int(card.id),
                    phone_e164=phone,
                    status="candidate",
                    recipient_basis=basis,
                    total_records_in_period=1,
                    arrived_records_in_period=1,
                    is_opted_out=bool(spec.get("recipient_opted_out", False)),
                    meta={},
                    easyweek_customer_uuid=(customer_uuid if basis == RECIPIENT_BASIS_MANUAL else None),
                    source_easyweek_event_id=(int(event.id) if basis == RECIPIENT_BASIS_EARNED else None),
                    source_record_id=(int(record.id) if basis == RECIPIENT_BASIS_EARNED else None),
                    source_booking_uuid=(booking_uuid if basis == RECIPIENT_BASIS_EARNED else None),
                    source_visits_total=(1 if basis == RECIPIENT_BASIS_EARNED else None),
                    source_visits_total_updated_at=(FROZEN_AT if basis == RECIPIENT_BASIS_EARNED else None),
                )
                session.add(recipient)
                await session.flush()
                recipient_ids.append(int(recipient.id))

                state = spec.get("state", "delivered")
                attempted = bool(spec.get("attempted", True))
                marker = f"marker-{batch.id}-{position}"
                order_uuid = order_uuid_for(int(batch.id), position)
                code = voucher_code_for(position)
                computed_key_id, computed_mac = stored_mac(
                    batch_id=int(batch.id),
                    slot=position,
                    schema_version=schema_version,
                    frozen_digest=frozen_digest,
                    recipient_basis=basis,
                    source_proof_digest=("d" * 64 if basis == RECIPIENT_BASIS_EARNED else None),
                    customer_uuid=str(customer_uuid),
                    target_order_uuid=order_uuid,
                    code=code,
                )
                # The key id a caller pinned wins, which is how a test seeds a
                # MAC written under a key this deployment no longer holds.
                slot_key_id = spec.get("hmac_key_id", hmac_key_id or computed_key_id)
                slot_mac = spec.get("voucher_code_hmac", computed_mac)
                item_status = spec.get(
                    "status",
                    {
                        "read": VOUCHER_PRODUCTION_ITEM_READ,
                        "delivered": VOUCHER_PRODUCTION_ITEM_DELIVERED,
                        "accepted": VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED,
                    }.get(state, VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED),
                )
                attempted_at = spec.get("attempted_at", ATTEMPTED_AT) if attempted else None
                message_id = (
                    spec.get("provider_message_id", f"wamid.b{int(batch.id)}s{position}") if attempted else None
                )
                item = EasyWeekVoucherProductionBatchItem(
                    batch_id=int(batch.id),
                    batch_recipient_count=count,
                    slot=position,
                    provider=PROVIDER_EASYWEEK,
                    company_id=company_id,
                    campaign_code=CAMPAIGN_CODE,
                    recipient_basis=basis,
                    source_booking_uuid=(uuid_module.UUID(booking_uuid) if basis == RECIPIENT_BASIS_EARNED else None),
                    source_proof_digest=("d" * 64 if basis == RECIPIENT_BASIS_EARNED else None),
                    campaign_run_id=int(run.id),
                    campaign_recipient_id=int(recipient.id),
                    easyweek_customer_uuid=customer_uuid,
                    campaign_period_start=PERIOD_START,
                    campaign_period_end=PERIOD_END,
                    voucher_value_minor=contract.face_value_minor,
                    voucher_quantity=1,
                    reconciliation_marker=marker,
                    target_order_uuid=uuid_module.UUID(order_uuid),
                    outbound_intent_uuid=uuid_module.UUID(
                        f"{int(batch.id):04d}{position:04d}-4444-4444-8444-444444444444"
                    ),
                    provider_message_id=message_id,
                    voucher_code_hmac=slot_mac,
                    hmac_key_id=slot_key_id,
                    status=item_status,
                    # The real CREATE/PAY stage order the §45 CHECKs require: a
                    # slot cannot be sent before the voucher it carries is
                    # proven created, paid for, bound by MAC and re-guarded. A
                    # fixture that skipped them would be seeding a row
                    # production cannot hold.
                    create_claimed_at=CREATED_AT,
                    create_attempted_at=CREATED_AT,
                    create_verified_at=CREATED_AT,
                    pay_claimed_at=PAID_AT,
                    pay_attempted_at=PAID_AT,
                    pay_verified_at=PAID_AT,
                    live_guard_reproven_at=attempted_at or PAID_AT,
                    send_attempt_count=1 if attempted else 0,
                    send_claimed_at=attempted_at,
                    send_attempted_at=attempted_at,
                    provider_accepted_at=attempted_at,
                    delivered_at=(
                        spec.get("delivered_at", attempted_at + timedelta(minutes=1))
                        if attempted and state in {"delivered", "read"}
                        else spec.get("delivered_at")
                    ),
                    read_at=(
                        spec.get("read_at", attempted_at + timedelta(minutes=2))
                        if attempted and state == "read"
                        else spec.get("read_at")
                    ),
                    evidence={},
                )
                session.add(item)
                await session.flush()

                # §46.3's proof behind a positive status, as the live status
                # path now writes it. ``receipt_proven=False`` seeds the
                # historical case instead: a slot whose status is the mailing's
                # record and whose proof was never recorded.
                if attempted and state in {"delivered", "read"} and spec.get("receipt_proven", True):
                    kinds = ["delivered"] if state == "delivered" else ["delivered", "read"]
                    for receipt_kind in kinds:
                        session.add(
                            EasyWeekVoucherProductionDeliveryReceipt(
                                fact_key=f"receipt-{batch.id}-{position}-{receipt_kind}",
                                kind=receipt_kind,
                                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                                item_id=int(item.id),
                                batch_id=int(batch.id),
                                slot=position,
                                provider_message_id=str(message_id),
                                observed_sender_digest=spec.get("sender_digest", SENDER_DIGEST),
                                observed_recipient_digest=_digest_of(phone),
                                route_proven=True,
                                observed_at=attempted_at + timedelta(minutes=1),
                                event_at=attempted_at + timedelta(minutes=1),
                                evidence={},
                            )
                        )
                if attempted:
                    session.add(
                        EasyWeekVoucherProductionBatchAttempt(
                            item_id=int(item.id),
                            intent_uuid=item.outbound_intent_uuid,
                            template_code="new_client_voucher_10eur_v2",
                            meta_template_name="kitilash_ka_new_client_voucher_10eur_v2",
                            template_language="de",
                            campaign_recipient_id=int(recipient.id),
                            batch_id=int(batch.id),
                            slot=position,
                            outcome="claimed",
                            claimed_at=attempted_at,
                            provider_message_id=message_id,
                            sender_phone_number_id_digest=spec.get("sender_digest", SENDER_DIGEST),
                            recipient_digest=(
                                None if spec.get("route_unproven") else spec.get("recipient_digest", _digest_of(phone))
                            ),
                        )
                    )
            await session.flush()
            return int(batch.id), recipient_ids


_MISSING = object()


async def seed_booking_created(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    booking_uuid: str,
    numeric_customer_id: int,
    created_at: datetime,
    company_id: int = COMPANY_ID,
    received_at: datetime | None = None,
    location_uuid: str = KARLSRUHE_LOCATION_UUID,
) -> int:
    """One captured ``booking-created`` delivery, as the ingress stored it.

    ``created_at`` is the provider's OWN statement of when the booking was
    made, and ``received_at`` is when this deployment happened to hear about it.
    They are deliberately separate arguments: the whole point of §46.5's
    creation rule is that the second is not the first.
    """
    async with session_maker() as session:
        async with session.begin():
            event = EasyWeekEvent(
                event_hint="booking-created",
                query={},
                headers={},
                payload={
                    "uid": booking_uuid,
                    "location_id": company_id,
                    "location_uuid": location_uuid,
                    "customer_id": int(numeric_customer_id),
                    "booking_created_at": created_at.isoformat(),
                },
                received_at=received_at or created_at,
                status="processed",
            )
            session.add(event)
            await session.flush()
            return int(event.id)


async def seed_booking_created_without_a_date(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    booking_uuid: str,
    numeric_customer_id: int,
    received_at: datetime | None = None,
    company_id: int = COMPANY_ID,
    location_uuid: str = KARLSRUHE_LOCATION_UUID,
    created_at_value: Any = _MISSING,
) -> int:
    """A creation delivery whose branch, booking and customer are proven and
    whose own instant is missing or unreadable.

    The shape §46.5 has to keep half of: the delivery says WHICH booking and
    WHOSE it is, and does not say when it was made. Dropping it whole threw the
    proven half away with the unproven one.
    """
    payload: dict[str, Any] = {
        "uid": booking_uuid,
        "location_id": company_id,
        "location_uuid": location_uuid,
        "customer_id": int(numeric_customer_id),
    }
    if created_at_value is not _MISSING:
        payload["booking_created_at"] = created_at_value
    async with session_maker() as session:
        async with session.begin():
            event = EasyWeekEvent(
                event_hint="booking-created",
                query={},
                headers={},
                payload=payload,
                received_at=received_at or NOW,
                status="processed",
            )
            session.add(event)
            await session.flush()
            return int(event.id)


async def seed_booking_cancelled(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    booking_uuid: str,
    numeric_customer_id: int,
    company_id: int = COMPANY_ID,
    received_at: datetime | None = None,
    location_uuid: str = KARLSRUHE_LOCATION_UUID,
) -> int:
    """One captured ``booking-canceled`` delivery of the same booking."""
    async with session_maker() as session:
        async with session.begin():
            event = EasyWeekEvent(
                event_hint="booking-canceled",
                query={},
                headers={},
                payload={
                    "uid": booking_uuid,
                    "location_id": company_id,
                    "location_uuid": location_uuid,
                    "customer_id": int(numeric_customer_id),
                },
                received_at=received_at or NOW,
                status="processed",
            )
            session.add(event)
            await session.flush()
            return int(event.id)
