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

import uuid as uuid_module
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_voucher_production.dispatch import SENDER_CODE
from altegio_bot.easyweek_voucher_identity import KARLSRUHE_LOCATION_UUID
from altegio_bot.easyweek_voucher_production_contract import production_contract
from altegio_bot.models.models import (
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


def person(index: int) -> dict[str, str]:
    """One obviously fake person: a uuid, a number and a booking."""
    return {
        "customer_uuid": f"{index:08d}-1111-4111-8111-111111111111",
        "phone": f"+4915100{index:05d}",
        "booking_uuid": f"{index:08d}-2222-4222-8222-222222222222",
    }


def _digest_of(phone: str) -> str:
    value = recipient_route_digest(phone)
    assert value is not None
    return value


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
    recipient_basis: str = RECIPIENT_BASIS_EARNED,
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
                recipient_basis=recipient_basis,
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
                    display_name=f"Person {position}",
                    phone_e164=card_phone,
                    raw={},
                    wa_opted_out=bool(spec.get("card_opted_out", False)),
                )
                session.add(card)
                await session.flush()
                record = Record(
                    company_id=company_id,
                    altegio_record_id=900000 + position,
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
                    recipient_basis=recipient_basis,
                    total_records_in_period=1,
                    arrived_records_in_period=1,
                    is_opted_out=bool(spec.get("recipient_opted_out", False)),
                    meta={},
                    easyweek_customer_uuid=(customer_uuid if recipient_basis == RECIPIENT_BASIS_MANUAL else None),
                    source_easyweek_event_id=(int(event.id) if recipient_basis == RECIPIENT_BASIS_EARNED else None),
                    source_record_id=(int(record.id) if recipient_basis == RECIPIENT_BASIS_EARNED else None),
                    source_booking_uuid=(booking_uuid if recipient_basis == RECIPIENT_BASIS_EARNED else None),
                    source_visits_total=(1 if recipient_basis == RECIPIENT_BASIS_EARNED else None),
                    source_visits_total_updated_at=(FROZEN_AT if recipient_basis == RECIPIENT_BASIS_EARNED else None),
                )
                session.add(recipient)
                await session.flush()
                recipient_ids.append(int(recipient.id))

                state = spec.get("state", "delivered")
                attempted = bool(spec.get("attempted", True))
                item_status = spec.get(
                    "status",
                    {
                        "read": VOUCHER_PRODUCTION_ITEM_READ,
                        "delivered": VOUCHER_PRODUCTION_ITEM_DELIVERED,
                        "accepted": VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED,
                    }.get(state, VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED),
                )
                attempted_at = spec.get("attempted_at", ATTEMPTED_AT) if attempted else None
                message_id = spec.get("provider_message_id", f"wamid.slot{position}") if attempted else None
                item = EasyWeekVoucherProductionBatchItem(
                    batch_id=int(batch.id),
                    batch_recipient_count=count,
                    slot=position,
                    provider=PROVIDER_EASYWEEK,
                    company_id=company_id,
                    campaign_code=CAMPAIGN_CODE,
                    recipient_basis=recipient_basis,
                    source_booking_uuid=(
                        uuid_module.UUID(booking_uuid) if recipient_basis == RECIPIENT_BASIS_EARNED else None
                    ),
                    source_proof_digest=("d" * 64 if recipient_basis == RECIPIENT_BASIS_EARNED else None),
                    campaign_run_id=int(run.id),
                    campaign_recipient_id=int(recipient.id),
                    easyweek_customer_uuid=customer_uuid,
                    campaign_period_start=PERIOD_START,
                    campaign_period_end=PERIOD_END,
                    voucher_value_minor=contract.face_value_minor,
                    voucher_quantity=1,
                    reconciliation_marker=f"marker-{batch.id}-{position}",
                    target_order_uuid=uuid_module.UUID(f"{position:08d}-3333-4333-8333-333333333333"),
                    outbound_intent_uuid=uuid_module.UUID(f"{position:08d}-4444-4444-8444-444444444444"),
                    provider_message_id=message_id,
                    voucher_code_hmac=spec.get("voucher_code_hmac", "a" * 64),
                    hmac_key_id=spec.get("hmac_key_id", hmac_key_id),
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
