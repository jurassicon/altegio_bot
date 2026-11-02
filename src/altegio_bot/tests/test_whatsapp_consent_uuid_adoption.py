"""A refusal recorded before any card survives the card being created (§46.4.2).

The chain this module exists for has four links and only breaks at the fourth,
which is why the existing tests — one for "no card at all", one for a number
change on a UUID that already existed — both passed while the combination lost
the refusal:

1. a STOP arrives from somebody this database has never seen, so the refusal is
   keyed by the number alone and bound to no customer UUID;
2. a UUID-only card for that person appears afterwards, and an ordinary booking
   then proves the bridge and adopts it;
3. the card's number then changes, provably, through the same path;
4. from that moment the aggregate read looks for refusals on the numbers cards
   carry NOW — and nothing connected the old refusal to the new number.

The adoption in step 2 is the one moment the connection can be justified, and it
is driven here through the real ``easyweek_uuid_identity`` path rather than
written by hand, because the whole question is whether production reaches it.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from altegio_bot import easyweek_uuid_identity as identity
from altegio_bot.models.models import (
    CONSENT_SOURCE_INBOUND,
    CONSENT_STATE_OPTED_OUT,
    PROVIDER_EASYWEEK,
    Client,
    EasyWeekEvent,
    WhatsAppConsentSuppression,
)
from altegio_bot.tests.easyweek_fixtures import TEST_CUSTOMER_ID, TEST_LOCATION_ID, booking_created
from altegio_bot.tests.test_easyweek_uuid_identity_worker import CUSTOMER, PHONE, Reader
from altegio_bot.tests.test_easyweek_uuid_identity_worker import (
    configuration as configuration,  # noqa: PLC0414 - autouse fixture, imported on purpose
)
from altegio_bot.whatsapp_consent import (
    CONSENT_COMMAND_STOP,
    apply_consent_decision,
    consent_decision,
    final_send_guard,
    is_suppressed,
    lock_consent_address,
)
from altegio_bot.workers import easyweek_inbox_worker as worker

NEW_PHONE = "+4915100000822"


async def _refuse(session_maker, address: str = PHONE) -> None:
    """A STOP from somebody with no card, through the ordinary ledger path."""
    moment = datetime.now(timezone.utc) - timedelta(days=5)
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, address)
            outcome = await apply_consent_decision(
                session,
                wa_address=address,
                command=CONSENT_COMMAND_STOP,
                authenticity="unverified",
                source=CONSENT_SOURCE_INBOUND,
                observed_at=moment,
                event_at=moment,
            )
    assert outcome.applied


async def _uuid_only_card(session_maker, *, company_id: int = TEST_LOCATION_ID, phone: str = PHONE) -> int:
    """A card carrying the customer UUID and no numeric id yet.

    The shape the manual and voucher identity paths leave behind, and the state
    the booking bridge exists to adopt. Created AFTER the refusal in these
    tests, which is the half the previous coverage did not have.
    """
    async with session_maker() as session:
        async with session.begin():
            card = Client(
                provider=PROVIDER_EASYWEEK,
                company_id=company_id,
                altegio_client_id=None,
                easyweek_customer_uuid=uuid.UUID(CUSTOMER),
                easyweek_identity_assigned_at=datetime.now(timezone.utc),
                phone_e164=phone,
                display_name="Synthetic",
                raw={},
            )
            session.add(card)
            await session.flush()
            return int(card.id)


async def _capture_booking(session_maker) -> tuple[int, dict]:
    """One captured booking event."""
    payload = booking_created()
    payload["customer_phone"] = payload["customer_attributes.customer_phone"] = PHONE
    async with session_maker() as session:
        async with session.begin():
            event = EasyWeekEvent(
                status="captured",
                event_hint="booking-created",
                auth_via="query",
                payload_hash=f"synthetic-{uuid.uuid4().hex}",
                payload=payload,
                body_truncated=False,
                booking_uuid=uuid.UUID(payload["uid"]),
            )
            session.add(event)
            await session.flush()
            return int(event.id), payload


async def _suppression(session_maker, address: str = PHONE) -> WhatsAppConsentSuppression | None:
    async with session_maker() as session:
        return (
            await session.execute(
                select(WhatsAppConsentSuppression).where(WhatsAppConsentSuppression.wa_address == address)
            )
        ).scalar_one_or_none()


async def _adopt(session_maker, monkeypatch, payload: dict) -> None:
    """Run the real adoption path over the captured event."""
    monkeypatch.setattr(identity, "EasyWeekClient", lambda: Reader(payload, session_maker))
    assert await worker.process_one() is True
    await worker.process_one()


async def test_a_refusal_with_no_card_follows_a_uuid_proven_later(session_maker, monkeypatch) -> None:
    """The whole chain, in order, ending at a guard that still refuses.

    Only the STOP and the card are written by hand: the UUID is proven by the
    real bridge, which is what justifies the binding at all.
    """
    await _refuse(session_maker)
    await _uuid_only_card(session_maker)
    event_id, payload = await _capture_booking(session_maker)

    await _adopt(session_maker, monkeypatch, payload)

    async with session_maker() as session:
        card = (await session.execute(select(Client).where(Client.provider == PROVIDER_EASYWEEK))).scalar_one()
        assert card.easyweek_customer_uuid == uuid.UUID(CUSTOMER)
        assert card.altegio_client_id == TEST_CUSTOMER_ID, "the bridge really ran"
        assert card.phone_e164 == PHONE
        assert (await session.get(EasyWeekEvent, event_id)).status == "processed"

    # The refusal is now bound to the proven identity, as a BAN extension.
    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    assert row.banned_easyweek_customer_uuid == uuid.UUID(CUSTOMER)
    assert row.evidence.get("uuid_binding") == "proven_uuid_adoption"

    # The number changes, and the old one is on no card any more.
    async with session_maker() as session:
        async with session.begin():
            card = (await session.execute(select(Client).where(Client.provider == PROVIDER_EASYWEEK))).scalar_one()
            card.phone_e164 = NEW_PHONE

    async with session_maker() as session:
        assert await is_suppressed(session, phone=NEW_PHONE, easyweek_customer_uuid=uuid.UUID(CUSTOMER))
        # And without the caller naming the UUID at all: the new number is on a
        # card that names it, which is enough to widen the search for refusals.
        assert await is_suppressed(session, phone=NEW_PHONE)
        guard = await final_send_guard(session, phone=NEW_PHONE, easyweek_customer_uuid=uuid.UUID(CUSTOMER))
    assert guard.allowed is False
    assert guard.reason == "consent_refusal_active"


async def test_the_binding_claims_nothing_about_who_the_person_is(session_maker, monkeypatch) -> None:
    """Ban extension only. No card, no identity and no branch is invented.

    A phone match deciding who somebody IS would be a new identity heuristic,
    and §46.4.2 allows exactly the opposite direction: the number may decide who
    must not be messaged and nothing else.
    """
    await _refuse(session_maker)
    await _uuid_only_card(session_maker)
    _event_id, payload = await _capture_booking(session_maker)
    await _adopt(session_maker, monkeypatch, payload)

    async with session_maker() as session:
        cards = list(
            (
                await session.scalars(
                    select(Client).where(
                        Client.provider == PROVIDER_EASYWEEK,
                        Client.easyweek_customer_uuid == uuid.UUID(CUSTOMER),
                    )
                )
            ).all()
        )
        assert len(cards) == 1, "the refusal created no card and duplicated none"
        card = cards[0]
        assert card.company_id == TEST_LOCATION_ID
        assert card.altegio_client_id == TEST_CUSTOMER_ID
        # The refusal did not become an opt-out on the card either: the ledger
        # is where it lives, and the legacy flag is not written from it here.
        assert card.wa_opted_out is False
        assert await session.scalar(select(func.count()).select_from(WhatsAppConsentSuppression)) == 1

    decision = None
    async with session_maker() as session:
        decision = await consent_decision(session, phone=PHONE)
    assert decision.suppressed is True
    assert decision.easyweek_customer_uuid == uuid.UUID(CUSTOMER)


async def test_a_card_that_already_existed_when_the_stop_arrived_also_keeps_it(session_maker, monkeypatch) -> None:
    """The other order, which has to end in the same place by different code.

    Here the STOP itself finds the card and records the UUID, so the adoption
    has nothing left to bind — and the refusal still survives the number change.
    """
    await _uuid_only_card(session_maker)
    await _refuse(session_maker)
    _event_id, payload = await _capture_booking(session_maker)
    await _adopt(session_maker, monkeypatch, payload)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.banned_easyweek_customer_uuid == uuid.UUID(CUSTOMER)

    async with session_maker() as session:
        async with session.begin():
            card = (await session.execute(select(Client).where(Client.provider == PROVIDER_EASYWEEK))).scalar_one()
            card.phone_e164 = NEW_PHONE
    async with session_maker() as session:
        assert await is_suppressed(session, phone=NEW_PHONE, easyweek_customer_uuid=uuid.UUID(CUSTOMER))


async def test_several_branch_cards_of_one_proven_identity_all_refuse(session_maker, monkeypatch) -> None:
    """A second branch's card for the same UUID is covered by the same refusal."""
    await _refuse(session_maker)
    await _uuid_only_card(session_maker)
    _event_id, payload = await _capture_booking(session_maker)
    await _adopt(session_maker, monkeypatch, payload)

    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=TEST_LOCATION_ID + 1,
                    altegio_client_id=None,
                    easyweek_customer_uuid=uuid.UUID(CUSTOMER),
                    phone_e164=NEW_PHONE,
                    display_name="Second branch",
                    raw={},
                )
            )

    async with session_maker() as session:
        decision = await consent_decision(session, phone=NEW_PHONE, easyweek_customer_uuid=uuid.UUID(CUSTOMER))
    assert decision.suppressed is True
    assert PHONE in decision.matched_addresses or "consent_suppression_uuid_bound" in decision.sources


async def test_an_ambiguous_number_binds_nothing_and_still_refuses(session_maker) -> None:
    """Two identities on one number: the ban stays on the number, not on a guess."""
    other = uuid.uuid4()
    async with session_maker() as session:
        async with session.begin():
            for index, (company, customer) in enumerate(
                ((TEST_LOCATION_ID, uuid.UUID(CUSTOMER)), (TEST_LOCATION_ID + 1, other))
            ):
                session.add(
                    Client(
                        provider=PROVIDER_EASYWEEK,
                        company_id=company,
                        altegio_client_id=990100 + index,
                        easyweek_customer_uuid=customer,
                        phone_e164=PHONE,
                        display_name=f"Card {index}",
                        raw={},
                    )
                )
    await _refuse(session_maker)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.banned_easyweek_customer_uuid is None
    assert row.evidence.get("uuid_binding") == "ambiguous"
    async with session_maker() as session:
        assert await is_suppressed(session, phone=PHONE)
        decision = await consent_decision(session, phone=PHONE)
    assert "easyweek_uuid_binding_ambiguous" in decision.gaps


async def test_a_refusal_already_bound_elsewhere_is_not_moved(session_maker, monkeypatch) -> None:
    """One number can be two people over time, so a binding is never overwritten."""
    from altegio_bot.whatsapp_consent import extend_refusal_to_uuid

    first = uuid.uuid4()
    await _refuse(session_maker)
    async with session_maker() as session:
        async with session.begin():
            await extend_refusal_to_uuid(session, addresses=[PHONE], customer_uuid=first)

    await _uuid_only_card(session_maker)
    _event_id, payload = await _capture_booking(session_maker)
    await _adopt(session_maker, monkeypatch, payload)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.banned_easyweek_customer_uuid == first
    assert row.evidence.get("uuid_binding_conflict_at") is not None
    # The number itself is still refused, which is the part that matters.
    async with session_maker() as session:
        assert await is_suppressed(session, phone=PHONE)


async def test_adoption_without_any_refusal_writes_no_consent_row(session_maker, monkeypatch) -> None:
    """The binding never CREATES a refusal. It only follows one that exists."""
    await _uuid_only_card(session_maker)
    _event_id, payload = await _capture_booking(session_maker)
    await _adopt(session_maker, monkeypatch, payload)

    async with session_maker() as session:
        assert await session.scalar(select(func.count()).select_from(WhatsAppConsentSuppression)) == 0
        assert await is_suppressed(session, phone=PHONE) is False
