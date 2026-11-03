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

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from altegio_bot import easyweek_uuid_identity as identity
from altegio_bot.models.models import (
    CONSENT_COMMAND_START,
    CONSENT_SOURCE_INBOUND,
    CONSENT_STATE_OPTED_IN,
    CONSENT_STATE_OPTED_OUT,
    INBOUND_AUTHENTICITY_SIGNED,
    PROVIDER_EASYWEEK,
    Client,
    EasyWeekEvent,
    WhatsAppConsentSuppression,
    WhatsAppEvent,
    WhatsAppInboundReceipt,
    WhatsAppSender,
)
from altegio_bot.tests.easyweek_fixtures import TEST_CUSTOMER_ID, TEST_LOCATION_ID, booking_created
from altegio_bot.tests.test_easyweek_uuid_identity_worker import CUSTOMER, PHONE, Reader
from altegio_bot.tests.test_easyweek_uuid_identity_worker import (
    configuration as configuration,  # noqa: PLC0414 - autouse fixture, imported on purpose
)
from altegio_bot.whatsapp_consent import (
    CONSENT_COMMAND_STOP,
    apply_consent_decision,
    apply_inbound_consent,
    consent_decision,
    extend_refusal_to_uuid,
    final_send_guard,
    is_suppressed,
    lock_consent_address,
    record_inbound_receipts,
)
from altegio_bot.whatsapp_consent_cards import settle_card_writes
from altegio_bot.workers import easyweek_inbox_worker as worker
from altegio_bot.workers import whatsapp_inbox_worker as wa_worker

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
        # The card now CARRIES the refusal, which is the point of the card
        # protocol: the ledger is the authority, but ``Client.wa_opted_out`` is
        # what every campaign path reads, and a card adopted in front of a
        # refusal that says nothing would let marketing through. Written in the
        # refusing direction only, by the writer that holds the row.
        assert card.wa_opted_out is True
        assert card.wa_opt_out_reason == "wa:consent_ledger"
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


# ===========================================================================
# Round 2, review fix 3 — a refusal already ACCEPTED but not yet decided
# ===========================================================================


def _stop_payload(address: str, *, msg_id: str, epoch: int, text: str = "STOP") -> dict:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA-ADOPTION",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {"phone_number_id": "PNID-ADOPTION"},
                            "messages": [
                                {
                                    "from": address.lstrip("+"),
                                    "id": msg_id,
                                    "timestamp": str(epoch),
                                    "type": "text",
                                    "text": {"body": text},
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


async def _accept_without_deciding(
    session_maker,
    *,
    address: str = PHONE,
    msg_id: str = "wamid.PENDING",
    epoch: int = 1_760_000_000,
    text: str = "STOP",
) -> int:
    """The ingress half of a refusal: durable, unresolved, nobody has acted.

    Written through ``record_inbound_receipts`` — the function the webhook
    really calls, in one transaction with the event, before the 200 — because
    the whole point is the window between accepting a message and deciding it.
    """
    now = datetime.now(timezone.utc)
    payload = _stop_payload(address, msg_id=msg_id, epoch=epoch, text=text)
    async with session_maker() as session:
        async with session.begin():
            event = WhatsAppEvent(
                dedupe_key=f"wa:{msg_id}",
                status="received",
                payload=payload,
                inbound_authenticity=INBOUND_AUTHENTICITY_SIGNED,
                received_at=now,
            )
            session.add(event)
            await session.flush()
            await record_inbound_receipts(
                session,
                dedupe_key=str(event.dedupe_key),
                event_id=int(event.id),
                payload=payload,
                signature_verified=True,
                observed_at=now,
            )
            return int(event.id)


async def _decide(session_maker, event_id: int) -> None:
    """The worker half: the durable consent pass, in its own transaction."""
    await apply_inbound_consent(session_maker, event_id=event_id)


async def _move_the_card(session_maker, *, to: str = NEW_PHONE) -> None:
    async with session_maker() as session:
        async with session.begin():
            card = (
                await session.execute(
                    select(Client).where(
                        Client.provider == PROVIDER_EASYWEEK,
                        Client.easyweek_customer_uuid == uuid.UUID(CUSTOMER),
                    )
                )
            ).scalar_one()
            card.phone_e164 = to


async def _receipts(session_maker, address: str = PHONE) -> list[WhatsAppInboundReceipt]:
    async with session_maker() as session:
        return list(
            (
                await session.scalars(
                    select(WhatsAppInboundReceipt)
                    .where(WhatsAppInboundReceipt.wa_address == address)
                    .order_by(WhatsAppInboundReceipt.id)
                )
            ).all()
        )


async def test_a_pending_refusal_survives_the_number_changing_under_it(session_maker, monkeypatch) -> None:
    """The review's sequence: accepted, then the contact moves, then decided.

    The protection used to start at the suppression row, so this window was
    open: the ingress had the STOP on disk, the worker had not reached it, the
    adoption found nothing to bind, the number changed, and the refusal was then
    written against a number no card names — unreachable from the new one. The
    proof is stamped onto the pending receipt instead, and the decision carries
    it onto the row.
    """
    await _uuid_only_card(session_maker)
    event_id = await _accept_without_deciding(session_maker)
    _booking_id, payload = await _capture_booking(session_maker)

    await _adopt(session_maker, monkeypatch, payload)

    # The proof reached the message that had not been decided yet.
    [receipt] = await _receipts(session_maker)
    assert receipt.resolved_at is None
    assert receipt.linked_easyweek_customer_uuid == uuid.UUID(CUSTOMER)
    assert receipt.linked_uuid_recorded_at is not None
    assert await _suppression(session_maker) is None, "nothing was decided yet"

    await _move_the_card(session_maker)
    await _decide(session_maker, event_id)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    assert row.banned_easyweek_customer_uuid == uuid.UUID(CUSTOMER)
    assert row.evidence.get("uuid_binding") == "proven_uuid_link_on_receipt"

    async with session_maker() as session:
        assert await is_suppressed(session, phone=NEW_PHONE, easyweek_customer_uuid=uuid.UUID(CUSTOMER))
        assert await is_suppressed(session, phone=NEW_PHONE)
        guard = await final_send_guard(session, phone=NEW_PHONE, easyweek_customer_uuid=uuid.UUID(CUSTOMER))
    # No permission at the final boundary is the zero-POST guarantee: the
    # executor may not make a request without one.
    assert guard.allowed is False
    assert guard.reason == "consent_refusal_active"


async def test_the_other_worker_order_ends_in_the_same_place(session_maker, monkeypatch) -> None:
    """Decided before the number moves, which is the ordinary case.

    Here the decision itself finds the card, so the suppression row is bound
    when it is created and the pending link has nothing left to do. Both orders
    must end with the same refusal applying to the new number.
    """
    await _uuid_only_card(session_maker)
    event_id = await _accept_without_deciding(session_maker, msg_id="wamid.PENDING2")
    _booking_id, payload = await _capture_booking(session_maker)
    await _adopt(session_maker, monkeypatch, payload)
    await _decide(session_maker, event_id)
    await _move_the_card(session_maker)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.banned_easyweek_customer_uuid == uuid.UUID(CUSTOMER)
    async with session_maker() as session:
        guard = await final_send_guard(session, phone=NEW_PHONE, easyweek_customer_uuid=uuid.UUID(CUSTOMER))
    assert guard.allowed is False


async def test_the_link_survives_a_restart_between_every_step(session_maker, monkeypatch) -> None:
    """Every step in its own session, with the next one reading only the database.

    A restart between accepting the message, proving the UUID, moving the number
    and deciding the refusal has to be survivable, so nothing may be carried in
    memory. The link columns are read back from disk here between each step,
    which is the only place they could be.
    """
    await _uuid_only_card(session_maker)
    event_id = await _accept_without_deciding(session_maker, msg_id="wamid.PENDING3")
    _booking_id, payload = await _capture_booking(session_maker)
    await _adopt(session_maker, monkeypatch, payload)

    async with session_maker() as session:
        stored = (
            await session.execute(
                select(
                    WhatsAppInboundReceipt.linked_easyweek_customer_uuid,
                    WhatsAppInboundReceipt.resolved_at,
                ).where(WhatsAppInboundReceipt.whatsapp_event_id == event_id)
            )
        ).all()
    assert stored == [(uuid.UUID(CUSTOMER), None)]

    await _move_the_card(session_maker)

    async with session_maker() as session:
        # Nothing but the database connects the two now.
        assert (await session.scalar(select(func.count()).select_from(Client).where(Client.phone_e164 == PHONE))) == 0

    await _decide(session_maker, event_id)
    async with session_maker() as session:
        assert await is_suppressed(session, phone=NEW_PHONE)


async def test_a_pending_refusal_on_a_number_the_card_has_left_still_holds_the_send(session_maker, monkeypatch) -> None:
    """The second sequence: the historical link is there, the refusal is not yet.

    A refusal was recorded on the old number once and lifted by a proven START,
    so the suppression row survives as an ``opted_in`` row bound to this UUID.
    The card then moves to the new number and a FRESH STOP arrives on the old
    one. The guard for the new number used to see nothing — the pending set was
    looked up by the addresses the cards carry — and allowed the send while an
    accepted refusal sat unread.

    Today's card phones are not the history of the negative links, and the
    history is where the pending set has to be looked up.
    """
    await _uuid_only_card(session_maker)
    await _refuse(session_maker)
    moment = datetime.now(timezone.utc) - timedelta(days=1)
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, PHONE)
            lifted = await apply_consent_decision(
                session,
                wa_address=PHONE,
                command=CONSENT_COMMAND_START,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source=CONSENT_SOURCE_INBOUND,
                observed_at=moment,
                event_at=moment,
            )
    assert lifted.applied

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_IN
    assert row.banned_easyweek_customer_uuid == uuid.UUID(CUSTOMER)

    await _move_the_card(session_maker)
    await _accept_without_deciding(session_maker, msg_id="wamid.PENDING4")

    async with session_maker() as session:
        guard = await final_send_guard(session, phone=NEW_PHONE, easyweek_customer_uuid=uuid.UUID(CUSTOMER))
    assert guard.allowed is False
    assert guard.reason == "pending_inbound_unresolved"
    assert guard.decision.pending_inbound == 1
    assert PHONE in guard.decision.matched_addresses


async def test_an_unresolved_ordinary_message_on_the_old_number_also_holds(session_maker, monkeypatch) -> None:
    """Any unaccounted message, not only a STOP: the hold is about not knowing.

    The guard cannot tell what an undecided message says, and §46.4.6 says a
    queue that is behind must not become a permission.
    """
    await _uuid_only_card(session_maker)
    await _refuse(session_maker)
    await _move_the_card(session_maker)
    await _accept_without_deciding(session_maker, msg_id="wamid.PENDING5", text="danke!")

    async with session_maker() as session:
        guard = await final_send_guard(session, phone=NEW_PHONE, easyweek_customer_uuid=uuid.UUID(CUSTOMER))
    # The old refusal refuses on its own; what this asserts is that the
    # undecided message on the number the card has left is SEEN at all.
    assert guard.allowed is False
    assert guard.decision.pending_inbound == 1


async def test_the_link_is_never_positive_identity(session_maker, monkeypatch) -> None:
    """A link that bans must not start proving anything.

    No card is created or changed by the binding, the pending receipt gains no
    authority to decide who this person is, and a refusal is not invented.
    """
    await _uuid_only_card(session_maker)
    await _accept_without_deciding(session_maker, msg_id="wamid.PENDING6", text="guten Tag")
    _booking_id, payload = await _capture_booking(session_maker)
    await _adopt(session_maker, monkeypatch, payload)

    async with session_maker() as session:
        assert await session.scalar(select(func.count()).select_from(WhatsAppConsentSuppression)) == 0
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
        assert len(cards) == 1
        assert cards[0].wa_opted_out is False
        assert await is_suppressed(session, phone=PHONE) is False


async def test_a_pending_link_is_not_moved_onto_another_identity(session_maker, monkeypatch) -> None:
    """One number can be two people over time, so a stamped link is left alone."""
    other = uuid.uuid4()
    await _uuid_only_card(session_maker)
    await _accept_without_deciding(session_maker, msg_id="wamid.PENDING7")
    async with session_maker() as session:
        async with session.begin():
            await extend_refusal_to_uuid(session, addresses=[PHONE], customer_uuid=other)

    _booking_id, payload = await _capture_booking(session_maker)
    await _adopt(session_maker, monkeypatch, payload)

    [receipt] = await _receipts(session_maker)
    assert receipt.linked_easyweek_customer_uuid == other


# ===========================================================================
# Round 3, review fix 2 — the receipt is read again under the locks
# ===========================================================================


async def test_the_proof_that_lands_between_the_read_and_the_locks_is_not_lost(session_maker, monkeypatch) -> None:
    """The review's schedule, with the bridge really running inside the window.

    The consent worker loads the receipts, then takes the address locks. In
    between, the booking bridge proves a new number for this person, moves the
    card and stamps the proven UUID onto the still-pending receipt. The worker
    then went on with the ``None`` it had loaded BEFORE the locks existed — and a
    second ``SELECT`` would not have helped, because an instance this session
    already holds comes back from the identity map with its old attributes and
    taking a lock refreshes nothing.

    What that cost: a suppression bound to no UUID, written against a number no
    card carries any more, and a resolved receipt — which the send guard does
    not look at. The new number passed the guard.
    """
    import altegio_bot.whatsapp_consent as consent_module

    await _uuid_only_card(session_maker)
    event_id = await _accept_without_deciding(session_maker, msg_id="wamid.R3GAP")
    _booking_id, payload = await _capture_booking(session_maker)

    real = consent_module._receipts_for_event
    state = {"armed": True, "seen": None}

    async def _patched(session, event, *, refresh: bool = False):
        rows = await real(session, event, refresh=refresh)
        if state["armed"] and not refresh:
            # The discovery read: no lock is held yet, which is exactly the
            # window. The bridge runs to completion in its own transactions.
            state["armed"] = False
            state["seen"] = [row.linked_easyweek_customer_uuid for row in rows]
            await _adopt(session_maker, monkeypatch, payload)
            await _move_the_card(session_maker)
        return rows

    monkeypatch.setattr(consent_module, "_receipts_for_event", _patched)
    await _decide(session_maker, event_id)

    assert state["armed"] is False
    assert state["seen"] == [None], "the first read really did see no link"

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    assert row.banned_easyweek_customer_uuid == uuid.UUID(CUSTOMER)
    assert row.evidence.get("uuid_binding") == "proven_uuid_link_on_receipt"

    [receipt] = await _receipts(session_maker)
    assert receipt.resolved_at is not None
    assert receipt.resolution == "applied"
    assert receipt.linked_easyweek_customer_uuid == uuid.UUID(CUSTOMER)

    async with session_maker() as session:
        assert await is_suppressed(session, phone=NEW_PHONE, easyweek_customer_uuid=uuid.UUID(CUSTOMER))
        assert await is_suppressed(session, phone=NEW_PHONE)
        guard = await final_send_guard(session, phone=NEW_PHONE, easyweek_customer_uuid=uuid.UUID(CUSTOMER))
    assert guard.allowed is False
    assert guard.reason == "consent_refusal_active"


async def test_processing_the_same_event_again_after_that_window_changes_nothing(session_maker, monkeypatch) -> None:
    """Idempotence across the re-read: a second pass is a no-op."""
    await _uuid_only_card(session_maker)
    event_id = await _accept_without_deciding(session_maker, msg_id="wamid.R3GAP2")
    _booking_id, payload = await _capture_booking(session_maker)
    await _adopt(session_maker, monkeypatch, payload)
    await _move_the_card(session_maker)

    await _decide(session_maker, event_id)
    first = await _suppression(session_maker)
    assert first is not None
    version, bound = int(first.consent_version), first.banned_easyweek_customer_uuid

    await _decide(session_maker, event_id)
    second = await _suppression(session_maker)
    assert second is not None
    assert int(second.consent_version) == version
    assert second.banned_easyweek_customer_uuid == bound
    async with session_maker() as session:
        assert await is_suppressed(session, phone=NEW_PHONE)


async def test_the_other_order_of_those_two_workers_is_also_safe(session_maker, monkeypatch) -> None:
    """The bridge after the decision: the suppression finds the card itself."""
    await _uuid_only_card(session_maker)
    event_id = await _accept_without_deciding(session_maker, msg_id="wamid.R3GAP3")
    _booking_id, payload = await _capture_booking(session_maker)

    await _decide(session_maker, event_id)
    row = await _suppression(session_maker)
    assert row is not None
    assert row.banned_easyweek_customer_uuid == uuid.UUID(CUSTOMER)

    await _adopt(session_maker, monkeypatch, payload)
    await _move_the_card(session_maker)

    async with session_maker() as session:
        guard = await final_send_guard(session, phone=NEW_PHONE, easyweek_customer_uuid=uuid.UUID(CUSTOMER))
    assert guard.allowed is False


# ===========================================================================
# Round 4, review fix 1 — the locked card set is a checked fact
# ===========================================================================

THIRD_PHONE = "+4915100000931"
WA_PNID = "PNID-ADOPTION"


async def _altegio_card(session_maker, phone: str, *, altegio_client_id: int = 994001) -> int:
    """A plain Altegio recipient of the same delivery, untouched by any proof."""
    async with session_maker() as session:
        async with session.begin():
            card = Client(
                provider="altegio",
                company_id=1,
                altegio_client_id=altegio_client_id,
                phone_e164=phone,
                display_name="Third",
                raw={},
                wa_opted_out=False,
            )
            session.add(card)
            await session.flush()
            return int(card.id)


async def _seed_whatsapp_sender(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            existing = await session.scalar(select(WhatsAppSender.id).where(WhatsAppSender.phone_number_id == WA_PNID))
            if existing is None:
                session.add(
                    WhatsAppSender(
                        company_id=1,
                        sender_code="adoption",
                        phone_number_id=WA_PNID,
                        display_phone="+4930000007",
                        is_active=True,
                    )
                )


def _two_stop_payload(first: str, second: str, *, epoch: int = 1_760_000_000) -> dict:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA-ADOPTION",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {"phone_number_id": WA_PNID},
                            "messages": [
                                {
                                    "from": address.lstrip("+"),
                                    "id": f"wamid.R4-{index}",
                                    "timestamp": str(epoch + index),
                                    "type": "text",
                                    "text": {"body": "STOP"},
                                }
                                for index, address in enumerate((first, second))
                            ],
                        },
                    }
                ],
            }
        ],
    }


async def _accept_two_stops(session_maker, first: str, second: str) -> int:
    """Store one delivery carrying a STOP for each of two people."""
    now = datetime.now(timezone.utc)
    payload = _two_stop_payload(first, second)
    async with session_maker() as session:
        async with session.begin():
            event = WhatsAppEvent(
                dedupe_key="wa:r4-two-stops",
                status="received",
                payload=payload,
                inbound_authenticity=INBOUND_AUTHENTICITY_SIGNED,
                received_at=now,
            )
            session.add(event)
            await session.flush()
            await record_inbound_receipts(
                session,
                dedupe_key=str(event.dedupe_key),
                event_id=int(event.id),
                payload=payload,
                signature_verified=True,
                observed_at=now,
                allowed_phone_number_ids={WA_PNID},
            )
            return int(event.id)


async def _store_captured(session_maker, payload: dict) -> int:
    """A second captured event for the SAME booking, byte for byte."""
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
            return int(event.id)


class _CaptureProvider:
    """Counts what would have been sent. Never sends."""

    def __init__(self) -> None:
        self.sent: list[tuple[int, str, str]] = []

    async def send(self, sender_id: int, phone_e164: str, text: str, contact_name: str | None = None) -> str:
        self.sent.append((sender_id, phone_e164, text))
        return f"wamid.ACK{len(self.sent)}"

    async def send_template(self, *args, **kwargs) -> str:  # pragma: no cover - never reached
        raise AssertionError("an inbound command never sends a template")


async def _process_whatsapp(session_maker, event_id: int, provider) -> None:
    original = wa_worker.SessionLocal
    try:
        wa_worker.SessionLocal = session_maker  # type: ignore[assignment]
        await wa_worker.process_one_event(event_id, provider)
    finally:
        wa_worker.SessionLocal = original


async def _wait_until_a_transaction_is_queued(session_maker, *, timeout: float = 30.0) -> None:
    """Wait until PostgreSQL reports a transaction waiting for a lock.

    Every lock type counts: the orders under test mix transaction-scoped
    advisory locks with row locks on the card rows, and a wait for either one is
    a wait.
    """
    import asyncio

    from sqlalchemy import text as sql_text

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        async with session_maker() as probe:
            waiting = await probe.scalar(sql_text("SELECT count(*) FROM pg_locks WHERE NOT granted"))
        if int(waiting or 0) >= 1:
            return
        assert loop.time() < deadline, "nothing ever queued on a lock"
        await asyncio.sleep(0.05)


def _marketing_verdict(card: Client) -> str | None:
    from altegio_bot.campaigns.segment import compute_excluded_reason

    return compute_excluded_reason(
        wa_opted_out=bool(card.wa_opted_out),
        phone_e164=card.phone_e164,
        count_before=0,
        lash_count=1,
        attended_lash_count=1,
    )


def _booking_of(payload: dict):
    registry = identity.configured_easyweek_locations()
    return worker.normalize_event(
        event_hint="booking-created",
        payload=payload,
        body_truncated=False,
        location_registry=registry.locations,
    )


async def test_a_card_moving_onto_an_address_mid_projection_does_not_deadlock(session_maker, monkeypatch) -> None:
    """The review's schedule, with the real identity path on both sides.

    One delivery carries a STOP for each of two people. When the projection
    locks the cards it can find, only the second recipient has one — the
    EasyWeek card of the proven UUID is still on another number. A legitimate
    contact proof then moves that card ONTO the first address and commits, and a
    second proof of the same booking takes the moved row and waits for the
    consent lock the projection is holding.

    ``FOR UPDATE`` only protected the rows that matched when it ran, and the
    card write matched by phone — so it reached for the moved row's lock after
    the consent locks, inverting the one order this project keeps. PostgreSQL
    made the projection the victim: the ledger refused, the receipts were
    resolved, BOTH cards stayed sendable, the event stayed `processing` with no
    recovery pass, and the real campaign classifier went on allowing marketing
    to the recipient no proof had ever touched.

    Now the write touches only the locked ids, so no row lock can follow a
    consent lock, and the set is compared again under those locks so the moved
    card is neither missed nor dropped: the transaction restarts and locks it
    first.
    """
    await _uuid_only_card(session_maker, phone=NEW_PHONE)
    third_id = await _altegio_card(session_maker, THIRD_PHONE)
    await _seed_whatsapp_sender(session_maker)
    delivery = await _accept_two_stops(session_maker, PHONE, THIRD_PHONE)

    _first_event, payload = await _capture_booking(session_maker)
    second_event = await _store_captured(session_maker, payload)
    booking = _booking_of(payload)

    real = wa_worker._card_ids_matching
    state: dict[str, object] = {"moved": False, "task": None, "locked_first": None}

    async def _patched(session, phones, *, lock: bool):
        ids = await real(session, phones, lock=lock)
        if lock and not state["moved"]:
            state["moved"] = True
            state["locked_first"] = set(ids)
            # The first proof: it moves the card onto this delivery's first
            # address and commits, entirely outside the projection — and brings
            # the ledger's refusal with it, because that is now the writer's
            # obligation rather than something a later projection has to notice.
            assert await identity.resolve_webhook_identity(
                session_maker,
                event_id=_first_event,
                booking=booking,
                reader=Reader(payload, session_maker, check_lock=False),
            )
        return ids

    real_lock = wa_worker.lock_consent_addresses

    async def _patched_lock(session, addresses):
        locked = await real_lock(session, addresses)
        if state["task"] is None:
            # The projection now holds the consent locks. The second proof takes
            # the moved card row and queues on the consent lock — which is the
            # moment the old code asked for that same row from the other side.
            state["task"] = asyncio.create_task(
                identity.resolve_webhook_identity(
                    session_maker,
                    event_id=second_event,
                    booking=booking,
                    reader=Reader(payload, session_maker, check_lock=False),
                )
            )
            await _wait_until_a_transaction_is_queued(session_maker)
        return locked

    monkeypatch.setattr(wa_worker, "_card_ids_matching", _patched)
    monkeypatch.setattr(wa_worker, "lock_consent_addresses", _patched_lock)
    provider = _CaptureProvider()
    await _process_whatsapp(session_maker, delivery, provider)
    assert await asyncio.wait_for(state["task"], timeout=60) is True

    # The pre-lock really did miss the card that later matched.
    moved_card_id = None
    async with session_maker() as session:
        moved_card_id = int(
            await session.scalar(select(Client.id).where(Client.phone_e164 == PHONE, Client.provider == "easyweek"))
        )
    assert moved_card_id not in state["locked_first"], "the pre-lock really did miss it"

    # Both refusals are durable and both cards agree with them.
    for address in (PHONE, THIRD_PHONE):
        row = await _suppression(session_maker, address)
        assert row is not None, address
        assert row.state == CONSENT_STATE_OPTED_OUT, address
    async with session_maker() as session:
        moved = await session.get(Client, moved_card_id)
        third = await session.get(Client, third_id)
        event = await session.get(WhatsAppEvent, delivery)
        receipts = list(
            (
                await session.scalars(
                    select(WhatsAppInboundReceipt).where(WhatsAppInboundReceipt.whatsapp_event_id == delivery)
                )
            ).all()
        )
    assert moved.wa_opted_out is True
    assert third.wa_opted_out is True
    assert _marketing_verdict(third) == "opted_out"
    assert _marketing_verdict(moved) == "opted_out"
    assert event.status == "processed"
    assert [r.resolution for r in receipts] == ["applied", "applied"]
    assert all(r.resolved_at is not None for r in receipts)
    # Only the acknowledgement of the first message leaves, and nothing else:
    # the provider is a double and no external request is made anywhere here.
    assert len(provider.sent) == 1
    assert "abgemeldet" in provider.sent[0][2]
    async with session_maker() as session:
        for address in (PHONE, THIRD_PHONE):
            guard = await final_send_guard(session, phone=address)
            assert guard.allowed is False, address


async def test_the_same_delivery_processed_again_changes_nothing(session_maker, monkeypatch) -> None:
    """The reverse schedule: the card has already moved before the delivery runs.

    Nothing has to restart — the pre-lock finds both cards — and re-processing
    the event is a no-op on both the ledger and the cards.
    """
    await _uuid_only_card(session_maker, phone=NEW_PHONE)
    third_id = await _altegio_card(session_maker, THIRD_PHONE)
    await _seed_whatsapp_sender(session_maker)

    _first_event, payload = await _capture_booking(session_maker)
    assert await identity.resolve_webhook_identity(
        session_maker,
        event_id=_first_event,
        booking=_booking_of(payload),
        reader=Reader(payload, session_maker, check_lock=False),
    )

    delivery = await _accept_two_stops(session_maker, PHONE, THIRD_PHONE)
    provider = _CaptureProvider()
    await _process_whatsapp(session_maker, delivery, provider)

    async def _versions() -> list[tuple[str, int, bool]]:
        async with session_maker() as session:
            rows = list(
                (
                    await session.scalars(
                        select(WhatsAppConsentSuppression).order_by(WhatsAppConsentSuppression.wa_address)
                    )
                ).all()
            )
            cards = list((await session.scalars(select(Client).order_by(Client.id))).all())
        return [(str(r.wa_address), int(r.consent_version), r.state == CONSENT_STATE_OPTED_OUT) for r in rows] + [
            (str(c.phone_e164), int(c.id), bool(c.wa_opted_out)) for c in cards
        ]

    before = await _versions()
    await _process_whatsapp(session_maker, delivery, _CaptureProvider())
    assert await _versions() == before

    async with session_maker() as session:
        third = await session.get(Client, third_id)
    assert third.wa_opted_out is True
    assert _marketing_verdict(third) == "opted_out"


async def test_the_historical_projection_survives_a_moved_card_set(session_maker, monkeypatch) -> None:
    """The same protocol on the reconciliation path.

    Its write is scoped to the rows it locked, so no row lock follows a consent
    lock. A card moving onto the candidate's address in that window is not left
    behind and does not cost the candidate its transaction: the proof that moves
    it converges it against the ledger itself. The refusal lands once, the flag
    lands with it, and nothing is reported as unsettled.
    """
    import altegio_bot.whatsapp_consent_projection as projection_module

    await _uuid_only_card(session_maker, phone=NEW_PHONE)
    # A stored delivery nobody ever processed: the candidate this run is about.
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppEvent(
                    dedupe_key="wa:r4-historical",
                    status="processed",
                    payload=_stop_payload(PHONE, msg_id="wamid.R4HIST", epoch=1_760_000_000),
                    received_at=now,
                )
            )
    _first_event, payload = await _capture_booking(session_maker)
    booking = _booking_of(payload)

    real = projection_module._card_ids_matching
    state: dict[str, object] = {"moved": False, "locked_first": None}

    async def _patched(session, address, *, lock: bool):
        ids = await real(session, address, lock=lock)
        if lock and address == PHONE and not state["moved"]:
            state["moved"] = True
            state["locked_first"] = set(ids)
            assert await identity.resolve_webhook_identity(
                session_maker,
                event_id=_first_event,
                booking=booking,
                reader=Reader(payload, session_maker, check_lock=False),
            )
        return ids

    monkeypatch.setattr(projection_module, "_card_ids_matching", _patched)
    report = await projection_module.project(session_maker, apply=True)

    assert state["moved"] is True
    assert state["locked_first"] == set(), "the pre-lock really did find no card"
    assert report.refusals_written == 1
    row = await _suppression(session_maker, PHONE)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    async with session_maker() as session:
        moved = (
            await session.execute(select(Client).where(Client.phone_e164 == PHONE, Client.provider == "easyweek"))
        ).scalar_one()
    assert moved.wa_opted_out is True
    assert _marketing_verdict(moved) == "opted_out"
    async with session_maker() as session:
        guard = await final_send_guard(session, phone=PHONE)
    assert guard.allowed is False


async def test_a_card_moved_onto_a_refused_address_arrives_already_refused(session_maker) -> None:
    """The window the review reproduced, at its widest: no projection at all.

    The STOP is applied, the projection runs and finds no card, and only
    afterwards does the real Altegio importer move an existing card onto that
    number. No re-check inside the projection could have covered this — the
    projection was over. The importer asks the ledger itself, which is why the
    flag every campaign path reads agrees with the refusal.
    """
    from altegio_bot.workers.inbox_worker import upsert_client as altegio_upsert

    card_id = await _altegio_card(session_maker, "+4915100000955", altegio_client_id=994777)
    await _refuse(session_maker, PHONE)

    async with session_maker() as session:
        async with session.begin():
            moved = await altegio_upsert(
                session,
                1,
                {"id": 994777, "name": "Moved", "phone": PHONE},
            )
            await settle_card_writes(session)
    assert moved == card_id, "the real importer moved the existing card"

    async with session_maker() as session:
        card = await session.get(Client, card_id)
    assert card.phone_e164 == PHONE
    assert card.wa_opted_out is True
    assert card.wa_opt_out_reason == "wa:consent_ledger"
    assert _marketing_verdict(card) == "opted_out"


async def test_a_brand_new_card_on_a_refused_address_is_refused_too(session_maker) -> None:
    """The same obligation when the row is created rather than moved."""
    from altegio_bot.workers.inbox_worker import upsert_client as altegio_upsert

    await _refuse(session_maker, PHONE)
    async with session_maker() as session:
        async with session.begin():
            created = await altegio_upsert(
                session,
                1,
                {"id": 994888, "name": "Fresh", "phone": PHONE},
            )
            await settle_card_writes(session)

    async with session_maker() as session:
        card = await session.get(Client, created)
    assert card.wa_opted_out is True
    assert _marketing_verdict(card) == "opted_out"


async def test_a_card_moved_onto_an_address_with_no_refusal_stays_sendable(session_maker) -> None:
    """The negative half: the protocol only ever moves TO refusing."""
    from altegio_bot.workers.inbox_worker import upsert_client as altegio_upsert

    card_id = await _altegio_card(session_maker, "+4915100000956", altegio_client_id=994999)
    async with session_maker() as session:
        async with session.begin():
            await altegio_upsert(session, 1, {"id": 994999, "name": "Clean", "phone": PHONE})
            await settle_card_writes(session)

    async with session_maker() as session:
        card = await session.get(Client, card_id)
    assert card.phone_e164 == PHONE
    assert card.wa_opted_out is False
    assert card.wa_opt_out_reason is None
    assert _marketing_verdict(card) is None


async def test_the_importer_does_not_restamp_a_card_that_already_refuses(session_maker) -> None:
    """``wa_opted_out_at`` says when somebody refused, not when an import ran."""
    from altegio_bot.workers.inbox_worker import upsert_client as altegio_upsert

    stamped = datetime.now(timezone.utc) - timedelta(days=9)
    async with session_maker() as session:
        async with session.begin():
            card = Client(
                provider="altegio",
                company_id=1,
                altegio_client_id=995111,
                phone_e164="+4915100000957",
                display_name="Already out",
                raw={},
                wa_opted_out=True,
                wa_opted_out_at=stamped,
                wa_opt_out_reason="wa:stop",
            )
            session.add(card)
            await session.flush()
            card_id = int(card.id)
    await _refuse(session_maker, PHONE)

    async with session_maker() as session:
        async with session.begin():
            await altegio_upsert(session, 1, {"id": 995111, "name": "Already out", "phone": PHONE})
            await settle_card_writes(session)

    async with session_maker() as session:
        after = await session.get(Client, card_id)
    assert after.wa_opted_out is True
    assert after.wa_opted_out_at == stamped
    assert after.wa_opt_out_reason == "wa:stop"


async def test_a_stable_recipient_is_never_left_allowed_by_another_address_moving(session_maker, monkeypatch) -> None:
    """The review's second scenario, with the real importer in every window.

    One delivery carries a STOP for three addresses whose cards keep moving and
    for one stable recipient. The projection used to roll its whole transaction
    back when a card set moved and, after three attempts, give up — leaving even
    the stable recipient with the ledger refusing and the card flag saying
    sendable, the event `processed` with a `card_set_unstable` error and nothing
    scheduled to finish the job.

    There is no rollback any more, so the stable recipient is converged by the
    projection and the moving ones by the importer that moves them. Every one of
    the four ends refused, and the event carries no error.
    """
    from altegio_bot.workers.inbox_worker import upsert_client as altegio_upsert

    movers = ["+4915100000961", "+4915100000962", "+4915100000963"]
    stable = "+4915100000964"
    parked = [
        await _altegio_card(session_maker, f"+491510000097{index}", altegio_client_id=996000 + index)
        for index in range(len(movers))
    ]
    stable_id = await _altegio_card(session_maker, stable, altegio_client_id=996100)
    await _seed_whatsapp_sender(session_maker)

    now = datetime.now(timezone.utc)
    payload = {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA-ADOPTION",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {"phone_number_id": WA_PNID},
                            "messages": [
                                {
                                    "from": address.lstrip("+"),
                                    "id": f"wamid.R5-{index}",
                                    "timestamp": str(1_760_000_000 + index),
                                    "type": "text",
                                    "text": {"body": "STOP"},
                                }
                                for index, address in enumerate([*movers, stable])
                            ],
                        },
                    }
                ],
            }
        ],
    }
    async with session_maker() as session:
        async with session.begin():
            event = WhatsAppEvent(
                dedupe_key="wa:r5-many",
                status="received",
                payload=payload,
                inbound_authenticity=INBOUND_AUTHENTICITY_SIGNED,
                received_at=now,
            )
            session.add(event)
            await session.flush()
            await record_inbound_receipts(
                session,
                dedupe_key=str(event.dedupe_key),
                event_id=int(event.id),
                payload=payload,
                signature_verified=True,
                observed_at=now,
                allowed_phone_number_ids={WA_PNID},
            )
            delivery = int(event.id)

    real = wa_worker._card_ids_matching
    moved: list[str] = []

    async def _patched(session, phones, *, lock: bool):
        ids = await real(session, phones, lock=lock)
        if lock and len(moved) < len(movers):
            # One real card move per pass, through the production importer, in
            # exactly the window the review used.
            index = len(moved)
            moved.append(movers[index])
            async with session_maker() as writer:
                async with writer.begin():
                    await altegio_upsert(
                        writer,
                        1,
                        {"id": 996000 + index, "name": "Mover", "phone": movers[index]},
                    )
                    await settle_card_writes(writer)
        return ids

    monkeypatch.setattr(wa_worker, "_card_ids_matching", _patched)
    provider = _CaptureProvider()
    await _process_whatsapp(session_maker, delivery, provider)

    assert moved == movers[:1], "one window per projection transaction, and there is only one"
    # Whatever moved and whatever did not, all four refusals are durable and all
    # four cards agree with them.
    for address in [*movers, stable]:
        row = await _suppression(session_maker, address)
        assert row is not None, address
        assert row.state == CONSENT_STATE_OPTED_OUT, address
    async with session_maker() as session:
        event = await session.get(WhatsAppEvent, delivery)
        stable_card = await session.get(Client, stable_id)
        first_mover = await session.get(Client, parked[0])
    assert event.status == "processed"
    assert event.error is None
    assert stable_card.wa_opted_out is True
    assert _marketing_verdict(stable_card) == "opted_out"
    assert first_mover.phone_e164 == movers[0]
    assert first_mover.wa_opted_out is True
    assert _marketing_verdict(first_mover) == "opted_out"
    async with session_maker() as session:
        for address in [*movers, stable]:
            guard = await final_send_guard(session, phone=address)
            assert guard.allowed is False, address


async def test_the_importer_moving_a_card_twice_is_idempotent(session_maker) -> None:
    """Running the same import again changes nothing and loses nothing."""
    from altegio_bot.workers.inbox_worker import upsert_client as altegio_upsert

    card_id = await _altegio_card(session_maker, "+4915100000981", altegio_client_id=997001)
    await _refuse(session_maker, PHONE)
    for _ in range(2):
        async with session_maker() as session:
            async with session.begin():
                await altegio_upsert(session, 1, {"id": 997001, "name": "Twice", "phone": PHONE})
                await settle_card_writes(session)

    async with session_maker() as session:
        card = await session.get(Client, card_id)
        rows = list((await session.scalars(select(WhatsAppConsentSuppression))).all())
    assert card.wa_opted_out is True
    assert len(rows) == 1
    assert _marketing_verdict(card) == "opted_out"
