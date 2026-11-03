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
