"""The compact voucher-mailing report, and what "booked after the mailing" means.

The claim under test is deliberately narrow: a NEW booking was created
somewhere in this EasyWeek workspace after a send this application can prove
left it. Everything here exists to stop that claim growing into something it is
not — a conversion, a visit, a payment, or a redeemed voucher — and to stop it
being made on evidence that does not carry it.

So the tests come in pairs. One side proves the positive answer appears when it
is earned; the other proves the same screen says "недостаточно данных" rather
than crediting somebody when the send is unconfirmed, the identity is in
conflict, the payload is incomplete, the branch registry is missing, or the
booking predates the send and merely arrived late.

Nothing here reaches EasyWeek or Meta. The report is a read, and one of these
tests is about exactly that.
"""

from __future__ import annotations

import uuid as uuid_module
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from altegio_bot.campaigns.easyweek_voucher_production import booking_attribution as attribution
from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    RECIPIENT_BASIS_EARNED,
    RECIPIENT_BASIS_MANUAL,
    Client,
    EasyWeekEvent,
    EasyWeekVoucherProductionBatchItem,
    MessageJob,
    OutboxMessage,
)
from altegio_bot.tests.easyweek_booking_report_fixtures import (
    DURLACH_COMPANY_ID,
    EARNED_NUMERIC,
    ETTLINGEN_COMPANY_ID,
    FOREIGN_COMPANY_ID,
    add_branch_card,
    bind_numeric,
    booking_payload,
    capture,
    delivered_batch,
    frozen_batch,
    iso,
    item_rows,
    mixed_delivered_batch,
    provider_message_id,
    set_card_uuid,
    set_send_times,
    slot_customer_uuid,
    utc,
)
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    COMPANY_ID,
    CUSTOMER_UUIDS,
    PHONES,
    FakeReader,
    seed_production_preview,
)

CONTRACT_DOC = Path(__file__).resolve().parents[3] / "docs" / "easyweek" / "VOUCHER_MAILING_BOOKING_REPORT.md"
RUNBOOK_DOC = Path(__file__).resolve().parents[3] / "docs" / "easyweek" / "VOUCHER_PRODUCTION_MAILING_RUNBOOK.md"

REPORT_URL = "/ops/voucher-mailings/api/booking-report"
CLIENTS_URL = "/ops/voucher-mailings/api/booking-report/clients"
PAGE_URL = "/ops/voucher-mailings/report"

# The window the owner approved, named once here so a test reads like the rule.
WINDOW = timedelta(days=30)

# A send and a reading moment that are both in the PAST, so the HTTP routes —
# which read the real clock and cannot be handed one — see the same window the
# direct calls do.
SENT = utc(2026, 9, 1, 9, 0)
NOW = utc(2026, 10, 10, 9, 0)

# A numeric EasyWeek customer id for the one recipient most tests use. Nothing
# derives it: it is given to the recipient's own local card, which is the bridge
# the freeze proved.
NUMERIC = 773001
OTHER_NUMERIC = 773002


async def _one_sent_recipient(session_maker, *, count=1, basis=None):
    """One frozen, paid, sent recipient whose send is proven at ``SENT``.

    The stages are real; only the stored instants are moved afterwards, because
    a test about a thirty-day boundary has to be able to put the send somewhere
    other than "when this test ran".
    """
    reader = FakeReader(count=count)
    seed = {"bases": [basis] * count} if basis else {}
    run_id, batch_id, _request = await delivered_batch(session_maker, reader, count=count, **seed)
    for slot in range(1, count + 1):
        await set_send_times(session_maker, batch_id=batch_id, slot=slot, attempted_at=SENT, accepted_at=SENT)
        await bind_numeric(session_maker, batch_id=batch_id, slot=slot, customer_id=NUMERIC + slot - 1)
    return run_id, batch_id


async def _booking(session_maker, *, created_at, company_id=COMPANY_ID, customer_id=NUMERIC, received=None, **changes):
    """One captured ``booking-created`` delivery for a fresh booking."""
    booking_uuid = str(uuid_module.uuid4())
    await capture(
        session_maker,
        payload=booking_payload(
            booking_uuid=booking_uuid,
            customer_id=customer_id,
            created_at=iso(created_at),
            company_id=company_id,
            **changes,
        ),
        received_at=received or created_at,
    )
    return booking_uuid


async def _report(session_maker, *, batch_id=None, preview_run_id=None, now=NOW):
    return await attribution.build_report(session_maker, batch_id=batch_id, preview_run_id=preview_run_id, now=now)


def _row(report, slot=1):
    return next(row for row in report.rows if row.slot == slot)


# ===========================================================================
# 2. A preview with no batch, and a batch with no send
# ===========================================================================


async def test_a_preview_without_a_batch_is_not_shown_as_a_mailing(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """Nothing was frozen and nothing was sent. The report says both."""
    run_id, _ = await seed_production_preview(session_maker, count=3)

    report = await _report(session_maker, preview_run_id=run_id)

    assert report.report_known is True
    assert report.batch_exists is False
    assert report.batch_id is None
    assert report.recipient_count == 0
    # The preview's own size is still reported, so the screen is not blank.
    assert report.preview_active_count == 3
    assert report.rows == ()
    assert report.any_send_confirmed is False
    assert report.summary()["booked_after_send"] == 0


async def test_a_frozen_batch_with_no_send_attributes_nothing(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """A frozen list is not a sent one, and a booking cannot be credited to it."""
    reader = FakeReader(count=2)
    _run_id, batch_id, _request = await frozen_batch(session_maker, reader, count=2)
    await bind_numeric(session_maker, batch_id=batch_id, slot=1, customer_id=NUMERIC)
    await _booking(session_maker, created_at=utc(2026, 11, 5))

    report = await _report(session_maker, batch_id=batch_id)

    assert report.batch_exists is True
    assert report.any_send_confirmed is False
    assert all(row.attribution_state == attribution.SEND_NOT_ATTEMPTED for row in report.rows)
    assert all(row.message_state == attribution.MESSAGE_NOT_SENT for row in report.rows)
    assert report.summary()["booked_after_send"] == 0
    # And no scan was even attempted: without an anchor there is no window.
    assert report.scan.scanned_events == 0


async def test_an_unknown_batch_id_is_reported_as_unknown(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """A report for a mailing that does not exist is not an empty mailing."""
    report = await _report(session_maker, batch_id=424242)

    assert report.batch_exists is False
    assert attribution.BATCH_UNKNOWN in report.reasons


# ===========================================================================
# 8. No confirmed send, and a late Meta confirmation
# ===========================================================================


async def test_an_attempt_without_acceptance_gives_no_positive_attribution(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """One attempt is not a proven send, so the answer is a lack of data."""
    reader = FakeReader(count=1)
    _run_id, batch_id, _request = await delivered_batch(session_maker, reader, count=1)
    await set_send_times(
        session_maker, batch_id=batch_id, slot=1, attempted_at=SENT, accepted_at=None, delivered_at=None, read_at=None
    )
    await bind_numeric(session_maker, batch_id=batch_id, slot=1, customer_id=NUMERIC)
    await _booking(session_maker, created_at=utc(2026, 11, 5))

    row = _row(await _report(session_maker, batch_id=batch_id))

    assert row.send_anchor.state == attribution.SEND_ATTEMPT_UNCONFIRMED
    assert row.message_state == attribution.MESSAGE_ATTEMPT_UNCONFIRMED
    assert row.attribution_state == attribution.INSUFFICIENT_DATA
    assert attribution.SEND_ATTEMPT_UNCONFIRMED in row.reasons
    assert row.bookings == ()


async def test_a_late_acceptance_callback_does_not_move_the_window_start(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """The anchor is the ATTEMPT, whenever the acceptance was recorded.

    A callback is allowed to be the first thing that records acceptance. If the
    window started at the callback instead, a booking made the day after the
    send would fall outside it and an honest conversion would disappear.
    """
    reader = FakeReader(count=1)
    _run_id, batch_id, _request = await delivered_batch(session_maker, reader, count=1)
    await bind_numeric(session_maker, batch_id=batch_id, slot=1, customer_id=NUMERIC)
    # Acceptance recorded three weeks after the attempt, which is what a very
    # late callback looks like in the ledger.
    await set_send_times(
        session_maker, batch_id=batch_id, slot=1, attempted_at=SENT, accepted_at=SENT + timedelta(days=21)
    )
    created = SENT + timedelta(days=1)
    await _booking(session_maker, created_at=created)

    row = _row(await _report(session_maker, batch_id=batch_id))

    assert row.send_anchor.anchor == SENT
    window = row.send_anchor.window()
    assert window is not None and window[0] == SENT and window[1] == SENT + WINDOW
    assert row.attribution_state == attribution.BOOKING_FOUND


# ===========================================================================
# 1. Delivered / read callbacks, their repeats and their late arrival
# ===========================================================================


async def test_delivered_and_read_callbacks_are_separate_facts_and_repeat_harmlessly(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """The real webhook path, twice, then once more. Counters do not drift."""
    reader = FakeReader(count=2)
    _run_id, batch_id, _request = await delivered_batch(session_maker, reader, count=2)
    message_id = await provider_message_id(session_maker, batch_id=batch_id, slot=1)

    first = await _report(session_maker, batch_id=batch_id)
    assert first.summary()["provider_accepted"] == 2
    assert first.summary()["delivered_confirmed"] == 0
    assert first.summary()["read_confirmed"] == 0

    await ledger_module.record_webhook_transition(session_maker, provider_message_id=message_id, status="delivered")
    await ledger_module.record_webhook_transition(session_maker, provider_message_id=message_id, status="delivered")
    second = await _report(session_maker, batch_id=batch_id)
    assert second.summary()["delivered_confirmed"] == 1
    assert second.summary()["read_confirmed"] == 0
    assert _row(second).message_state == attribution.MESSAGE_DELIVERED

    await ledger_module.record_webhook_transition(session_maker, provider_message_id=message_id, status="read")
    # A duplicate read, and then a delivered that arrives AFTER the read: the
    # ledger refuses to walk backwards and the report follows it.
    await ledger_module.record_webhook_transition(session_maker, provider_message_id=message_id, status="read")
    await ledger_module.record_webhook_transition(session_maker, provider_message_id=message_id, status="delivered")
    third = await _report(session_maker, batch_id=batch_id)
    assert third.summary()["read_confirmed"] == 1
    assert third.summary()["delivered_confirmed"] == 1
    assert third.summary()["provider_accepted"] == 2
    assert _row(third).message_state == attribution.MESSAGE_READ
    # The three counters overlap and the report says so rather than implying
    # they partition the recipients.
    assert third.summary()["counters_overlap"] is True


async def test_a_read_callback_carries_the_send_anchor_when_acceptance_is_absent(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """Delivery evidence is enough to believe a message left.

    The schema makes `delivered` imply acceptance, so this is the shape where a
    callback recorded both at once. The anchor stays the attempt.
    """
    reader = FakeReader(count=1)
    _run_id, batch_id, _request = await delivered_batch(session_maker, reader, count=1)
    message_id = await provider_message_id(session_maker, batch_id=batch_id, slot=1)
    await ledger_module.record_webhook_transition(session_maker, provider_message_id=message_id, status="read")
    await set_send_times(
        session_maker,
        batch_id=batch_id,
        slot=1,
        attempted_at=SENT,
        accepted_at=SENT + timedelta(hours=4),
        delivered_at=SENT + timedelta(hours=4),
        read_at=SENT + timedelta(hours=5),
    )
    await bind_numeric(session_maker, batch_id=batch_id, slot=1, customer_id=NUMERIC)

    row = _row(await _report(session_maker, batch_id=batch_id))

    assert row.send_anchor.anchor == SENT
    assert "webhook_read" in row.send_anchor.evidence
    assert row.message_state == attribution.MESSAGE_READ


# ===========================================================================
# 3. Earned numeric-only and manual UUID-first recipients
# ===========================================================================


async def test_both_recipient_bases_are_attributed_from_the_frozen_proof(
    session_maker, production_configuration, workspace_branches, binding_key, monkeypatch
):
    """An earned numeric-only card and a manual UUID-first card both work.

    The batch is genuinely mixed: the earned member is proven live from its own
    source booking and history. Its local card has NO UUID column filled in,
    which is the production shape, so its bridge is the card the freeze
    re-proved. The manual member's card carries the UUID binding as well.
    """
    _run_id, batch_id, _request, _ids = await mixed_delivered_batch(session_maker, monkeypatch)
    manual_card = await bind_numeric(session_maker, batch_id=batch_id, slot=2, customer_id=OTHER_NUMERIC)
    await set_card_uuid(
        session_maker,
        client_id=manual_card,
        customer_uuid=await slot_customer_uuid(session_maker, batch_id=batch_id, slot=2),
    )
    for slot in (1, 2):
        await set_send_times(session_maker, batch_id=batch_id, slot=slot, attempted_at=SENT, accepted_at=SENT)
    await _booking(session_maker, created_at=SENT + timedelta(days=2), customer_id=EARNED_NUMERIC)
    await _booking(session_maker, created_at=SENT + timedelta(days=3), customer_id=OTHER_NUMERIC)

    report = await _report(session_maker, batch_id=batch_id)

    earned = _row(report, 1)
    manual = _row(report, 2)
    assert earned.recipient_basis == RECIPIENT_BASIS_EARNED
    assert earned.identity.state == attribution.IDENTITY_PROVEN
    # Numeric-only: the only bridge is the card the freeze bound to this slot.
    assert earned.identity.sources == (attribution.SOURCE_FROZEN_LOCAL_CARD,)
    assert earned.attribution_state == attribution.BOOKING_FOUND
    assert manual.recipient_basis == RECIPIENT_BASIS_MANUAL
    assert manual.identity.state == attribution.IDENTITY_PROVEN
    assert attribution.SOURCE_CLIENT_UUID_BINDING in manual.identity.sources
    assert manual.attribution_state == attribution.BOOKING_FOUND
    assert report.summary()["booked_after_send"] == 2


# ===========================================================================
# 4. Another branch, and one person with cards in several branches
# ===========================================================================


async def test_a_booking_in_another_branch_of_the_workspace_counts(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """The mailing is Karlsruhe only; the analytics are the whole workspace."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    await _booking(session_maker, created_at=SENT + timedelta(days=4), company_id=DURLACH_COMPANY_ID)

    report = await _report(session_maker, batch_id=batch_id)
    row = _row(report)

    assert row.attribution_state == attribution.BOOKING_FOUND
    assert row.nearest is not None
    assert row.nearest.branch_name == "durlach"
    assert row.nearest.company_id == DURLACH_COMPANY_ID
    # And the coverage block names every configured branch, not only this one.
    assert {entry["name"] for entry in report.branches} == {"karlsruhe", "durlach", "ettlingen"}


async def test_one_customer_with_cards_in_two_branches_is_one_recipient(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """Two local cards, one proven UUID, one numeric id — and one conversion."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    proven = await slot_customer_uuid(session_maker, batch_id=batch_id, slot=1)
    await add_branch_card(
        session_maker,
        company_id=DURLACH_COMPANY_ID,
        customer_uuid=proven,
        customer_id=NUMERIC,
        phone=PHONES[0],
    )
    await _booking(session_maker, created_at=SENT + timedelta(days=2), company_id=DURLACH_COMPANY_ID)
    await _booking(session_maker, created_at=SENT + timedelta(days=5), company_id=ETTLINGEN_COMPANY_ID)

    report = await _report(session_maker, batch_id=batch_id)
    row = _row(report)

    assert row.identity.state == attribution.IDENTITY_PROVEN
    assert set(row.identity.card_company_ids) == {COMPANY_ID, DURLACH_COMPANY_ID}
    assert len(row.bookings) == 2
    # Two bookings, two branches, ONE recipient in the counter.
    assert report.summary()["booked_after_send"] == 1
    # Neither card was merged or rewritten by reading the report.
    async with session_maker() as session:
        cards = list(
            (
                await session.scalars(
                    select(Client.company_id).where(
                        Client.provider == "easyweek", Client.easyweek_customer_uuid == uuid_module.UUID(proven)
                    )
                )
            ).all()
        )
    assert sorted(cards) == sorted([DURLACH_COMPANY_ID])


# ===========================================================================
# 5. Any service, including one notifications would never send for
# ===========================================================================


async def test_any_service_counts_including_one_outside_the_notification_rules(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """No lash filter, no category filter, no two-service rule. Any booking."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    await _booking(
        session_maker,
        created_at=SENT + timedelta(days=6),
        service_name="Beratungsgespräch",
        service_category=None,
    )

    row = _row(await _report(session_maker, batch_id=batch_id))

    assert row.attribution_state == attribution.BOOKING_FOUND


# ===========================================================================
# 6. Several bookings and repeated events: one recipient in the counter
# ===========================================================================


async def test_several_bookings_and_repeated_deliveries_count_one_recipient(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """Three bookings, each delivered twice. One recipient, three bookings."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    for day in (2, 7, 19):
        created = SENT + timedelta(days=day)
        booking_uuid = str(uuid_module.uuid4())
        payload = booking_payload(booking_uuid=booking_uuid, customer_id=NUMERIC, created_at=iso(created))
        await capture(session_maker, payload=payload, received_at=created)
        # The same delivery again: a retry, a Resend, or both.
        await capture(session_maker, payload=payload, received_at=created + timedelta(minutes=5))

    report = await _report(session_maker, batch_id=batch_id)
    row = _row(report)

    assert len(row.bookings) == 3
    assert report.summary()["booked_after_send"] == 1
    assert row.nearest is not None
    assert row.nearest.booking_created_at == SENT + timedelta(days=2)


async def test_a_reschedule_is_not_a_second_booking(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """Moving an appointment changes its time, not the number of bookings."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    created = SENT + timedelta(days=3)
    booking_uuid = str(uuid_module.uuid4())
    await capture(
        session_maker,
        payload=booking_payload(booking_uuid=booking_uuid, customer_id=NUMERIC, created_at=iso(created)),
        received_at=created,
    )
    await capture(
        session_maker,
        payload=booking_payload(
            booking_uuid=booking_uuid,
            customer_id=NUMERIC,
            created_at=iso(created),
            booking_date_start="2026-12-02T10:00:00+0000",
        ),
        event_hint="booking-rescheduled",
        received_at=created + timedelta(days=1),
    )

    row = _row(await _report(session_maker, batch_id=batch_id))

    assert len(row.bookings) == 1
    assert row.bookings[0].state == attribution.BOOKING_ACTIVE
    assert row.bookings[0].booking_created_at == created


# ===========================================================================
# 7. A booking created before the send whose event arrived afterwards
# ===========================================================================


async def test_a_late_event_about_a_booking_created_before_the_send_is_not_counted(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """The window is checked on the creation instant, not on the delivery."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    await _booking(
        session_maker,
        created_at=SENT - timedelta(hours=2),
        received=SENT + timedelta(days=3),
    )

    row = _row(await _report(session_maker, batch_id=batch_id))

    assert row.attribution_state == attribution.BOOKING_NOT_OBSERVED
    assert row.bookings == ()


# ===========================================================================
# 9. The thirty-day boundaries, aware datetimes and DST
# ===========================================================================


async def test_the_window_excludes_the_send_instant_and_includes_its_last_moment(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """`send < created <= send + 30d`, at all four edges."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)

    async def attributed(created):
        async with session_maker() as session:
            async with session.begin():
                await session.execute(EasyWeekEvent.__table__.delete())
        await _booking(session_maker, created_at=created, received=max(created, SENT) + timedelta(hours=1))
        return _row(await _report(session_maker, batch_id=batch_id)).attribution_state

    assert await attributed(SENT) == attribution.BOOKING_NOT_OBSERVED
    assert await attributed(SENT + timedelta(seconds=1)) == attribution.BOOKING_FOUND
    assert await attributed(SENT + WINDOW) == attribution.BOOKING_FOUND
    assert await attributed(SENT + WINDOW + timedelta(seconds=1)) == attribution.BOOKING_NOT_OBSERVED


async def test_a_dst_change_moves_the_local_clock_and_not_the_boundary(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """The window is a duration from an instant; the rendering is local.

    The send sits before Europe/Berlin leaves summer time and the end of the
    window sits after it, so the two boundaries print with different offsets
    while the instants themselves are exactly thirty days apart.
    """
    berlin = ZoneInfo("Europe/Berlin")
    # Across the spring transition: winter time at the send, summer time at the
    # end of the window.
    sent = utc(2026, 3, 20, 9, 0)
    reader = FakeReader(count=1)
    _run_id, batch_id, _request = await delivered_batch(session_maker, reader, count=1)
    await set_send_times(session_maker, batch_id=batch_id, slot=1, attempted_at=sent, accepted_at=sent)
    await bind_numeric(session_maker, batch_id=batch_id, slot=1, customer_id=NUMERIC)
    # Created at the very last instant of the window, after the clocks changed.
    await _booking(session_maker, created_at=sent + WINDOW)

    row = _row(await _report(session_maker, batch_id=batch_id, now=utc(2026, 5, 1)))
    window = row.send_anchor.window()

    assert window is not None
    assert window[1] - window[0] == WINDOW
    # Winter time at the start, summer time at the end: a real DST step.
    assert window[0].astimezone(berlin).utcoffset() == timedelta(hours=1)
    assert window[1].astimezone(berlin).utcoffset() == timedelta(hours=2)
    # The local wall clock therefore differs by an hour between the two ends.
    assert window[0].astimezone(berlin).strftime("%H:%M") == "10:00"
    assert window[1].astimezone(berlin).strftime("%H:%M") == "11:00"
    assert row.attribution_state == attribution.BOOKING_FOUND
    # Every stored instant stays aware.
    assert all(booking.booking_created_at.tzinfo is not None for booking in row.bookings)


# ===========================================================================
# 10. Cancellation, completion, and a new booking after a cancellation
# ===========================================================================


async def test_a_cancelled_booking_keeps_the_observed_fact_and_shows_its_state(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """ "Created a booking" happened. "Is coming" is a different sentence."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    created = SENT + timedelta(days=2)
    booking_uuid = str(uuid_module.uuid4())
    payload = booking_payload(booking_uuid=booking_uuid, customer_id=NUMERIC, created_at=iso(created))
    await capture(session_maker, payload=payload, received_at=created)
    await capture(
        session_maker, payload=payload, event_hint="booking-canceled", received_at=created + timedelta(days=1)
    )

    row = _row(await _report(session_maker, batch_id=batch_id))

    assert row.attribution_state == attribution.BOOKING_FOUND
    assert len(row.bookings) == 1
    assert row.bookings[0].state == attribution.BOOKING_CANCELLED


async def test_one_cancelled_and_one_active_booking_are_both_visible(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """A cancellation followed by a new booking is two bookings, not one."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    cancelled_at = SENT + timedelta(days=2)
    cancelled_uuid = str(uuid_module.uuid4())
    payload = booking_payload(booking_uuid=cancelled_uuid, customer_id=NUMERIC, created_at=iso(cancelled_at))
    await capture(session_maker, payload=payload, received_at=cancelled_at)
    await capture(
        session_maker, payload=payload, event_hint="booking-canceled", received_at=cancelled_at + timedelta(hours=2)
    )
    await _booking(session_maker, created_at=SENT + timedelta(days=9))

    row = _row(await _report(session_maker, batch_id=batch_id))

    assert [booking.state for booking in row.bookings] == [
        attribution.BOOKING_CANCELLED,
        attribution.BOOKING_ACTIVE,
    ]
    assert row.attribution_state == attribution.BOOKING_FOUND


async def test_a_completed_visit_is_never_claimed_from_a_creation_alone(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """Only a succeeded delivery makes a booking "completed"."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    created = SENT + timedelta(days=2)
    first = booking_payload(booking_uuid=str(uuid_module.uuid4()), customer_id=NUMERIC, created_at=iso(created))
    await capture(session_maker, payload=first, received_at=created)
    second = booking_payload(booking_uuid=str(uuid_module.uuid4()), customer_id=NUMERIC, created_at=iso(created))
    await capture(session_maker, payload=second, received_at=created)
    await capture(
        session_maker, payload=second, event_hint="booking-succeeded", received_at=created + timedelta(days=4)
    )

    row = _row(await _report(session_maker, batch_id=batch_id))

    states = sorted(booking.state for booking in row.bookings)
    assert states == [attribution.BOOKING_ACTIVE, attribution.BOOKING_COMPLETED]


async def test_an_update_after_a_cancellation_does_not_revive_it(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """An edit is not a resurrection: nothing proves that transition."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    created = SENT + timedelta(days=2)
    booking_uuid = str(uuid_module.uuid4())
    payload = booking_payload(booking_uuid=booking_uuid, customer_id=NUMERIC, created_at=iso(created))
    await capture(session_maker, payload=payload, received_at=created)
    await capture(
        session_maker, payload=payload, event_hint="booking-canceled", received_at=created + timedelta(hours=1)
    )
    await capture(
        session_maker, payload=payload, event_hint="booking-updated", received_at=created + timedelta(hours=2)
    )

    row = _row(await _report(session_maker, batch_id=batch_id))

    assert row.bookings[0].state == attribution.BOOKING_CANCELLED


# ===========================================================================
# 11. A booking reassigned to another client
# ===========================================================================


async def test_a_reassigned_booking_is_never_credited_to_the_new_client(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """The creation event names the owner; a later owner is not a conversion."""
    reader = FakeReader(count=2)
    _run_id, batch_id, _request = await delivered_batch(session_maker, reader, count=2)
    for slot, numeric in ((1, NUMERIC), (2, OTHER_NUMERIC)):
        await set_send_times(session_maker, batch_id=batch_id, slot=slot, attempted_at=SENT, accepted_at=SENT)
        await bind_numeric(session_maker, batch_id=batch_id, slot=slot, customer_id=numeric)

    created = SENT + timedelta(days=2)
    booking_uuid = str(uuid_module.uuid4())
    await capture(
        session_maker,
        payload=booking_payload(booking_uuid=booking_uuid, customer_id=NUMERIC, created_at=iso(created)),
        received_at=created,
    )
    # The salon moves the appointment to a different customer.
    await capture(
        session_maker,
        payload=booking_payload(booking_uuid=booking_uuid, customer_id=OTHER_NUMERIC, created_at=iso(created)),
        event_hint="booking-updated",
        received_at=created + timedelta(days=1),
    )

    report = await _report(session_maker, batch_id=batch_id)

    # Nobody new is credited.
    assert _row(report, 2).attribution_state == attribution.BOOKING_NOT_OBSERVED
    # The original creation is still the historical fact, and the reassignment
    # is visible rather than hidden.
    first = _row(report, 1)
    assert first.attribution_state == attribution.BOOKING_FOUND
    assert first.bookings[0].customer_changed_later is True
    assert report.summary()["booked_after_send"] == 1


# ===========================================================================
# 12. Unknown and conflicting identity, incomplete payloads, partial coverage
# ===========================================================================


async def test_a_recipient_with_no_numeric_id_is_reported_as_missing_data(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """No bridge, no answer. Not a zero, and not somebody else's booking."""
    reader = FakeReader(count=1)
    _run_id, batch_id, _request = await delivered_batch(session_maker, reader, count=1)
    await set_send_times(session_maker, batch_id=batch_id, slot=1, attempted_at=SENT, accepted_at=SENT)
    # A UUID-only card: the shared CHECK requires one of the two identities, so
    # "no numeric id" is a card that carries the proven UUID and nothing else.
    card = await bind_numeric(session_maker, batch_id=batch_id, slot=1, customer_id=NUMERIC)
    await set_card_uuid(
        session_maker,
        client_id=card,
        customer_uuid=await slot_customer_uuid(session_maker, batch_id=batch_id, slot=1),
    )
    await bind_numeric(session_maker, batch_id=batch_id, slot=1, customer_id=None)
    await _booking(session_maker, created_at=SENT + timedelta(days=2))

    row = _row(await _report(session_maker, batch_id=batch_id))

    assert row.identity.state == attribution.IDENTITY_NUMERIC_MISSING
    assert row.attribution_state == attribution.INSUFFICIENT_DATA
    assert row.bookings == ()


async def test_two_numeric_ids_for_one_proven_uuid_are_a_conflict(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """An ambiguous bridge credits nobody and says the identity is unknown."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    proven = await slot_customer_uuid(session_maker, batch_id=batch_id, slot=1)
    await add_branch_card(
        session_maker,
        company_id=DURLACH_COMPANY_ID,
        customer_uuid=proven,
        customer_id=OTHER_NUMERIC,
        phone="+4915177990001",
    )
    await _booking(session_maker, created_at=SENT + timedelta(days=2))

    row = _row(await _report(session_maker, batch_id=batch_id))

    assert row.identity.state == attribution.IDENTITY_CONFLICT
    assert row.attribution_state == attribution.INSUFFICIENT_DATA
    assert row.bookings == ()


async def test_a_numeric_id_bound_to_another_person_is_a_conflict(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """The reverse direction matters: the number must not be somebody else's."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    await add_branch_card(
        session_maker,
        company_id=ETTLINGEN_COMPANY_ID,
        customer_uuid=CUSTOMER_UUIDS[30],
        customer_id=NUMERIC,
        phone="+4915177990002",
    )
    await _booking(session_maker, created_at=SENT + timedelta(days=2))

    row = _row(await _report(session_maker, batch_id=batch_id))

    assert row.identity.state == attribution.IDENTITY_CONFLICT
    assert row.attribution_state == attribution.INSUFFICIENT_DATA


async def test_an_incomplete_or_truncated_payload_never_becomes_a_positive_fact(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """Four different gaps, four counted reasons, and no conversion."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    created = SENT + timedelta(days=2)
    # No creation instant.
    await capture(
        session_maker,
        payload=booking_payload(booking_uuid=str(uuid_module.uuid4()), customer_id=NUMERIC, created_at=None),
        received_at=created,
    )
    # No customer.
    await capture(
        session_maker,
        payload=booking_payload(booking_uuid=str(uuid_module.uuid4()), customer_id=None, created_at=iso(created)),
        received_at=created,
    )
    # Truncated: the missing tail could hold the very fields above.
    await capture(
        session_maker,
        payload=booking_payload(booking_uuid=str(uuid_module.uuid4()), customer_id=NUMERIC, created_at=iso(created)),
        received_at=created,
        body_truncated=True,
    )
    # A naive creation instant has no defensible interpretation.
    await capture(
        session_maker,
        payload=booking_payload(
            booking_uuid=str(uuid_module.uuid4()), customer_id=NUMERIC, created_at="2026-11-03T10:00:00"
        ),
        received_at=created,
    )

    report = await _report(session_maker, batch_id=batch_id)
    row = _row(report)

    assert row.attribution_state == attribution.BOOKING_NOT_OBSERVED
    assert row.bookings == ()
    assert report.scan.rejected == {
        attribution.REJECT_CREATED_AT: 2,
        attribution.REJECT_CUSTOMER_ID: 1,
        attribution.REJECT_TRUNCATED: 1,
    }


async def test_two_disagreeing_creation_events_are_not_a_conversion(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """One booking cannot have been created twice by two different people."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    created = SENT + timedelta(days=2)
    booking_uuid = str(uuid_module.uuid4())
    await capture(
        session_maker,
        payload=booking_payload(booking_uuid=booking_uuid, customer_id=NUMERIC, created_at=iso(created)),
        received_at=created,
    )
    await capture(
        session_maker,
        payload=booking_payload(booking_uuid=booking_uuid, customer_id=OTHER_NUMERIC, created_at=iso(created)),
        received_at=created + timedelta(minutes=1),
    )

    report = await _report(session_maker, batch_id=batch_id)

    assert _row(report).attribution_state == attribution.BOOKING_NOT_OBSERVED
    assert report.scan.rejected[attribution.REJECT_CONFLICTING_CREATION] == 1


async def test_branch_coverage_is_reported_per_configured_branch(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """A zero is "nothing observed", and the page is told to say exactly that."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    await _booking(session_maker, created_at=SENT + timedelta(days=2), company_id=COMPANY_ID)
    await _booking(session_maker, created_at=SENT + timedelta(days=3), company_id=DURLACH_COMPANY_ID)

    report = await _report(session_maker, batch_id=batch_id)
    observed = {entry["name"]: entry["bookings_observed"] for entry in report.branches}

    assert observed == {"karlsruhe": 1, "durlach": 1, "ettlingen": 0}
    assert report.scan.scan_from is not None and report.scan.scan_from < SENT
    assert report.scan.scan_to == NOW


async def test_an_unreadable_branch_registry_refuses_both_answers(
    session_maker, production_configuration, binding_key, monkeypatch
):
    """Without a proven workspace there is neither a yes nor a no."""
    from altegio_bot.settings import settings

    _run_id, batch_id = await _one_sent_recipient(session_maker)
    await _booking(session_maker, created_at=SENT + timedelta(days=2))
    monkeypatch.setattr(settings, "easyweek_location_map", "{not json", raising=False)

    report = await _report(session_maker, batch_id=batch_id)

    assert report.registry_ready is False
    assert attribution.REGISTRY_UNAVAILABLE in report.reasons
    assert _row(report).attribution_state == attribution.INSUFFICIENT_DATA
    assert report.summary()["booked_after_send"] == 0


# ===========================================================================
# 13. Reading the report twice, with no external effect of any kind
# ===========================================================================


async def test_reading_the_report_twice_changes_nothing_and_sends_nothing(
    session_maker, production_configuration, workspace_branches, binding_key, transports
):
    """The report is a read. Not of the ledger, not of the queue, not of Meta.

    The dispatch transport seam is replaced by a stub with nothing installed in
    it, so any attempt to open a reader, a mutator or a sender while a report is
    being built raises instead of quietly succeeding. The stages above supply
    their fakes directly, so the stub is untouched until the report runs.
    """
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    await _booking(session_maker, created_at=SENT + timedelta(days=2))

    before = await item_rows(session_maker, batch_id=batch_id)
    fingerprint = [
        (
            row.status,
            row.send_attempt_count,
            row.send_attempted_at,
            row.provider_accepted_at,
            row.delivered_at,
            row.read_at,
            row.voucher_code_hmac,
            row.hmac_key_id,
            row.target_order_uuid,
            row.updated_at,
        )
        for row in before
    ]
    async with session_maker() as session:
        counts_before = (
            int(await session.scalar(select(func.count()).select_from(MessageJob)) or 0),
            int(await session.scalar(select(func.count()).select_from(OutboxMessage)) or 0),
            int(await session.scalar(select(func.count()).select_from(EasyWeekEvent)) or 0),
            int(await session.scalar(select(func.count()).select_from(Client)) or 0),
        )

    first = await _report(session_maker, batch_id=batch_id)
    second = await _report(session_maker, batch_id=batch_id)

    assert first.as_safe_dict()["rows"] == second.as_safe_dict()["rows"]
    after = await item_rows(session_maker, batch_id=batch_id)
    assert [
        (
            row.status,
            row.send_attempt_count,
            row.send_attempted_at,
            row.provider_accepted_at,
            row.delivered_at,
            row.read_at,
            row.voucher_code_hmac,
            row.hmac_key_id,
            row.target_order_uuid,
            row.updated_at,
        )
        for row in after
    ] == fingerprint
    async with session_maker() as session:
        counts_after = (
            int(await session.scalar(select(func.count()).select_from(MessageJob)) or 0),
            int(await session.scalar(select(func.count()).select_from(OutboxMessage)) or 0),
            int(await session.scalar(select(func.count()).select_from(EasyWeekEvent)) or 0),
            int(await session.scalar(select(func.count()).select_from(Client)) or 0),
        )
    assert counts_after == counts_before


async def test_reading_the_report_fills_no_customer_uuid(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """An earned numeric-only card stays numeric-only."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    card = await bind_numeric(session_maker, batch_id=batch_id, slot=1, customer_id=NUMERIC)
    await set_card_uuid(session_maker, client_id=card, customer_uuid=None)

    await _report(session_maker, batch_id=batch_id)

    async with session_maker() as session:
        stored = await session.scalar(select(Client.easyweek_customer_uuid).where(Client.id == card))
    assert stored is None


# ===========================================================================
# 14. Auth, isolation and redaction
# ===========================================================================


async def test_the_report_api_refuses_an_unauthenticated_request(anon_client):
    """Ops auth guards the report exactly as it guards the rest of the section."""
    answer = await anon_client.get(f"{REPORT_URL}?batch_id=1", headers={"Accept": "application/json"})
    assert answer.status_code == 401
    names = await anon_client.get(f"{CLIENTS_URL}?batch_id=1", headers={"Accept": "application/json"})
    assert names.status_code == 401
    page = await anon_client.get(f"{PAGE_URL}?batch_id=1", headers={"Accept": "text/html"})
    assert page.status_code in (302, 303, 307)
    assert "/ops/login" in page.headers.get("location", "")


async def test_an_unscoped_report_is_refused(ui_client):
    """No batch and no preview is not "every mailing"."""
    answer = await ui_client.get(REPORT_URL)
    assert answer.status_code == 400
    assert answer.json()["report_known"] is False


async def test_an_altegio_booking_is_never_counted(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """Provider isolation: an Altegio client sharing a number proves nothing."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider=PROVIDER_ALTEGIO,
                    company_id=COMPANY_ID,
                    altegio_client_id=NUMERIC,
                    easyweek_customer_uuid=None,
                    phone_e164="+4915177990003",
                    display_name="Altegio card",
                    raw={},
                )
            )
    # A delivery from a location that is not in this workspace's registry.
    await _booking(session_maker, created_at=SENT + timedelta(days=2), company_id=FOREIGN_COMPANY_ID)

    report = await _report(session_maker, batch_id=batch_id)

    assert _row(report).attribution_state == attribution.BOOKING_NOT_OBSERVED
    assert report.scan.rejected[attribution.REJECT_FOREIGN_LOCATION] == 1


async def test_a_location_id_that_does_not_match_its_uuid_is_refused(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """A configured numeric id with the wrong workspace UUID is not this branch."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    await _booking(
        session_maker,
        created_at=SENT + timedelta(days=2),
        company_id=COMPANY_ID,
        location_uuid="8395fab6-7ee8-4702-88d9-fd78f925bbbb",
    )

    report = await _report(session_maker, batch_id=batch_id)

    assert _row(report).attribution_state == attribution.BOOKING_NOT_OBSERVED
    assert report.scan.rejected[attribution.REJECT_LOCATION_MISMATCH] == 1


async def test_the_report_body_carries_no_code_no_uuid_and_no_payload(
    session_maker, production_configuration, workspace_branches, binding_key, ui_client
):
    """What a report may print: slots, codes, states, branches and dates."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    proven = await slot_customer_uuid(session_maker, batch_id=batch_id, slot=1)
    booking_uuid = await _booking(session_maker, created_at=SENT + timedelta(days=2))

    answer = await ui_client.get(f"{REPORT_URL}?batch_id={batch_id}")
    assert answer.status_code == 200
    body = answer.text

    for secret in (proven, booking_uuid, "voucher_code", "hmac", "payload"):
        assert secret.lower() not in body.lower(), secret
    async with session_maker() as session:
        rows = await item_rows(session_maker, batch_id=batch_id)
        del session
    for row in rows:
        for value in (
            row.voucher_code_hmac,
            row.hmac_key_id,
            str(row.target_order_uuid),
            str(row.outbound_intent_uuid),
        ):
            if value and value != "None":
                assert value not in body
    # The private headers are on it, so it never lands in a shared cache.
    assert "no-store" in answer.headers.get("cache-control", "")


async def test_the_names_endpoint_is_the_only_place_a_person_is_named(
    session_maker, production_configuration, workspace_branches, binding_key, ui_client
):
    """PII lives in one narrow answer, for one authenticated operator."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)

    report = (await ui_client.get(f"{REPORT_URL}?batch_id={batch_id}")).text
    assert "Synthetic Production" not in report
    assert PHONES[0] not in report

    names = await ui_client.get(f"{CLIENTS_URL}?batch_id={batch_id}")
    assert names.status_code == 200
    clients = names.json()["clients"]
    assert clients[0]["display_name"].startswith("Synthetic Production")
    assert clients[0]["phone_e164"] == PHONES[0]
    assert "no-store" in names.headers.get("cache-control", "")


async def test_the_page_escapes_a_client_name_and_a_branch_name(
    session_maker, production_configuration, workspace_branches, binding_key, ui_client
):
    """Markup in stored data is text. The page builds no HTML out of it."""
    from sqlalchemy import update

    from altegio_bot.models.models import CampaignRecipient

    _run_id, batch_id = await _one_sent_recipient(session_maker)
    async with session_maker() as session:
        async with session.begin():
            recipient_id = await session.scalar(
                select(EasyWeekVoucherProductionBatchItem.campaign_recipient_id).where(
                    EasyWeekVoucherProductionBatchItem.batch_id == batch_id
                )
            )
            await session.execute(
                update(CampaignRecipient)
                .where(CampaignRecipient.id == recipient_id)
                .values(display_name="<script>alert(1)</script>")
            )

    names = await ui_client.get(f"{CLIENTS_URL}?batch_id={batch_id}")
    assert names.json()["clients"][0]["display_name"] == "<script>alert(1)</script>"
    # The page itself never interpolates a name: it is written with textContent,
    # so the served HTML contains no name at all.
    page = await ui_client.get(f"{PAGE_URL}?batch_id={batch_id}")
    assert page.status_code == 200
    assert "<script>alert(1)</script>" not in page.text


# ===========================================================================
# 15. Historical batches, and the contracts this report must not touch
# ===========================================================================


async def test_a_historical_batch_reports_without_changing_its_contract(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """The €15 schema keeps its amounts, its bindings and its stop semantics."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    snapshot_before = await ledger_module.load(session_maker, batch_id=batch_id)

    report = await _report(session_maker, batch_id=batch_id)

    assert report.schema_version == snapshot_before.schema_version
    snapshot_after = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot_after.as_safe_dict() == snapshot_before.as_safe_dict()
    assert snapshot_after.voucher_unit_price_minor == snapshot_before.voucher_unit_price_minor
    assert snapshot_after.frozen_digest == snapshot_before.frozen_digest


async def test_booking_states_never_replace_execution_statuses(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """`booked` is a separate column of thought, not a new item status."""
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    await _booking(session_maker, created_at=SENT + timedelta(days=2))

    report = await _report(session_maker, batch_id=batch_id)
    row = _row(report)

    assert row.attribution_state == attribution.BOOKING_FOUND
    # The execution status is still the ledger's own word, untouched.
    assert row.item_status == "provider_accepted"
    rows = await item_rows(session_maker, batch_id=batch_id)
    assert rows[0].status == "provider_accepted"
    assert attribution.BOOKING_FOUND not in {entry.status for entry in rows}


async def test_the_policy_and_the_window_are_printed_and_not_configurable(
    session_maker, production_configuration, workspace_branches, binding_key
):
    """A report says which rule produced it, and no setting can change it."""
    from altegio_bot.settings import settings

    _run_id, batch_id = await _one_sent_recipient(session_maker)

    payload = (await _report(session_maker, batch_id=batch_id)).as_safe_dict()

    assert payload["policy_version"] == "easyweek-booking-report-v1"
    assert payload["attribution_window_days"] == 30
    # There is deliberately no setting for either of them: a late configuration
    # change must not quietly redefine what an old report meant.
    assert not [name for name in dir(settings) if "attribution" in name.lower()]


async def test_the_page_names_the_zone_once_and_does_not_repeat_utc(
    session_maker, production_configuration, workspace_branches, binding_key, ui_client, monkeypatch
):
    """One zone, stated beside the report, with local times in the table."""
    from altegio_bot.settings import settings

    # The salon's configured zone. Set here rather than assumed, because the
    # value is deployment configuration and a test must not depend on whichever
    # zone this machine happens to carry.
    monkeypatch.setattr(settings, "ops_local_tz", "Europe/Berlin", raising=False)
    _run_id, batch_id = await _one_sent_recipient(session_maker)
    await _booking(session_maker, created_at=SENT + timedelta(days=2))

    page = await ui_client.get(f"{PAGE_URL}?batch_id={batch_id}")
    assert page.status_code == 200
    assert page.text.count("Europe/Berlin") == 1
    assert "UTC" not in page.text

    body = (await ui_client.get(f"{REPORT_URL}?batch_id={batch_id}")).json()
    assert body["local_timezone"] == "Europe/Berlin"
    row = body["rows"][0]
    # Local strings for the table, and the aware instant kept beside them for
    # anything that compares rather than prints.
    assert row["bookings"][0]["booking_created_at_local"] == "03.09.2026 11:00"
    assert row["bookings"][0]["booking_created_at"].endswith("+00:00")


# ===========================================================================
# The contract itself, in tracked documentation
# ===========================================================================


def test_the_tracked_contract_states_the_claim_the_window_and_the_limits():
    """The report's meaning is written down where a reviewer can find it.

    Tracked, deliberately: the canonical plan is a local working document, so a
    rule that exists only there cannot be the contract this code answers to.
    """
    contract = CONTRACT_DOC.read_text(encoding="utf-8")

    assert attribution.ATTRIBUTION_POLICY_VERSION in contract
    assert "Attribution window: **30 days**" in contract
    assert "send_anchor < booking_created_at <= send_anchor + 30 days" in contract
    # The four things the claim is NOT.
    for limit in ("advertising influence", "of a visit", "of a payment", "voucher being used"):
        assert limit in contract, limit
    # And the rules a later reader is most likely to get wrong.
    for rule in (
        "never** the anchor",
        "Acceptance, delivery and reading overlap",
        "absence of a confirmation",
        "a matching name or phone is **never** a bridge",
        "configured EasyWeek location registry",
        "A zero means",
    ):
        assert rule in contract, rule


def test_the_runbook_tells_the_operator_what_the_report_does_not_prove():
    """The operator-facing text carries the same limits, not just the code."""
    runbook = RUNBOOK_DOC.read_text(encoding="utf-8")

    assert "### 7.1 The compact report" in runbook
    assert "VOUCHER_MAILING_BOOKING_REPORT.md" in runbook
    for limit in (
        "not** proof that the advert worked",
        "does not move the start of the window",
        "A cancellation does not erase the fact",
        "not proof that there was no booking",
    ):
        assert limit in runbook, limit


async def test_an_unknown_mailing_number_is_not_drawn_as_an_empty_one(ui_client):
    """The served page carries the refusal for an id that names nothing."""
    body = (await ui_client.get(f"{REPORT_URL}?batch_id=999999")).json()

    assert body["report_known"] is True
    assert body["batch_exists"] is False
    assert attribution.BATCH_UNKNOWN in body["reasons"]
    assert body["rows"] == []
    # And the page knows how to say that, rather than reusing the "not frozen
    # yet" wording that belongs to a real preview.
    page = await ui_client.get(f"{PAGE_URL}?batch_id=999999")
    assert "Рассылка с таким номером не найдена" in page.text
