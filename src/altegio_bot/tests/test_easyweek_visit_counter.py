"""PostgreSQL contract for the PR-11 EasyWeek visit counter.

``booking-succeeded`` is EasyWeek stating a fact: this customer has now had
``visits_total`` completed visits. PR-11 stores that number and nothing else.

Two properties carry the whole design, and most of this file exists to pin them:

* **It is a snapshot, never a tally.** A Resend, a replay with a different
  payload hash and a genuine second visit are indistinguishable at the level of
  "a webhook arrived". ``current + 1`` would diverge from EasyWeek on the very
  first Resend; storing the stated total makes all three converge.
* **Identity is re-derived from the database every time.** The payload names a
  customer; only the Record and Client rows can prove which one, in which
  branch, under which provider.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import altegio_bot.db as app_db
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_normalizer import canonical_booking_uuid
from altegio_bot.models.models import Client, EasyWeekEvent, MessageJob, Record
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import (
    TEST_BOOKING_ID,
    TEST_BOOKING_UUID,
    TEST_CUSTOMER_ID,
    TEST_LOCATION_ID,
    TEST_LOCATION_UUID,
    booking_canceled,
    booking_created,
    booking_updated,
)
from altegio_bot.workers import easyweek_inbox_worker as worker

# A SECOND synthetic booking for the same customer: two bookings are what put
# two transactions on one Client row, which is the only way to contend the lock.
SECOND_BOOKING_UUID = "22222222-3333-4444-8555-666666666666"
SECOND_BOOKING_ID = 4200002


@pytest.fixture(autouse=True)
def _base_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Processing on, every optional consumer off — the production default."""
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_reviews_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_visit_counter_enabled", False, raising=False)
    monkeypatch.setattr(
        settings,
        "easyweek_allowed_service_categories",
        json.dumps(["Fixture Category"]),
        raising=False,
    )
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "test-branch": {
                    "location_id": TEST_LOCATION_ID,
                    "location_uuid": TEST_LOCATION_UUID,
                    "meta_template_prefix": "tb",
                    "booking_page_url": "https://booking.example.invalid/test",
                }
            }
        ),
        raising=False,
    )


@pytest_asyncio.fixture
async def bound_session_local(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> async_sessionmaker[AsyncSession]:
    monkeypatch.setattr(app_db, "SessionLocal", session_maker, raising=False)
    monkeypatch.setattr(worker, "SessionLocal", session_maker, raising=False)
    return session_maker


def _counter_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_visit_counter_enabled", True, raising=False)


def _reviews_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_reviews_enabled", True, raising=False)


def _succeeded(*, visits_total: Any = 1, **overrides) -> dict[str, Any]:
    payload = booking_created()
    payload["booking_status"] = "Succeeded appointment"
    payload["visits_total"] = visits_total
    payload.update(overrides)
    return payload


async def _capture(
    session: AsyncSession,
    payload: dict[str, Any],
    *,
    event_hint: str,
    payload_hash: str,
    truncated: bool = False,
) -> int:
    event = EasyWeekEvent(
        status="captured",
        event_hint=event_hint,
        auth_via="query",
        payload_hash=payload_hash,
        payload=payload,
        body_truncated=truncated,
        booking_uuid=canonical_booking_uuid(payload),
    )
    session.add(event)
    await session.flush()
    return int(event.id)


async def _run_until_idle(limit: int = 20) -> int:
    processed = 0
    for _ in range(limit):
        if not await worker.process_one():
            break
        processed += 1
    return processed


async def _seed_booking(session_maker) -> None:
    """One proven Record + Client, created the way production creates them."""
    async with session_maker() as session:
        async with session.begin():
            await _capture(session, booking_created(), event_hint="booking-created", payload_hash="created-1")
    await _run_until_idle()


async def _client_row(session_maker) -> Client:
    async with session_maker() as session:
        return (await session.execute(select(Client).where(Client.provider == "easyweek"))).scalars().one()


async def _record_row(session_maker) -> Record:
    async with session_maker() as session:
        return (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()


async def _event_rows(session_maker) -> list[EasyWeekEvent]:
    async with session_maker() as session:
        return list((await session.execute(select(EasyWeekEvent).order_by(EasyWeekEvent.id))).scalars().all())


async def _deliver_succeeded(session_maker, *, visits_total: Any = 1, payload_hash: str = "succeeded-1", **overrides):
    async with session_maker() as session:
        async with session.begin():
            await _capture(
                session,
                _succeeded(visits_total=visits_total, **overrides),
                event_hint="booking-succeeded",
                payload_hash=payload_hash,
            )
    return await _run_until_idle()


# ===========================================================================
# The counter itself
# ===========================================================================


async def test_the_first_proven_snapshot_is_stored(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    await _seed_booking(bound_session_local)

    await _deliver_succeeded(bound_session_local, visits_total=3)

    client = await _client_row(bound_session_local)
    assert client.easyweek_visits_total == 3
    assert client.easyweek_visits_total_updated_at is not None
    assert client.provider == "easyweek"
    assert client.company_id == TEST_LOCATION_ID


async def test_an_exact_resend_does_not_move_the_counter(bound_session_local, monkeypatch) -> None:
    """The same delivery, byte for byte. A tally would read this as a second visit."""
    _counter_on(monkeypatch)
    await _seed_booking(bound_session_local)

    await _deliver_succeeded(bound_session_local, visits_total=3, payload_hash="succeeded-1")
    first = await _client_row(bound_session_local)
    stamp = first.easyweek_visits_total_updated_at

    await _deliver_succeeded(bound_session_local, visits_total=3, payload_hash="succeeded-1")

    client = await _client_row(bound_session_local)
    assert client.easyweek_visits_total == 3
    assert client.easyweek_visits_total_updated_at == stamp


async def test_a_replay_with_a_different_payload_hash_does_not_move_the_counter(
    bound_session_local, monkeypatch
) -> None:
    """A different hash is a different DELIVERY, not a different visit."""
    _counter_on(monkeypatch)
    await _seed_booking(bound_session_local)

    await _deliver_succeeded(bound_session_local, visits_total=3, payload_hash="succeeded-1")
    stamp = (await _client_row(bound_session_local)).easyweek_visits_total_updated_at

    # Same stated total, different body (EasyWeek re-rendered a mutable field).
    await _deliver_succeeded(
        bound_session_local,
        visits_total=3,
        payload_hash="succeeded-2",
        booking_status="Succeeded appointment ",
    )

    client = await _client_row(bound_session_local)
    assert client.easyweek_visits_total == 3
    assert client.easyweek_visits_total_updated_at == stamp, "an unchanged value must not look freshly proven"


async def test_a_higher_snapshot_moves_the_counter_forward(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    await _seed_booking(bound_session_local)

    await _deliver_succeeded(bound_session_local, visits_total=3, payload_hash="succeeded-1")
    stamp = (await _client_row(bound_session_local)).easyweek_visits_total_updated_at

    await _deliver_succeeded(bound_session_local, visits_total=5, payload_hash="succeeded-2")

    client = await _client_row(bound_session_local)
    assert client.easyweek_visits_total == 5
    assert client.easyweek_visits_total_updated_at > stamp, "an accepted value refreshes the timestamp"


async def test_a_lower_snapshot_never_walks_the_counter_back(bound_session_local, monkeypatch) -> None:
    """Late or out-of-order deliveries must not undo a proven visit.

    PR-12 will read this number to decide whether a customer already came back.
    Going backwards would re-open a decision that was correctly closed.
    """
    _counter_on(monkeypatch)
    await _seed_booking(bound_session_local)

    await _deliver_succeeded(bound_session_local, visits_total=5, payload_hash="succeeded-1")
    stamp = (await _client_row(bound_session_local)).easyweek_visits_total_updated_at

    await _deliver_succeeded(bound_session_local, visits_total=2, payload_hash="succeeded-old")

    client = await _client_row(bound_session_local)
    assert client.easyweek_visits_total == 5
    assert client.easyweek_visits_total_updated_at == stamp
    # The stale delivery is still a valid succeeded event and reaches a
    # terminal state rather than blocking its booking.
    assert [e.status for e in await _event_rows(bound_session_local)][-1] == "processed"


async def _seed_second_booking(session_maker) -> None:
    """A SECOND EasyWeek booking for the SAME customer, in the same branch.

    Two bookings, not two deliveries of one: the predecessor rule serialises
    everything sharing a `booking_uuid`, so a single booking can never reach the
    Client row lock from two transactions at once. Only two distinct bookings
    that resolve to one Client put two writers on the same row.
    """
    payload = booking_created()
    payload["uid"] = SECOND_BOOKING_UUID
    payload["id"] = SECOND_BOOKING_ID
    payload["booking_hash_id"] = "90000002"
    payload["booking_page"] = "https://eyw.me/r/90000002"
    async with session_maker() as session:
        async with session.begin():
            await _capture(session, payload, event_hint="booking-created", payload_hash="created-2")
    await _run_until_idle()


def _second_succeeded(*, visits_total: int) -> dict[str, Any]:
    payload = _succeeded(visits_total=visits_total)
    payload["uid"] = SECOND_BOOKING_UUID
    payload["id"] = SECOND_BOOKING_ID
    return payload


async def test_two_bookings_of_one_client_serialise_on_the_client_row_lock(bound_session_local, monkeypatch) -> None:
    """The lower snapshot must not overwrite the higher one, whatever the order.

    This is a real lock test, and it is built to FAIL if
    ``Client.with_for_update()`` is removed from ``record_visit_counter``.

    The two transactions are interleaved deliberately rather than merely started
    together:

    * T1 (visits_total=6) enters ``record_visit_counter``, takes the Client row
      lock and then holds its transaction open;
    * T2 (visits_total=4) starts while T1 still holds it, and is made to commit
      LAST.

    With the lock, T2 blocks inside its own SELECT until T1 commits, then re-reads
    6, sees 4 < 6 and refuses — final value 6. Without it, T2 reads the pre-T1
    snapshot (no counter yet), writes 4 and commits after T1 — final value 4, and
    this test fails. Committing last is the part that matters: `asyncio.gather`
    alone would let T2 finish first and the assertion would pass either way.
    """
    _counter_on(monkeypatch)
    await _seed_booking(bound_session_local)
    await _seed_second_booking(bound_session_local)

    async with bound_session_local() as session:
        async with session.begin():
            high_event_id = await _capture(
                session,
                _succeeded(visits_total=6),
                event_hint="booking-succeeded",
                payload_hash="succeeded-high",
            )
            low_event_id = await _capture(
                session,
                _second_succeeded(visits_total=4),
                event_hint="booking-succeeded",
                payload_hash="succeeded-low",
            )

    # Both bookings must really resolve to ONE client row, or the lock is never
    # contended and the test proves nothing.
    async with bound_session_local() as session:
        records = list(
            (await session.execute(select(Record).where(Record.provider == "easyweek").order_by(Record.id)))
            .scalars()
            .all()
        )
    assert len(records) == 2
    assert {str(r.easyweek_booking_uuid) for r in records} == {TEST_BOOKING_UUID, SECOND_BOOKING_UUID}
    assert {r.altegio_record_id for r in records} == {TEST_BOOKING_ID, SECOND_BOOKING_ID}
    assert len({r.company_id for r in records}) == 1
    assert len({r.client_id for r in records}) == 1, "one client, two bookings"

    registry = configured_easyweek_locations()
    holder_took_the_lock = asyncio.Event()
    holder_committed = asyncio.Event()

    async def _writes_six() -> None:
        async with bound_session_local() as session:
            async with session.begin():
                event = await session.get(EasyWeekEvent, high_event_id)
                await worker.record_visit_counter(session, event=event, registry=registry)
                holder_took_the_lock.set()
                # Hold the transaction open so the other writer meets the lock.
                await asyncio.sleep(0.3)
        holder_committed.set()

    async def _writes_four() -> None:
        await holder_took_the_lock.wait()
        async with bound_session_local() as session:
            async with session.begin():
                event = await session.get(EasyWeekEvent, low_event_id)
                # With the row lock this call blocks here until T1 commits.
                await worker.record_visit_counter(session, event=event, registry=registry)
                # Commit LAST either way, so an unlocked read-modify-write would
                # land its stale 4 on top of the 6.
                await asyncio.wait_for(holder_committed.wait(), timeout=10)

    await asyncio.wait_for(asyncio.gather(_writes_six(), _writes_four()), timeout=30)

    client = await _client_row(bound_session_local)
    assert client.easyweek_visits_total == 6, "the lower snapshot must never overwrite the higher"

    # Both deliveries still reach a correct terminal state through the worker,
    # and re-processing them changes nothing.
    stamp = client.easyweek_visits_total_updated_at
    await _run_until_idle()

    final = await _client_row(bound_session_local)
    statuses = {e.payload_hash: e.status for e in await _event_rows(bound_session_local)}
    assert final.easyweek_visits_total == 6
    assert final.easyweek_visits_total_updated_at == stamp
    assert statuses["succeeded-high"] == "processed"
    assert statuses["succeeded-low"] == "processed"


async def test_one_booking_still_serialises_through_the_predecessor_rule(bound_session_local, monkeypatch) -> None:
    """The lifecycle contract for a single booking is unchanged.

    Kept alongside the lock test: two deliveries of the SAME booking must still
    be ordered by the predecessor rule, and the highest snapshot must still win.
    """
    _counter_on(monkeypatch)
    await _seed_booking(bound_session_local)

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(
                session,
                _succeeded(visits_total=4),
                event_hint="booking-succeeded",
                payload_hash="succeeded-a",
            )
            await _capture(
                session,
                _succeeded(visits_total=6),
                event_hint="booking-succeeded",
                payload_hash="succeeded-b",
            )

    await asyncio.gather(_run_until_idle(), _run_until_idle())

    client = await _client_row(bound_session_local)
    assert client.easyweek_visits_total == 6
    statuses = [e.status for e in await _event_rows(bound_session_local)]
    assert statuses.count("processed") == 3


# ===========================================================================
# Identity is proved, never assumed
# ===========================================================================


async def test_a_booking_uuid_from_another_branch_writes_nothing(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    await _seed_booking(bound_session_local)

    # The Record now belongs to TEST_LOCATION_ID; move the branch under it.
    async with bound_session_local() as session:
        async with session.begin():
            record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
            record.company_id = TEST_LOCATION_ID + 1

    await _deliver_succeeded(bound_session_local, visits_total=9)

    client = await _client_row(bound_session_local)
    assert client.easyweek_visits_total is None
    assert client.easyweek_visits_total_updated_at is None


async def test_a_booking_id_mismatch_writes_nothing(bound_session_local, monkeypatch) -> None:
    """Same UUID, different numeric booking id: one of them is not this booking."""
    _counter_on(monkeypatch)
    await _seed_booking(bound_session_local)

    async with bound_session_local() as session:
        async with session.begin():
            record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
            record.altegio_record_id = TEST_BOOKING_ID + 77

    await _deliver_succeeded(bound_session_local, visits_total=9)

    assert (await _client_row(bound_session_local)).easyweek_visits_total is None


async def test_a_customer_id_that_is_not_this_client_writes_nothing(bound_session_local, monkeypatch) -> None:
    """The Record's client must be the customer the payload names."""
    _counter_on(monkeypatch)
    await _seed_booking(bound_session_local)

    await _deliver_succeeded(bound_session_local, visits_total=9, customer_id=TEST_CUSTOMER_ID + 1)

    assert (await _client_row(bound_session_local)).easyweek_visits_total is None


async def test_a_succeeded_delivery_without_a_record_writes_nothing(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)

    processed = await _deliver_succeeded(bound_session_local, visits_total=3)

    assert processed == 1, "a succeeded delivery we cannot place is still a valid delivery"
    async with bound_session_local() as session:
        assert (
            await session.execute(select(func.count()).select_from(Client).where(Client.provider == "easyweek"))
        ).scalar_one() == 0
    assert [e.status for e in await _event_rows(bound_session_local)] == ["processed"]


async def test_a_record_without_a_client_writes_nothing(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    await _seed_booking(bound_session_local)

    async with bound_session_local() as session:
        async with session.begin():
            record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
            record.client_id = None

    await _deliver_succeeded(bound_session_local, visits_total=3)

    assert (await _client_row(bound_session_local)).easyweek_visits_total is None


@pytest.mark.parametrize(
    "visits_total",
    [
        pytest.param(0, id="zero"),
        pytest.param(-1, id="negative"),
        pytest.param(3.0, id="float"),
        pytest.param("3", id="numeric_string"),
        pytest.param(True, id="bool"),
        pytest.param(2147483648, id="overflow"),
    ],
)
async def test_an_unusable_visits_total_writes_nothing_but_still_terminalizes(
    bound_session_local, monkeypatch, visits_total
) -> None:
    """Fail-closed for the counter, not for the event.

    The delivery still proves a finished visit — failing it would also destroy
    the review PR-9/PR-10 may owe from the same event.
    """
    _counter_on(monkeypatch)
    await _seed_booking(bound_session_local)

    await _deliver_succeeded(bound_session_local, visits_total=visits_total)

    assert (await _client_row(bound_session_local)).easyweek_visits_total is None
    assert [e.status for e in await _event_rows(bound_session_local)][-1] == "processed"


async def test_a_missing_visits_total_writes_nothing_but_still_terminalizes(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    await _seed_booking(bound_session_local)

    payload = _succeeded()
    payload.pop("visits_total")
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, payload, event_hint="booking-succeeded", payload_hash="succeeded-1")
    await _run_until_idle()

    assert (await _client_row(bound_session_local)).easyweek_visits_total is None
    assert [e.status for e in await _event_rows(bound_session_local)][-1] == "processed"


async def test_the_altegio_clients_are_never_touched(bound_session_local, monkeypatch) -> None:
    """conftest seeds two Altegio clients. EasyWeek's counter is not theirs."""
    _counter_on(monkeypatch)
    await _seed_booking(bound_session_local)
    await _deliver_succeeded(bound_session_local, visits_total=7)

    async with bound_session_local() as session:
        altegio = list((await session.execute(select(Client).where(Client.provider == "altegio"))).scalars().all())
    assert altegio, "the baseline Altegio clients must still exist"
    for client in altegio:
        assert client.easyweek_visits_total is None
        assert client.easyweek_visits_total_updated_at is None


# ===========================================================================
# Flag matrix — one effective contract for claim, predecessor and processing
# ===========================================================================


async def test_both_consumers_off_defers_without_starving_the_booking(bound_session_local, monkeypatch) -> None:
    """`booking-succeeded` is captured, not destroyed, and holds nothing up."""
    await _seed_booking(bound_session_local)

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, _succeeded(), event_hint="booking-succeeded", payload_hash="succeeded-1")
            await _capture(session, booking_updated(), event_hint="booking-updated", payload_hash="updated-1")

    await _run_until_idle()

    rows = {e.payload_hash: e for e in await _event_rows(bound_session_local)}
    assert rows["succeeded-1"].status == "captured", "no enabled consumer, so it is never claimed"
    assert rows["succeeded-1"].processed_at is None
    assert rows["updated-1"].status == "processed", "a later lifecycle event of the SAME booking still flows"
    assert (await _client_row(bound_session_local)).easyweek_visits_total is None


async def test_a_deferred_succeeded_is_picked_up_once_the_counter_is_enabled(bound_session_local, monkeypatch) -> None:
    """Nothing is lost while the flag is off — it is waiting, not discarded."""
    await _seed_booking(bound_session_local)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, _succeeded(visits_total=2), event_hint="booking-succeeded", payload_hash="s-1")
    await _run_until_idle()
    assert (await _event_rows(bound_session_local))[-1].status == "captured"

    _counter_on(monkeypatch)
    await _run_until_idle()

    assert (await _event_rows(bound_session_local))[-1].status == "processed"
    assert (await _client_row(bound_session_local)).easyweek_visits_total == 2


async def test_the_counter_alone_creates_no_message_job(bound_session_local, monkeypatch) -> None:
    """Counter on, every notification fence shut. Nothing may be queued."""
    _counter_on(monkeypatch)
    await _seed_booking(bound_session_local)

    await _deliver_succeeded(bound_session_local, visits_total=4)

    async with bound_session_local() as session:
        jobs = (
            await session.execute(select(func.count()).select_from(MessageJob).where(MessageJob.provider == "easyweek"))
        ).scalar_one()
    assert jobs == 0
    assert (await _client_row(bound_session_local)).easyweek_visits_total == 4


async def test_reviews_on_and_counter_off_is_unchanged(bound_session_local, monkeypatch) -> None:
    """PR-9/PR-10 behaviour must be exactly what it was before PR-11."""
    _reviews_on(monkeypatch)
    monkeypatch.setattr(settings, "easyweek_google_review_links", "{}", raising=False)
    await _seed_booking(bound_session_local)

    await _deliver_succeeded(bound_session_local, visits_total=4)

    client = await _client_row(bound_session_local)
    assert client.easyweek_visits_total is None, "the counter is off; nothing may be written"
    # The succeeded event was still CLAIMED by the review consumer, which is
    # what "unchanged" means here.
    assert [e.status for e in await _event_rows(bound_session_local)][-1] in {"captured", "processed"}


async def test_both_consumers_on_commit_together(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    _reviews_on(monkeypatch)
    monkeypatch.setattr(settings, "easyweek_google_review_links", "{}", raising=False)
    await _seed_booking(bound_session_local)

    await _deliver_succeeded(bound_session_local, visits_total=8)

    client = await _client_row(bound_session_local)
    event = (await _event_rows(bound_session_local))[-1]
    if event.status == "processed":
        assert client.easyweek_visits_total == 8, "counter and terminal status commit together"
    else:
        # The review consumer deferred on configuration; then the counter must
        # not have been committed either.
        assert client.easyweek_visits_total is None


async def test_a_rolled_back_transaction_leaves_neither_the_counter_nor_the_status(
    bound_session_local, monkeypatch
) -> None:
    """The counter and `mark_processed` are one transaction, or they are nothing."""
    _counter_on(monkeypatch)
    await _seed_booking(bound_session_local)

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, _succeeded(visits_total=5), event_hint="booking-succeeded", payload_hash="s-1")

    real_mark = worker.mark_processed

    def _boom(event):
        real_mark(event)
        raise RuntimeError("crash between the counter update and the commit")

    # `process_one` treats an unexpected exception as transient: it rolls the
    # claim back and reschedules. That IS the behaviour under test — what must
    # not survive the rollback is the counter.
    monkeypatch.setattr(worker, "mark_processed", _boom)
    await worker.process_one()
    monkeypatch.setattr(worker, "mark_processed", real_mark)

    client = await _client_row(bound_session_local)
    assert client.easyweek_visits_total is None, "the counter never survives a rolled back claim"
    assert (await _event_rows(bound_session_local))[-1].status != "processed"

    # And once the scheduled retry is due, it lands the value exactly once.
    async with bound_session_local() as session:
        async with session.begin():
            row = (await session.execute(select(EasyWeekEvent).order_by(EasyWeekEvent.id.desc()))).scalars().first()
            row.next_retry_at = None
    await _run_until_idle()

    assert (await _client_row(bound_session_local)).easyweek_visits_total == 5
    assert (await _event_rows(bound_session_local))[-1].status == "processed"


async def test_lifecycle_events_after_a_deferred_succeeded_keep_flowing(bound_session_local, monkeypatch) -> None:
    """A cancel after a deferred succeeded must still reach the Record."""
    await _seed_booking(bound_session_local)

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, _succeeded(), event_hint="booking-succeeded", payload_hash="s-1")
            await _capture(session, booking_canceled(), event_hint="booking-canceled", payload_hash="cancel-1")

    await _run_until_idle()

    record = await _record_row(bound_session_local)
    assert bool(record.is_deleted) is True, "the cancellation was applied despite the deferred succeeded"


# ===========================================================================
# The flag, and who reads it
# ===========================================================================


def test_the_flag_defaults_to_false() -> None:
    from altegio_bot.settings import Settings

    assert Settings.model_fields["easyweek_visit_counter_enabled"].default is False


def test_the_counter_gate_ignores_the_notification_master_switch(monkeypatch) -> None:
    """Pausing outbound messaging must not stop domain bookkeeping.

    The snapshots missed while it was off cannot be recovered: EasyWeek does not
    re-deliver, and `current + 1` is not a reconstruction.
    """
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_visit_counter_enabled", True, raising=False)

    assert worker.visit_counter_enabled() is True
    assert worker.review_planning_enabled() is False
    assert worker.succeeded_consumer_enabled() is True


@pytest.mark.parametrize(
    ("notifications", "reviews", "counter", "expected"),
    [
        pytest.param(False, False, False, False, id="everything_off"),
        pytest.param(True, True, False, True, id="reviews_only"),
        pytest.param(False, False, True, True, id="counter_only"),
        pytest.param(True, True, True, True, id="both"),
        pytest.param(False, True, False, False, id="reviews_without_the_master_switch"),
    ],
)
def test_one_effective_gate_answers_for_every_succeeded_consumer(
    monkeypatch, notifications, reviews, counter, expected
) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", notifications, raising=False)
    monkeypatch.setattr(settings, "easyweek_reviews_enabled", reviews, raising=False)
    monkeypatch.setattr(settings, "easyweek_visit_counter_enabled", counter, raising=False)

    assert worker.succeeded_consumer_enabled() is expected


# ===========================================================================
# A notification pause must not destroy a review the visit earned
# ===========================================================================
#
# The counter and the review are two independent consumers of one
# `booking-succeeded`, behind two different fences. PR-11 made the counter
# claim the event even with notifications off — correctly, since the counter
# sends nothing — and that quietly created a way to lose a review: the event
# reached `processed` with the review never considered, and a `processed` row
# is never claimed again.
#
# The fix is a narrow durable marker rather than a held claim. Holding the row
# `captured` would have to roll back the counter with it, and would make the row
# a predecessor blocking its own booking. So the row terminalizes AND records
# that a review is still owed.

REVIEW_URL = "https://g.page/r/CfakeFAKEfake123/review"


def _review_links_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        settings,
        "easyweek_google_review_links",
        json.dumps({str(TEST_LOCATION_ID): REVIEW_URL}),
        raising=False,
    )


def _reviews_wanted(monkeypatch: pytest.MonkeyPatch) -> None:
    """The operator wants reviews. Whether the fence is open is a separate flag."""
    monkeypatch.setattr(settings, "easyweek_reviews_enabled", True, raising=False)
    _review_links_on(monkeypatch)


def _fence_open(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)


def _future_start(**delta) -> datetime:
    # Whole seconds: the webhook format cannot express microseconds, so a
    # `utcnow()` with them would not survive the round trip.
    return (datetime.now(timezone.utc) + timedelta(**delta)).replace(microsecond=0)


def _booking_starting(payload: dict[str, Any], start: datetime) -> dict[str, Any]:
    end = start + timedelta(hours=1)
    payload["booking_date_start"] = start.strftime("%Y-%m-%dT%H:%M:%S+0000")
    payload["booking_date_end"] = end.strftime("%Y-%m-%dT%H:%M:%S+0000")
    return payload


async def _seed_future_booking(session_maker) -> None:
    """A booking whose review moment (`starts_at + 3d`) is still ahead."""
    async with session_maker() as session:
        async with session.begin():
            await _capture(
                session,
                _booking_starting(booking_created(), _future_start(days=1)),
                event_hint="booking-created",
                payload_hash="created-1",
            )
    await _run_until_idle()


async def _review_jobs(session_maker) -> list[MessageJob]:
    async with session_maker() as session:
        return list(
            (
                await session.execute(
                    select(MessageJob).where(MessageJob.job_type == "review_3d").order_by(MessageJob.id)
                )
            )
            .scalars()
            .all()
        )


async def _deliver_future_succeeded(session_maker, *, visits_total=3, payload_hash="succeeded-1"):
    payload = _booking_starting(_succeeded(visits_total=visits_total), _future_start(days=1))
    async with session_maker() as session:
        async with session.begin():
            await _capture(session, payload, event_hint="booking-succeeded", payload_hash=payload_hash)
    return await _run_until_idle()


# --- A: counter on, reviews wanted, fence shut ------------------------------


async def test_a_closed_fence_keeps_the_counter_and_the_review_obligation(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    _reviews_wanted(monkeypatch)  # notifications deliberately still off
    await _seed_future_booking(bound_session_local)

    await _deliver_future_succeeded(bound_session_local, visits_total=3)

    client = await _client_row(bound_session_local)
    event = (await _event_rows(bound_session_local))[-1]

    assert client.easyweek_visits_total == 3, "the counter does not wait for the notification fence"
    assert await _review_jobs(bound_session_local) == [], "and no message is queued while it is shut"
    # Terminal for the queue, but NOT irreversibly done: the obligation is
    # recorded on the row itself.
    assert event.status == "processed"
    assert event.review_deferred_at is not None


async def test_the_deferred_obligation_is_recorded_only_when_reviews_are_wanted(
    bound_session_local, monkeypatch
) -> None:
    """Reviews OFF is a decision, not a pause — it owes nothing."""
    _counter_on(monkeypatch)
    await _seed_future_booking(bound_session_local)

    await _deliver_future_succeeded(bound_session_local, visits_total=3)

    event = (await _event_rows(bound_session_local))[-1]
    assert event.status == "processed"
    assert event.review_deferred_at is None


# --- B: the fence opens -----------------------------------------------------


async def test_opening_the_fence_recovers_exactly_one_review(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    _reviews_wanted(monkeypatch)
    await _seed_future_booking(bound_session_local)
    await _deliver_future_succeeded(bound_session_local, visits_total=3)

    before = await _client_row(bound_session_local)
    stamp = before.easyweek_visits_total_updated_at

    _fence_open(monkeypatch)
    reconsidered = await worker.recover_deferred_reviews()
    # A second pass must find nothing left to do.
    again = await worker.recover_deferred_reviews()

    jobs = await _review_jobs(bound_session_local)
    client = await _client_row(bound_session_local)
    event = (await _event_rows(bound_session_local))[-1]

    assert reconsidered == 1
    assert again == 0, "the obligation is discharged, not re-offered forever"
    assert len(jobs) == 1, "at most one review per booking"
    assert jobs[0].provider == "easyweek"
    assert jobs[0].company_id == TEST_LOCATION_ID
    # Recovery reconsiders the review and nothing else.
    assert client.easyweek_visits_total == 3
    assert client.easyweek_visits_total_updated_at == stamp
    assert event.status == "processed"
    assert event.review_deferred_at is None


async def test_a_resend_after_recovery_adds_no_second_review_and_no_second_count(
    bound_session_local, monkeypatch
) -> None:
    _counter_on(monkeypatch)
    _reviews_wanted(monkeypatch)
    await _seed_future_booking(bound_session_local)
    await _deliver_future_succeeded(bound_session_local, visits_total=3)
    _fence_open(monkeypatch)
    await worker.recover_deferred_reviews()
    stamp = (await _client_row(bound_session_local)).easyweek_visits_total_updated_at

    await _deliver_future_succeeded(bound_session_local, visits_total=3, payload_hash="succeeded-2")

    client = await _client_row(bound_session_local)
    assert len(await _review_jobs(bound_session_local)) == 1
    assert client.easyweek_visits_total == 3
    assert client.easyweek_visits_total_updated_at == stamp


async def test_recovery_does_nothing_while_the_fence_stays_shut(bound_session_local, monkeypatch) -> None:
    """No decision is possible, so no rows are read — that is what stops a busy loop."""
    _counter_on(monkeypatch)
    _reviews_wanted(monkeypatch)
    await _seed_future_booking(bound_session_local)
    await _deliver_future_succeeded(bound_session_local, visits_total=3)

    assert await worker.recover_deferred_reviews() == 0
    assert await worker.recover_deferred_reviews() == 0

    event = (await _event_rows(bound_session_local))[-1]
    assert event.review_deferred_at is not None, "still owed"
    assert await _review_jobs(bound_session_local) == []


# --- C: a waiting review must not hold up its own booking -------------------


async def test_a_deferred_review_does_not_block_later_lifecycle_events(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    _reviews_wanted(monkeypatch)
    await _seed_future_booking(bound_session_local)
    await _deliver_future_succeeded(bound_session_local, visits_total=3)

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(
                session,
                _booking_starting(booking_updated(), _future_start(days=2)),
                event_hint="booking-updated",
                payload_hash="updated-1",
            )
            await _capture(session, booking_canceled(), event_hint="booking-canceled", payload_hash="cancel-1")

    processed = await _run_until_idle()

    rows = {e.payload_hash: e for e in await _event_rows(bound_session_local)}
    assert processed >= 2, "later deliveries of the same booking keep flowing"
    assert rows["updated-1"].status == "processed"
    assert rows["cancel-1"].status == "processed"
    # The obligation is untouched by the lifecycle events that overtook it.
    assert rows["succeeded-1"].review_deferred_at is not None
    record = await _record_row(bound_session_local)
    assert bool(record.is_deleted) is True


async def test_a_deferred_review_is_never_reclaimed_as_an_event(bound_session_local, monkeypatch) -> None:
    """No tight retry loop: the row is terminal, so the claim never sees it again."""
    _counter_on(monkeypatch)
    _reviews_wanted(monkeypatch)
    await _seed_future_booking(bound_session_local)
    await _deliver_future_succeeded(bound_session_local, visits_total=3)

    assert await _run_until_idle() == 0, "nothing left to claim"

    event = (await _event_rows(bound_session_local))[-1]
    assert event.status == "processed"
    assert event.next_retry_at is None
    assert event.review_deferred_at is not None


# --- D: counter-only is unaffected ------------------------------------------


async def test_counter_only_still_completes_without_any_obligation(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    await _seed_future_booking(bound_session_local)

    await _deliver_future_succeeded(bound_session_local, visits_total=5)

    client = await _client_row(bound_session_local)
    event = (await _event_rows(bound_session_local))[-1]

    assert client.easyweek_visits_total == 5
    assert await _review_jobs(bound_session_local) == []
    assert event.status == "processed"
    assert event.review_deferred_at is None
    # And recovery has nothing to reconsider even once the fence opens.
    _fence_open(monkeypatch)
    assert await worker.recover_deferred_reviews() == 0


async def test_reviews_wanted_without_the_counter_is_unchanged_pr9_behaviour(bound_session_local, monkeypatch) -> None:
    """counter=false, reviews=true, notifications=false — exactly as before PR-11."""
    _reviews_wanted(monkeypatch)  # counter stays off
    await _seed_future_booking(bound_session_local)
    await _deliver_future_succeeded(bound_session_local, visits_total=3)

    event = (await _event_rows(bound_session_local))[-1]
    assert event.status == "captured", "no enabled consumer, so it is never claimed"
    assert event.review_deferred_at is None, "an unclaimed row owes nothing yet"
    assert (await _client_row(bound_session_local)).easyweek_visits_total is None


# ===========================================================================
# A deferred review belongs to the customer who had the visit
# ===========================================================================
#
# The marker survives an arbitrary amount of time, and `booking-updated` may
# reassign the Record to a different customer while it waits. `plan_review_job`
# resolves its client from `Record.client_id` — mutable by construction — so
# recovery had to re-prove the identity the ORIGINAL delivery named, or it would
# earn one person's review for another. That is a cross-client send, the exact
# thing the planner exists to prevent.
#
# The proof comes from the captured payload, which is never rewritten.

OTHER_CUSTOMER_ID = TEST_CUSTOMER_ID + 1


def _reassignment_to_other_customer() -> dict[str, Any]:
    payload = _booking_starting(booking_updated(), _future_start(days=1))
    payload["customer_id"] = OTHER_CUSTOMER_ID
    return payload


async def test_a_deferred_review_is_never_earned_for_a_reassigned_client(bound_session_local, monkeypatch) -> None:
    """Client A had the visit; the booking later moves to Client B; nobody is messaged."""
    _counter_on(monkeypatch)
    _reviews_wanted(monkeypatch)  # fence still shut
    await _seed_future_booking(bound_session_local)
    await _deliver_future_succeeded(bound_session_local, visits_total=3)

    client_a = await _client_row(bound_session_local)
    assert client_a.altegio_client_id == TEST_CUSTOMER_ID
    assert (await _event_rows(bound_session_local))[-1].review_deferred_at is not None

    # The booking is reassigned to a different customer.
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(
                session,
                _reassignment_to_other_customer(),
                event_hint="booking-updated",
                payload_hash="updated-reassign",
            )
    await _run_until_idle()

    record = await _record_row(bound_session_local)
    async with bound_session_local() as session:
        client_b = (
            (await session.execute(select(Client).where(Client.altegio_client_id == OTHER_CUSTOMER_ID))).scalars().one()
        )
    assert record.client_id == client_b.id, "the Record really did move to Client B"
    assert client_b.id != client_a.id

    _fence_open(monkeypatch)
    decided = await worker.recover_deferred_reviews()

    jobs = await _review_jobs(bound_session_local)
    succeeded_row = next(e for e in await _event_rows(bound_session_local) if e.payload_hash == "succeeded-1")

    assert jobs == [], "no review may be earned for a customer who did not have the visit"
    assert decided == 1, "the obligation was decided — fail closed, not left dangling"
    # Discharged: retrying forever cannot make the reassignment go away.
    assert succeeded_row.review_deferred_at is None


async def test_a_deferred_review_still_lands_when_the_client_is_unchanged(bound_session_local, monkeypatch) -> None:
    """The control: an ordinary `booking-updated` must not block the review."""
    _counter_on(monkeypatch)
    _reviews_wanted(monkeypatch)
    await _seed_future_booking(bound_session_local)
    await _deliver_future_succeeded(bound_session_local, visits_total=3)

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(
                session,
                _booking_starting(booking_updated(), _future_start(days=2)),
                event_hint="booking-updated",
                payload_hash="updated-same-client",
            )
    await _run_until_idle()

    _fence_open(monkeypatch)
    await worker.recover_deferred_reviews()

    jobs = await _review_jobs(bound_session_local)
    client = await _client_row(bound_session_local)
    assert len(jobs) == 1
    assert jobs[0].client_id == client.id


async def test_a_succeeded_payload_without_a_provable_customer_earns_nothing(bound_session_local, monkeypatch) -> None:
    """No customer id, no proof, no review — and the obligation is discharged."""
    _counter_on(monkeypatch)
    _reviews_wanted(monkeypatch)
    await _seed_future_booking(bound_session_local)

    payload = _booking_starting(_succeeded(visits_total=3), _future_start(days=1))
    payload.pop("customer_id")
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, payload, event_hint="booking-succeeded", payload_hash="succeeded-nocust")
    await _run_until_idle()

    _fence_open(monkeypatch)
    decided = await worker.recover_deferred_reviews()

    assert decided == 1
    assert await _review_jobs(bound_session_local) == []
    row = next(e for e in await _event_rows(bound_session_local) if e.payload_hash == "succeeded-nocust")
    assert row.review_deferred_at is None


# ===========================================================================
# A bounded batch must be a queue, not a fixed prefix
# ===========================================================================
#
# Recovery takes the first REVIEW_RECOVERY_BATCH rows by id. Every outcome that
# left the marker in place therefore re-took the same slot on the next pass, and
# fifty undecidable rows were enough to make row fifty-one unreachable forever.
#
# The fix is that every outcome either clears the marker or moves it forward, and
# the selection honours that moment.


async def _seed_deferred_rows(session_maker, *, count: int, malformed: bool, first_index: int = 0) -> list[int]:
    """Directly seeded `processed` succeeded rows that already owe a review."""
    ids: list[int] = []
    async with session_maker() as session:
        async with session.begin():
            for offset in range(count):
                index = first_index + offset
                payload = _booking_starting(_succeeded(visits_total=2), _future_start(days=1))
                if malformed:
                    payload["uid"] = f"not-a-uuid-{index}"
                else:
                    payload["uid"] = TEST_BOOKING_UUID
                event = EasyWeekEvent(
                    status="processed",
                    event_hint="booking-succeeded",
                    auth_via="query",
                    payload_hash=f"deferred-{index}",
                    payload=payload,
                    body_truncated=False,
                    booking_uuid=canonical_booking_uuid(payload),
                    review_deferred_at=datetime.now(timezone.utc) - timedelta(minutes=5),
                )
                session.add(event)
                await session.flush()
                ids.append(int(event.id))
    return ids


async def test_a_permanently_unparseable_row_is_discharged_not_retried(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    _reviews_wanted(monkeypatch)
    _fence_open(monkeypatch)
    await _seed_future_booking(bound_session_local)
    (poison_id,) = await _seed_deferred_rows(bound_session_local, count=1, malformed=True)

    decided = await worker.recover_deferred_reviews()

    async with bound_session_local() as session:
        row = await session.get(EasyWeekEvent, poison_id)
    assert decided == 1
    assert row.review_deferred_at is None, "the payload will parse the same way forever"
    assert await _review_jobs(bound_session_local) == []


async def test_fifty_poison_rows_do_not_starve_the_valid_row_behind_them(bound_session_local, monkeypatch) -> None:
    """The reported defect, end to end."""
    _counter_on(monkeypatch)
    _reviews_wanted(monkeypatch)
    _fence_open(monkeypatch)
    await _seed_future_booking(bound_session_local)

    poison_ids = await _seed_deferred_rows(bound_session_local, count=worker.REVIEW_RECOVERY_BATCH, malformed=True)
    (good_id,) = await _seed_deferred_rows(
        bound_session_local, count=1, malformed=False, first_index=worker.REVIEW_RECOVERY_BATCH
    )
    assert len(poison_ids) == 50
    assert good_id > max(poison_ids), "the valid row really is behind the whole batch"

    await worker.recover_deferred_reviews()
    await worker.recover_deferred_reviews()

    async with bound_session_local() as session:
        good = await session.get(EasyWeekEvent, good_id)
        still_marked = (
            await session.execute(
                select(func.count()).select_from(EasyWeekEvent).where(EasyWeekEvent.review_deferred_at.is_not(None))
            )
        ).scalar_one()

    assert good.review_deferred_at is None, "row 51 was reached and decided"
    assert still_marked == 0, "and no obligation is left circling"
    assert len(await _review_jobs(bound_session_local)) == 1, "exactly one review, from the one valid row"


async def test_an_undecidable_configuration_keeps_the_obligation_but_yields_its_slot(
    bound_session_local, monkeypatch
) -> None:
    _counter_on(monkeypatch)
    _reviews_wanted(monkeypatch)
    await _seed_future_booking(bound_session_local)
    # The obligation is created while the fence is shut and the map is still
    # good; only then does the map break. A broken map at delivery time would
    # keep the event `captured` instead, which is a different path entirely.
    await _deliver_future_succeeded(bound_session_local, visits_total=3)

    monkeypatch.setattr(settings, "easyweek_google_review_links", "{not json", raising=False)
    _fence_open(monkeypatch)

    first = await worker.recover_deferred_reviews()
    row_after_first = next(e for e in await _event_rows(bound_session_local) if e.payload_hash == "succeeded-1")
    second = await worker.recover_deferred_reviews()

    assert first == 0 and second == 0, "nothing was decided"
    assert row_after_first.review_deferred_at is not None, "the obligation stands"
    assert row_after_first.review_deferred_at > datetime.now(timezone.utc), "and it stepped aside"
    assert await _review_jobs(bound_session_local) == []


async def test_the_obligation_is_honoured_once_the_configuration_is_fixed(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    _reviews_wanted(monkeypatch)
    await _seed_future_booking(bound_session_local)
    await _deliver_future_succeeded(bound_session_local, visits_total=3)

    monkeypatch.setattr(settings, "easyweek_google_review_links", "{not json", raising=False)
    _fence_open(monkeypatch)
    await worker.recover_deferred_reviews()

    # The operator fixes the map, and the retry moment arrives.
    _review_links_on(monkeypatch)
    async with bound_session_local() as session:
        async with session.begin():
            row = (
                (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.payload_hash == "succeeded-1")))
                .scalars()
                .one()
            )
            row.review_deferred_at = datetime.now(timezone.utc) - timedelta(seconds=1)

    decided = await worker.recover_deferred_reviews()
    # A further pass must not earn a second review.
    again = await worker.recover_deferred_reviews()

    assert decided == 1
    assert again == 0
    assert len(await _review_jobs(bound_session_local)) == 1
    row = next(e for e in await _event_rows(bound_session_local) if e.payload_hash == "succeeded-1")
    assert row.review_deferred_at is None


async def test_recovery_logs_carry_no_payload_or_pii(bound_session_local, monkeypatch, caplog) -> None:
    _counter_on(monkeypatch)
    _reviews_wanted(monkeypatch)
    _fence_open(monkeypatch)
    await _seed_future_booking(bound_session_local)
    await _seed_deferred_rows(bound_session_local, count=1, malformed=True)
    await _deliver_future_succeeded(bound_session_local, visits_total=3)

    with caplog.at_level(logging.DEBUG):
        await worker.recover_deferred_reviews()

    blob = "\n".join(record.getMessage() for record in caplog.records)
    fixture = booking_created()
    for key in (
        "customer_phone",
        "customer_email",
        "customer_full_name",
        "customer_name",
        "customer_attributes.customer_phone",
        "customer_attributes.customer_email",
    ):
        value = str(fixture.get(key, "")).strip()
        assert value, f"the fixture must actually carry {key} for this test to mean anything"
        assert value not in blob, key
    # The malformed value itself is payload content and must not be echoed —
    # only the stable reason code may name what was wrong with it.
    assert "not-a-uuid" not in blob
    assert "invalid_booking_uuid" in blob, "the stable code IS reported"


async def test_a_deferred_review_without_a_stored_count_is_still_planned(bound_session_local, monkeypatch) -> None:
    """Recovery re-proves the visit from the row's own payload.

    Plan §31.11 answers the limit from the stored snapshot, and a review can
    wait days for its moment. A row that reaches recovery without one — deferred
    before the counter existed, or while it was switched off — is a delivery
    that genuinely earned a review, so refusing it as "unproven" would destroy
    it silently. Recovery therefore keeps the live path's order: place the
    snapshot this delivery carries, then decide.
    """
    _counter_on(monkeypatch)
    _reviews_wanted(monkeypatch)
    _fence_open(monkeypatch)
    await _seed_future_booking(bound_session_local)
    (deferred_id,) = await _seed_deferred_rows(bound_session_local, count=1, malformed=False)

    client = await _client_row(bound_session_local)
    assert client.easyweek_visits_total is None, "the fixture never stored one"

    decided = await worker.recover_deferred_reviews()

    assert decided == 1
    client = await _client_row(bound_session_local)
    assert client.easyweek_visits_total == 2, "placed from the deferred row's own payload"
    assert client.easyweek_visits_total_updated_at is not None
    assert len(await _review_jobs(bound_session_local)) == 1

    async with bound_session_local() as session:
        row = await session.get(EasyWeekEvent, deferred_id)
    assert row.review_deferred_at is None


async def test_recovery_still_refuses_a_deferred_review_over_the_limit(bound_session_local, monkeypatch) -> None:
    """Re-proving is not a way around the limit."""
    _counter_on(monkeypatch)
    _reviews_wanted(monkeypatch)
    _fence_open(monkeypatch)
    await _seed_future_booking(bound_session_local)
    await _seed_deferred_rows(bound_session_local, count=1, malformed=False)

    async with bound_session_local() as session:
        async with session.begin():
            client = (await session.execute(select(Client).where(Client.provider == "easyweek"))).scalars().one()
            client.easyweek_visits_total = 7
            client.easyweek_visits_total_updated_at = datetime.now(timezone.utc)

    await worker.recover_deferred_reviews()

    assert await _review_jobs(bound_session_local) == []
    assert (await _client_row(bound_session_local)).easyweek_visits_total == 7, "monotonic, never lowered"
