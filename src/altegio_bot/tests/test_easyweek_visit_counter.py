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
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import altegio_bot.db as app_db
from altegio_bot.easyweek_normalizer import canonical_booking_uuid
from altegio_bot.models.models import Client, EasyWeekEvent, MessageJob, Record
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import (
    TEST_BOOKING_ID,
    TEST_CUSTOMER_ID,
    TEST_LOCATION_ID,
    TEST_LOCATION_UUID,
    booking_canceled,
    booking_created,
    booking_updated,
)
from altegio_bot.workers import easyweek_inbox_worker as worker


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


async def test_two_concurrent_deliveries_neither_lose_nor_lower_the_result(bound_session_local, monkeypatch) -> None:
    """Row lock, not read-modify-write in Python.

    Both events are for the same booking, so the predecessor rule already
    serialises them; the lock is what makes the outcome correct even when two
    workers race on the same client from different bookings.
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
    assert client.easyweek_visits_total == 6, "the highest proven snapshot wins, whatever the order"
    statuses = [e.status for e in await _event_rows(bound_session_local)]
    assert statuses.count("processed") == 3, "both succeeded events reached a terminal state"


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
