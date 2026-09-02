"""PostgreSQL contract for PR-12 planning: which retention messages are earned.

Two messages, two sources of evidence, and one shared rule about what counts as
proof:

* ``repeat_10d`` is earned ONLY by a proven ``booking-succeeded`` whose
  ``visits_total`` was accepted onto the client row. Not by a create, not by an
  update, and never by the observation that a start time has passed.
* ``comeback_3d`` is earned ONLY by a proven cancellation delivery, and freezes
  the moment that delivery was captured.

Both freeze the counter value proven at that moment. A message that could not
prove one is never created — the baseline is the only thing that can later
answer "has this customer already come back?", and a job without it would either
have to guess or go out blind.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import altegio_bot.db as app_db
from altegio_bot.easyweek_normalizer import canonical_booking_uuid
from altegio_bot.easyweek_retention import (
    COMEBACK_DELAY,
    PAYLOAD_PROOF_VERSION,
    PAYLOAD_RECORD_STARTS_AT,
    PAYLOAD_SOURCE_CANCELLED_AT,
    PAYLOAD_VISITS_BASELINE,
    REPEAT_DELAY,
    RETENTION_PROOF_VERSION,
)
from altegio_bot.models.models import Client, EasyWeekEvent, MessageJob, Record
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import (
    FOREIGN_LOCATION_ID,
    TEST_BOOKING_ID,
    TEST_CUSTOMER_ID,
    TEST_LOCATION_ID,
    TEST_LOCATION_UUID,
    booking_canceled,
    booking_created,
    booking_created_multi_service,
)
from altegio_bot.workers import easyweek_inbox_worker as worker

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _base_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Processing on, every optional consumer off — the production default."""
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_reviews_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_visit_counter_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_retention_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_retention_send_enabled", False, raising=False)
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


# ---------------------------------------------------------------------------
# Flag helpers — named so a test reads as the rollout state it describes
# ---------------------------------------------------------------------------


def _counter_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_visit_counter_enabled", True, raising=False)


def _retention_on(monkeypatch: pytest.MonkeyPatch) -> None:
    """Planning fully enabled: the master fence and the PR-12 flag."""
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_retention_enabled", True, raising=False)


def _retention_wanted(monkeypatch: pytest.MonkeyPatch) -> None:
    """The PR-12 flag on, the master notification fence deliberately shut."""
    monkeypatch.setattr(settings, "easyweek_retention_enabled", True, raising=False)


def _fence_open(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)


# ---------------------------------------------------------------------------
# Payload / capture helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    # Whole seconds: the webhook format cannot express microseconds, so a
    # `utcnow()` with them would not survive the round trip.
    return datetime.now(timezone.utc).replace(microsecond=0)


def _at(**delta: Any) -> datetime:
    return _now() + timedelta(**delta)


def _starting(payload: dict[str, Any], start: datetime) -> dict[str, Any]:
    end = start + timedelta(hours=1)
    payload["booking_date_start"] = start.strftime("%Y-%m-%dT%H:%M:%S+0000")
    payload["booking_date_end"] = end.strftime("%Y-%m-%dT%H:%M:%S+0000")
    return payload


def _succeeded(payload: dict[str, Any] | None = None, *, visits_total: Any = 3, **overrides: Any) -> dict[str, Any]:
    body = payload if payload is not None else booking_created()
    body["booking_status"] = "Succeeded appointment"
    body["visits_total"] = visits_total
    body.update(overrides)
    return body


async def _capture(
    session: AsyncSession,
    payload: dict[str, Any],
    *,
    event_hint: str,
    payload_hash: str,
) -> int:
    event = EasyWeekEvent(
        status="captured",
        event_hint=event_hint,
        auth_via="query",
        payload_hash=payload_hash,
        payload=payload,
        body_truncated=False,
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


async def _deliver(session_maker, payload: dict[str, Any], *, event_hint: str, payload_hash: str) -> int:
    async with session_maker() as session:
        async with session.begin():
            await _capture(session, payload, event_hint=event_hint, payload_hash=payload_hash)
    return await _run_until_idle()


async def _seed_booking(session_maker, *, start: datetime | None = None) -> datetime:
    """One proven Record + Client, created the way production creates them."""
    moment = start or _at(days=-1)
    await _deliver(
        session_maker,
        _starting(booking_created(), moment),
        event_hint="booking-created",
        payload_hash="created-1",
    )
    return moment


async def _jobs(session_maker, job_type: str) -> list[MessageJob]:
    async with session_maker() as session:
        return list(
            (await session.execute(select(MessageJob).where(MessageJob.job_type == job_type).order_by(MessageJob.id)))
            .scalars()
            .all()
        )


async def _client_row(session_maker) -> Client:
    async with session_maker() as session:
        return (await session.execute(select(Client).where(Client.provider == "easyweek"))).scalars().one()


async def _record_row(session_maker) -> Record:
    async with session_maker() as session:
        return (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()


async def _event_rows(session_maker) -> list[EasyWeekEvent]:
    async with session_maker() as session:
        return list((await session.execute(select(EasyWeekEvent).order_by(EasyWeekEvent.id))).scalars().all())


# ===========================================================================
# repeat_10d — earned only by a proven booking-succeeded
# ===========================================================================


async def test_a_proven_succeeded_delivery_earns_one_repeat(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    _retention_on(monkeypatch)
    start = await _seed_booking(bound_session_local)

    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start), visits_total=3),
        event_hint="booking-succeeded",
        payload_hash="succeeded-1",
    )

    jobs = await _jobs(bound_session_local, "repeat_10d")
    assert len(jobs) == 1
    job = jobs[0]
    record = await _record_row(bound_session_local)
    assert job.provider == "easyweek"
    assert job.company_id == TEST_LOCATION_ID
    assert job.record_id == record.id
    assert job.status == "queued"
    assert job.attempts == 0
    assert job.run_at == record.starts_at + REPEAT_DELAY
    assert job.payload[PAYLOAD_VISITS_BASELINE] == 3
    assert job.payload[PAYLOAD_PROOF_VERSION] == RETENTION_PROOF_VERSION
    assert job.payload[PAYLOAD_RECORD_STARTS_AT] == record.starts_at.astimezone(timezone.utc).isoformat()


async def test_the_repeat_payload_carries_no_personal_data(bound_session_local, monkeypatch) -> None:
    """A technical payload is not a place to keep a person's details."""
    _counter_on(monkeypatch)
    _retention_on(monkeypatch)
    start = await _seed_booking(bound_session_local)

    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start)),
        event_hint="booking-succeeded",
        payload_hash="succeeded-1",
    )

    payload = json.dumps((await _jobs(bound_session_local, "repeat_10d"))[0].payload)
    for secret in ("Test Person", "+49000000000", "test.person@example.invalid", "Fixture Service"):
        assert secret not in payload


async def test_an_exact_resend_earns_no_second_repeat(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    _retention_on(monkeypatch)
    start = await _seed_booking(bound_session_local)
    payload = _succeeded(_starting(booking_created(), start))

    await _deliver(bound_session_local, dict(payload), event_hint="booking-succeeded", payload_hash="succeeded-1")
    await _deliver(bound_session_local, dict(payload), event_hint="booking-succeeded", payload_hash="succeeded-1")

    assert len(await _jobs(bound_session_local, "repeat_10d")) == 1


async def test_a_different_payload_hash_for_the_same_fact_earns_no_second_repeat(
    bound_session_local, monkeypatch
) -> None:
    """A different DELIVERY is not a different visit."""
    _counter_on(monkeypatch)
    _retention_on(monkeypatch)
    start = await _seed_booking(bound_session_local)

    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start)),
        event_hint="booking-succeeded",
        payload_hash="succeeded-1",
    )
    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start), booking_status="Succeeded appointment "),
        event_hint="booking-succeeded",
        payload_hash="succeeded-2",
    )

    assert len(await _jobs(bound_session_local, "repeat_10d")) == 1


async def test_concurrent_processing_earns_one_repeat(bound_session_local, monkeypatch) -> None:
    """Two workers, two deliveries of one booking, one message."""
    _counter_on(monkeypatch)
    _retention_on(monkeypatch)
    start = await _seed_booking(bound_session_local)

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(
                session,
                _succeeded(_starting(booking_created(), start), visits_total=3),
                event_hint="booking-succeeded",
                payload_hash="succeeded-a",
            )
            await _capture(
                session,
                _succeeded(_starting(booking_created(), start), visits_total=3),
                event_hint="booking-succeeded",
                payload_hash="succeeded-b",
            )

    await asyncio.wait_for(asyncio.gather(_run_until_idle(), _run_until_idle()), timeout=30)

    assert len(await _jobs(bound_session_local, "repeat_10d")) == 1


async def test_a_late_succeeded_delivery_earns_no_historic_repeat(bound_session_local, monkeypatch) -> None:
    """Ten days gone is not a late reminder; it is a backfill nobody asked for."""
    _counter_on(monkeypatch)
    _retention_on(monkeypatch)
    start = await _seed_booking(bound_session_local, start=_at(days=-11))

    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start)),
        event_hint="booking-succeeded",
        payload_hash="succeeded-1",
    )

    assert await _jobs(bound_session_local, "repeat_10d") == []
    # The delivery itself is still a perfectly valid succeeded event.
    assert (await _event_rows(bound_session_local))[-1].status == "processed"


async def test_without_the_counter_no_repeat_is_created(bound_session_local, monkeypatch) -> None:
    """Fail closed: with no proven baseline there is nothing to measure against."""
    _retention_on(monkeypatch)  # counter deliberately off
    start = await _seed_booking(bound_session_local)

    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start)),
        event_hint="booking-succeeded",
        payload_hash="succeeded-1",
    )

    assert await _jobs(bound_session_local, "repeat_10d") == []
    assert (await _client_row(bound_session_local)).easyweek_visits_total is None


async def test_a_stale_snapshot_earns_no_repeat(bound_session_local, monkeypatch) -> None:
    """The baseline must be the total THIS delivery proved, not an older one.

    The counter is monotonic, so a lower snapshot arriving late leaves the stored
    value alone — and a repeat planned against it would be measuring the wrong
    appointment.
    """
    _counter_on(monkeypatch)
    _retention_on(monkeypatch)
    start = await _seed_booking(bound_session_local)

    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start), visits_total=6),
        event_hint="booking-succeeded",
        payload_hash="succeeded-high",
    )
    first = await _jobs(bound_session_local, "repeat_10d")
    assert len(first) == 1 and first[0].payload[PAYLOAD_VISITS_BASELINE] == 6

    # A delayed delivery stating an older total.
    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start), visits_total=4),
        event_hint="booking-succeeded",
        payload_hash="succeeded-low",
    )

    jobs = await _jobs(bound_session_local, "repeat_10d")
    assert len(jobs) == 1
    assert jobs[0].payload[PAYLOAD_VISITS_BASELINE] == 6, "the stale delivery must not rewrite a proven baseline"
    assert (await _client_row(bound_session_local)).easyweek_visits_total == 6


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"location_id": FOREIGN_LOCATION_ID}, id="foreign_company"),
        pytest.param({"customer_id": TEST_CUSTOMER_ID + 1}, id="other_customer"),
        pytest.param({"id": TEST_BOOKING_ID + 1}, id="other_booking_id"),
        pytest.param({"uid": "99999999-8888-4777-8666-555555555555"}, id="other_booking_uuid"),
    ],
)
async def test_an_unproven_identity_earns_no_repeat(bound_session_local, monkeypatch, overrides) -> None:
    _counter_on(monkeypatch)
    _retention_on(monkeypatch)
    start = await _seed_booking(bound_session_local)

    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start), **overrides),
        event_hint="booking-succeeded",
        payload_hash="succeeded-1",
    )

    assert await _jobs(bound_session_local, "repeat_10d") == []


async def test_a_disallowed_category_earns_no_repeat(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    _retention_on(monkeypatch)
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", json.dumps(["Something Else"]), raising=False)
    start = await _seed_booking(bound_session_local)

    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start)),
        event_hint="booking-succeeded",
        payload_hash="succeeded-1",
    )

    assert await _jobs(bound_session_local, "repeat_10d") == []


async def test_an_ambiguous_multi_service_booking_earns_no_repeat(bound_session_local, monkeypatch) -> None:
    """`repeat_10d` names ONE service. Two services means none can be named."""
    _counter_on(monkeypatch)
    _retention_on(monkeypatch)
    start = _at(days=-1)
    await _deliver(
        bound_session_local,
        _starting(booking_created_multi_service(), start),
        event_hint="booking-created",
        payload_hash="created-1",
    )

    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created_multi_service(), start)),
        event_hint="booking-succeeded",
        payload_hash="succeeded-1",
    )

    assert await _jobs(bound_session_local, "repeat_10d") == []


async def test_an_opted_out_client_earns_no_repeat(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    _retention_on(monkeypatch)
    start = await _seed_booking(bound_session_local)
    async with bound_session_local() as session:
        async with session.begin():
            client = (await session.execute(select(Client).where(Client.provider == "easyweek"))).scalars().one()
            client.wa_opted_out = True

    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start)),
        event_hint="booking-succeeded",
        payload_hash="succeeded-1",
    )

    assert await _jobs(bound_session_local, "repeat_10d") == []


# ===========================================================================
# The flags
# ===========================================================================


async def test_planning_off_creates_nothing_and_leaves_the_counter_working(bound_session_local, monkeypatch) -> None:
    """The post-deploy state: both flags false, everything else unchanged."""
    _counter_on(monkeypatch)
    _fence_open(monkeypatch)  # notifications on, retention deliberately off
    start = await _seed_booking(bound_session_local)

    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start), visits_total=5),
        event_hint="booking-succeeded",
        payload_hash="succeeded-1",
    )

    assert await _jobs(bound_session_local, "repeat_10d") == []
    assert (await _client_row(bound_session_local)).easyweek_visits_total == 5
    event = (await _event_rows(bound_session_local))[-1]
    assert event.status == "processed"
    assert event.review_deferred_at is None, "retention OFF is a decision, not a pause"


async def test_a_closed_master_fence_keeps_the_obligation(bound_session_local, monkeypatch) -> None:
    """Retention wanted, notifications shut: the message is owed, not lost."""
    _counter_on(monkeypatch)
    _retention_wanted(monkeypatch)
    start = await _seed_booking(bound_session_local)

    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start), visits_total=3),
        event_hint="booking-succeeded",
        payload_hash="succeeded-1",
    )

    assert await _jobs(bound_session_local, "repeat_10d") == []
    assert (await _client_row(bound_session_local)).easyweek_visits_total == 3
    event = (await _event_rows(bound_session_local))[-1]
    assert event.status == "processed", "the queue must not be held"
    assert event.review_deferred_at is not None, "and the obligation must survive"


async def test_opening_the_master_fence_recovers_the_deferred_repeat(bound_session_local, monkeypatch) -> None:
    _counter_on(monkeypatch)
    _retention_wanted(monkeypatch)
    start = await _seed_booking(bound_session_local)
    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start), visits_total=3),
        event_hint="booking-succeeded",
        payload_hash="succeeded-1",
    )
    assert await _jobs(bound_session_local, "repeat_10d") == []

    _fence_open(monkeypatch)
    assert await worker.recover_deferred_reviews() == 1

    jobs = await _jobs(bound_session_local, "repeat_10d")
    assert len(jobs) == 1
    assert jobs[0].payload[PAYLOAD_VISITS_BASELINE] == 3
    assert (await _event_rows(bound_session_local))[-1].review_deferred_at is None


async def test_the_recovery_is_idempotent(bound_session_local, monkeypatch) -> None:
    """A second pass must not earn a second message."""
    _counter_on(monkeypatch)
    _retention_wanted(monkeypatch)
    start = await _seed_booking(bound_session_local)
    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start)),
        event_hint="booking-succeeded",
        payload_hash="succeeded-1",
    )

    _fence_open(monkeypatch)
    await worker.recover_deferred_reviews()
    await worker.recover_deferred_reviews()

    assert len(await _jobs(bound_session_local, "repeat_10d")) == 1


async def test_the_succeeded_event_is_not_claimed_when_every_consumer_is_off(bound_session_local, monkeypatch) -> None:
    """With nothing switched on the evidence waits rather than being destroyed."""
    start = await _seed_booking(bound_session_local)

    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start)),
        event_hint="booking-succeeded",
        payload_hash="succeeded-1",
    )

    assert (await _event_rows(bound_session_local))[-1].status == "captured"


async def test_retention_alone_is_enough_to_claim_a_succeeded_event(bound_session_local, monkeypatch) -> None:
    """The claim gate and the planner ask the SAME question.

    If the claim admitted retention but the planner asked something stricter, the
    event would be terminalized having produced nothing.
    """
    _retention_on(monkeypatch)  # counter off: the repeat will fail closed
    start = await _seed_booking(bound_session_local)

    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start)),
        event_hint="booking-succeeded",
        payload_hash="succeeded-1",
    )

    assert (await _event_rows(bound_session_local))[-1].status == "processed"
    assert await _jobs(bound_session_local, "repeat_10d") == []


async def test_review_and_repeat_coexist_on_one_delivery(bound_session_local, monkeypatch) -> None:
    """Three consumers of one event, each earning exactly its own outcome."""
    _counter_on(monkeypatch)
    _retention_on(monkeypatch)
    monkeypatch.setattr(settings, "easyweek_reviews_enabled", True, raising=False)
    monkeypatch.setattr(
        settings,
        "easyweek_google_review_links",
        json.dumps({str(TEST_LOCATION_ID): "https://g.page/r/CaV0vSmrSYkdEAE/review"}),
        raising=False,
    )
    start = await _seed_booking(bound_session_local, start=_at(days=-1))

    await _deliver(
        bound_session_local,
        _succeeded(_starting(booking_created(), start), visits_total=2),
        event_hint="booking-succeeded",
        payload_hash="succeeded-1",
    )

    assert (await _client_row(bound_session_local)).easyweek_visits_total == 2
    assert len(await _jobs(bound_session_local, "repeat_10d")) == 1
    # review_3d fires at starts_at + 3d, which for a booking one day ago is past.
    # What matters here is that the repeat is unaffected by the review's outcome.
    assert (await _event_rows(bound_session_local))[-1].status == "processed"


# ===========================================================================
# comeback_3d — earned only by a proven cancellation
# ===========================================================================


async def _seed_client_counter(session_maker, *, visits_total: int = 4) -> None:
    """Give the EasyWeek client a proven counter the way PR-11 would."""
    async with session_maker() as session:
        async with session.begin():
            client = (await session.execute(select(Client).where(Client.provider == "easyweek"))).scalars().one()
            client.easyweek_visits_total = visits_total
            client.easyweek_visits_total_updated_at = datetime.now(timezone.utc)


async def test_a_proven_cancellation_earns_one_comeback(bound_session_local, monkeypatch) -> None:
    _retention_on(monkeypatch)
    await _seed_booking(bound_session_local, start=_at(days=2))
    await _seed_client_counter(bound_session_local, visits_total=4)

    await _deliver(
        bound_session_local,
        _starting(booking_canceled(), _at(days=2)),
        event_hint="booking-canceled",
        payload_hash="canceled-1",
    )

    jobs = await _jobs(bound_session_local, "comeback_3d")
    assert len(jobs) == 1
    job = jobs[0]
    assert job.provider == "easyweek"
    assert job.company_id == TEST_LOCATION_ID
    assert job.status == "queued"
    assert job.attempts == 0
    assert job.payload[PAYLOAD_VISITS_BASELINE] == 4
    assert job.payload[PAYLOAD_PROOF_VERSION] == RETENTION_PROOF_VERSION

    cancelled_at = datetime.fromisoformat(job.payload[PAYLOAD_SOURCE_CANCELLED_AT])
    assert job.run_at == cancelled_at + COMEBACK_DELAY
    event = (await _event_rows(bound_session_local))[-1]
    assert cancelled_at == event.received_at.astimezone(timezone.utc), "the cancellation moment is the captured one"


async def test_repeated_cancel_deliveries_earn_one_comeback(bound_session_local, monkeypatch) -> None:
    _retention_on(monkeypatch)
    await _seed_booking(bound_session_local, start=_at(days=2))
    await _seed_client_counter(bound_session_local)

    for index in range(3):
        await _deliver(
            bound_session_local,
            _starting(booking_canceled(), _at(days=2)),
            event_hint="booking-canceled",
            payload_hash=f"canceled-{index}",
        )

    assert len(await _jobs(bound_session_local, "comeback_3d")) == 1


async def test_a_later_update_delivery_earns_no_comeback(bound_session_local, monkeypatch) -> None:
    """Only the delivery that PROVED the cancellation earns the message."""
    _retention_on(monkeypatch)
    await _seed_booking(bound_session_local, start=_at(days=2))
    await _seed_client_counter(bound_session_local)

    await _deliver(
        bound_session_local,
        _starting(booking_created(), _at(days=2)),
        event_hint="booking-updated",
        payload_hash="updated-1",
    )

    assert await _jobs(bound_session_local, "comeback_3d") == []


async def test_a_cancellation_without_a_proven_baseline_earns_nothing(bound_session_local, monkeypatch) -> None:
    """Fail closed, and never substitute a zero.

    A missing count does not mean "new customer"; it means the one question this
    message depends on cannot be answered.
    """
    _retention_on(monkeypatch)
    await _seed_booking(bound_session_local, start=_at(days=2))

    await _deliver(
        bound_session_local,
        _starting(booking_canceled(), _at(days=2)),
        event_hint="booking-canceled",
        payload_hash="canceled-1",
    )

    assert await _jobs(bound_session_local, "comeback_3d") == []
    record = await _record_row(bound_session_local)
    assert record.is_deleted is True, "the cancellation itself still applies"


async def test_retention_off_earns_no_comeback(bound_session_local, monkeypatch) -> None:
    _fence_open(monkeypatch)  # notifications on, retention off
    await _seed_booking(bound_session_local, start=_at(days=2))
    await _seed_client_counter(bound_session_local)

    await _deliver(
        bound_session_local,
        _starting(booking_canceled(), _at(days=2)),
        event_hint="booking-canceled",
        payload_hash="canceled-1",
    )

    assert await _jobs(bound_session_local, "comeback_3d") == []


async def test_a_closed_master_fence_earns_no_comeback(bound_session_local, monkeypatch) -> None:
    """The established lifecycle contract: no customer messages means none."""
    _retention_wanted(monkeypatch)
    await _seed_booking(bound_session_local, start=_at(days=2))
    await _seed_client_counter(bound_session_local)

    await _deliver(
        bound_session_local,
        _starting(booking_canceled(), _at(days=2)),
        event_hint="booking-canceled",
        payload_hash="canceled-1",
    )

    assert await _jobs(bound_session_local, "comeback_3d") == []
    assert await _jobs(bound_session_local, "record_canceled") == []


async def test_an_opted_out_client_earns_no_comeback(bound_session_local, monkeypatch) -> None:
    _retention_on(monkeypatch)
    await _seed_booking(bound_session_local, start=_at(days=2))
    await _seed_client_counter(bound_session_local)
    async with bound_session_local() as session:
        async with session.begin():
            client = (await session.execute(select(Client).where(Client.provider == "easyweek"))).scalars().one()
            client.wa_opted_out = True

    await _deliver(
        bound_session_local,
        _starting(booking_canceled(), _at(days=2)),
        event_hint="booking-canceled",
        payload_hash="canceled-1",
    )

    assert await _jobs(bound_session_local, "comeback_3d") == []


async def test_a_disallowed_category_earns_no_comeback(bound_session_local, monkeypatch) -> None:
    _retention_on(monkeypatch)
    await _seed_booking(bound_session_local, start=_at(days=2))
    await _seed_client_counter(bound_session_local)
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", json.dumps(["Something Else"]), raising=False)

    await _deliver(
        bound_session_local,
        _starting(booking_canceled(), _at(days=2)),
        event_hint="booking-canceled",
        payload_hash="canceled-1",
    )

    assert await _jobs(bound_session_local, "comeback_3d") == []


async def test_a_restored_booking_withdraws_its_queued_comeback(bound_session_local, monkeypatch) -> None:
    """A booking that is active again owes no invitation to come back.

    The cancel path is terminal for the lifecycle, so the restoration is applied
    directly to the row — which is exactly the state an out-of-band repair or a
    future un-cancel signal would produce, and the withdrawal must survive it.
    """
    _retention_on(monkeypatch)
    await _seed_booking(bound_session_local, start=_at(days=2))
    await _seed_client_counter(bound_session_local)
    await _deliver(
        bound_session_local,
        _starting(booking_canceled(), _at(days=2)),
        event_hint="booking-canceled",
        payload_hash="canceled-1",
    )
    assert len(await _jobs(bound_session_local, "comeback_3d")) == 1

    async with bound_session_local() as session:
        async with session.begin():
            record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
            record.is_deleted = False

    await _deliver(
        bound_session_local,
        _starting(booking_created(), _at(days=3)),
        event_hint="booking-updated",
        payload_hash="updated-after-restore",
    )

    jobs = await _jobs(bound_session_local, "comeback_3d")
    assert len(jobs) == 1
    assert jobs[0].status == "canceled"
    assert "withdrawn" in (jobs[0].last_error or "")


# ===========================================================================
# Nothing that was already working may change
# ===========================================================================


async def test_lifecycle_and_reminders_are_untouched_by_retention(bound_session_local, monkeypatch) -> None:
    """PR-12 adds messages; it does not alter the ones already flowing."""
    _retention_on(monkeypatch)
    monkeypatch.setattr(settings, "easyweek_reminders_enabled", True, raising=False)

    await _seed_booking(bound_session_local, start=_at(days=2))

    assert len(await _jobs(bound_session_local, "record_created")) == 1
    assert len(await _jobs(bound_session_local, "reminder_24h")) == 1
    assert len(await _jobs(bound_session_local, "reminder_2h")) == 1
    assert await _jobs(bound_session_local, "repeat_10d") == []
    assert await _jobs(bound_session_local, "comeback_3d") == []
