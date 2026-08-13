"""PR-8: the preflight must be hard to pass and impossible to write through.

This command is what earns the right to open the send fence, so its failure mode
is a false green: reporting "ready" on an empty queue, on a bounded slice of a
longer one, or on jobs it never actually checked. Each of those is pinned here.

It is also the only EasyWeek ops command that talks to the live API while
production data is sitting in front of it, so the other half of these tests is
that it writes nothing at all — not a job, not a record, not an event.

The API client is injected, so nothing here reaches the network.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import select

from altegio_bot.easyweek_client import EasyWeekAuthError, EasyWeekRetryableError
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    Client,
    MessageJob,
    OutboxMessage,
    Record,
)
from altegio_bot.scripts.easyweek_reminder_preflight import (
    OPEN_STATUSES,
    PreflightReport,
    _parse_args,
    run_preflight,
    select_open_reminder_jobs,
)
from altegio_bot.settings import settings

BOOKING = uuid.UUID("11111111-2222-4333-8444-555555555555")
LOCATION_UUID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
COMPANY_ID = 999001
STARTS_AT = datetime(2026, 9, 14, 8, 30, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _registry(monkeypatch: pytest.MonkeyPatch) -> None:
    import json

    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "test-branch": {
                    "location_id": COMPANY_ID,
                    "location_uuid": LOCATION_UUID,
                    "meta_template_prefix": "tb",
                    "booking_page_url": "https://booking.example.invalid/test",
                }
            }
        ),
        raising=False,
    )


def _api(**overrides: Any) -> dict[str, Any]:
    payload = {
        "uuid": str(BOOKING),
        "location_uuid": LOCATION_UUID,
        "start_time": STARTS_AT.isoformat(),
        "is_canceled": False,
        "is_completed": False,
    }
    payload.update(overrides)
    return payload


class FakeReader:
    """Answers per booking uuid; records every call."""

    def __init__(self, payload: Any = None, *, error: Exception | None = None) -> None:
        self.payload = payload
        self.error = error
        self.calls: list[str] = []

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        self.calls.append(booking_uuid)
        if self.error is not None:
            raise self.error
        if self.payload is not None:
            return self.payload
        # Answer for the booking that was actually asked about, the way the real
        # API does — otherwise a multi-booking test would fail on the fake's
        # limitation rather than on anything the guard decided.
        return _api(uuid=booking_uuid)


async def _noop_sleep(_seconds: float) -> None:
    return None


async def _seed(
    session,
    *,
    booking: uuid.UUID = BOOKING,
    job_type: str = "reminder_24h",
    status: str = "queued",
    provider: str = PROVIDER_EASYWEEK,
    company_id: int = COMPANY_ID,
    starts_at: datetime = STARTS_AT,
    planned_start: datetime | None = None,
    suffix: str = "1",
) -> MessageJob:
    client = Client(
        provider=provider,
        company_id=company_id,
        altegio_client_id=int(suffix) + 7000,
        display_name="Anna",
        phone_e164=f"+4917000000{suffix}",
        raw={},
    )
    session.add(client)
    await session.flush()

    record = Record(
        provider=provider,
        company_id=company_id,
        altegio_record_id=4200000 + int(suffix),
        easyweek_booking_uuid=booking if provider == PROVIDER_EASYWEEK else None,
        client_id=client.id,
        staff_name="Tanja",
        starts_at=starts_at,
        raw={},
    )
    session.add(record)
    await session.flush()

    job = MessageJob(
        provider=provider,
        company_id=company_id,
        record_id=record.id,
        client_id=client.id,
        job_type=job_type,
        status=status,
        dedupe_key=f"preflight-{job_type}-{suffix}",
        run_at=STARTS_AT - timedelta(hours=24),
        payload={
            "provider": "easyweek",
            "booking_uuid": str(booking),
            "company_id": company_id,
            "job_type": job_type,
            "record_starts_at": (planned_start or starts_at).isoformat(),
        },
    )
    session.add(job)
    await session.flush()
    return job


# ---------------------------------------------------------------------------
# What counts as a candidate
# ---------------------------------------------------------------------------


async def test_only_open_easyweek_reminder_jobs_are_candidates(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            queued = await _seed(session, suffix="1")
            processing = await _seed(
                session,
                booking=uuid.UUID("22222222-2222-4333-8444-555555555555"),
                job_type="reminder_2h",
                status="processing",
                suffix="2",
            )
            # None of these may be selected.
            await _seed(
                session,
                booking=uuid.UUID("33333333-2222-4333-8444-555555555555"),
                job_type="record_created",
                suffix="3",
            )
            await _seed(
                session,
                booking=uuid.UUID("44444444-2222-4333-8444-555555555555"),
                status="done",
                suffix="4",
            )
            await _seed(
                session,
                booking=uuid.UUID("55555555-2222-4333-8444-555555555555"),
                status="canceled",
                suffix="5",
            )
            altegio = await _seed(session, provider=PROVIDER_ALTEGIO, suffix="6")

    async with session_maker() as session:
        jobs, truncated = await select_open_reminder_jobs(session, limit=50)

    ids = {job.id for job in jobs}
    assert ids == {queued.id, processing.id}
    assert altegio.id not in ids, "an Altegio reminder is a different subsystem"
    assert truncated is False


def test_the_open_statuses_are_exactly_queued_and_processing() -> None:
    """A job claimed a second ago is still an open reminder."""
    assert set(OPEN_STATUSES) == {"queued", "processing"}


# ---------------------------------------------------------------------------
# The exit-code contract
# ---------------------------------------------------------------------------


async def test_every_candidate_proven_is_the_only_green(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed(session, suffix="1")

    reader = FakeReader()
    async with session_maker() as session:
        report = await run_preflight(session, client=reader, sleep=_noop_sleep)

    assert report.candidate_count == 1
    assert report.checked_count == 1
    assert report.truncated is False
    assert report.outcomes == {"proven_current": 1}
    assert report.ready is True
    assert reader.calls == [str(BOOKING)]


async def test_an_empty_queue_is_never_green(session_maker) -> None:
    """A fence opened on the strength of "no problems found" is opened blind."""
    async with session_maker() as session:
        report = await run_preflight(session, client=FakeReader(), sleep=_noop_sleep)

    assert report.candidate_count == 0
    assert report.ready is False


async def test_a_truncated_queue_is_never_green(session_maker) -> None:
    """A bounded look at a longer queue says nothing about the rest."""
    async with session_maker() as session:
        async with session.begin():
            for index in range(3):
                await _seed(
                    session,
                    booking=uuid.UUID(f"1111111{index}-2222-4333-8444-555555555555"),
                    suffix=str(index + 1),
                )

    async with session_maker() as session:
        report = await run_preflight(session, client=FakeReader(), limit=2, sleep=_noop_sleep)

    assert report.truncated is True
    assert report.candidate_count == 2
    assert report.outcomes == {"proven_current": 2}
    assert report.ready is False, "all-proven does not rescue a truncated look"


async def test_one_unprovable_job_fails_the_whole_preflight(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            good = await _seed(session, suffix="1")
            moved = await _seed(
                session,
                booking=uuid.UUID("22222222-2222-4333-8444-555555555555"),
                job_type="reminder_2h",
                starts_at=STARTS_AT + timedelta(hours=2),
                planned_start=STARTS_AT,
                suffix="2",
            )

    async with session_maker() as session:
        report = await run_preflight(session, client=FakeReader(), sleep=_noop_sleep)

    assert report.candidate_count == 2
    assert report.outcomes["proven_current"] == 1
    assert report.outcomes["start_time_mismatch"] == 1
    assert report.ready is False
    assert report.unproven_job_ids == [moved.id]
    assert good.id not in report.unproven_job_ids


@pytest.mark.parametrize(
    ("label", "error", "expected"),
    [
        (
            "rate-limited",
            EasyWeekRetryableError("t", operation="get_booking", status_code=429),
            "retryable_unavailable",
        ),
        ("unauthorized", EasyWeekAuthError("a", operation="get_booking", status_code=401), "configuration_unavailable"),
    ],
)
async def test_an_api_we_cannot_reach_is_red_rather_than_green(
    session_maker, label: str, error: Exception, expected: str
) -> None:
    """ "We could not ask" must never be reported as "everything is fine"."""
    async with session_maker() as session:
        async with session.begin():
            await _seed(session, suffix="1")

    async with session_maker() as session:
        report = await run_preflight(session, client=FakeReader(error=error), sleep=_noop_sleep)

    assert report.outcomes == {expected: 1}, label
    assert report.ready is False, label


async def test_a_cancelled_booking_is_reported_and_not_green(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed(session, suffix="1")

    async with session_maker() as session:
        report = await run_preflight(session, client=FakeReader(_api(is_canceled=True)), sleep=_noop_sleep)

    assert report.outcomes == {"canceled": 1}
    assert report.ready is False


def test_ready_requires_all_three_conditions_together() -> None:
    """Stated directly, so no future edit can loosen one of them by accident."""
    proven = PreflightReport(candidate_count=2, checked_count=2, truncated=False)
    proven.outcomes["proven_current"] = 2
    assert proven.ready is True

    for broken in (
        PreflightReport(candidate_count=0, checked_count=0, truncated=False),
        PreflightReport(candidate_count=2, checked_count=2, truncated=True),
        PreflightReport(candidate_count=2, checked_count=1, truncated=False),
    ):
        broken.outcomes["proven_current"] = broken.checked_count
        assert broken.ready is False


# ---------------------------------------------------------------------------
# Read-only, and quiet about the booking
# ---------------------------------------------------------------------------


async def test_the_preflight_changes_no_row_at_all(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            job = await _seed(session, suffix="1")
    job_id = job.id

    async with session_maker() as session:
        before_jobs = (await session.execute(select(MessageJob.id, MessageJob.status, MessageJob.attempts))).all()
        before_records = (await session.execute(select(Record.id, Record.starts_at, Record.is_deleted))).all()

    async with session_maker() as session:
        await run_preflight(session, client=FakeReader(_api(is_canceled=True)), sleep=_noop_sleep)

    async with session_maker() as session:
        after_jobs = (await session.execute(select(MessageJob.id, MessageJob.status, MessageJob.attempts))).all()
        after_records = (await session.execute(select(Record.id, Record.starts_at, Record.is_deleted))).all()
        outbox = (await session.execute(select(OutboxMessage.id))).all()

    assert after_jobs == before_jobs, "a refused reminder must not be cancelled by an audit"
    assert after_records == before_records
    assert outbox == [], "nothing may be queued for delivery"
    assert any(row[0] == job_id for row in after_jobs)


async def test_the_report_names_ids_and_reason_codes_but_no_booking_data(session_maker) -> None:
    """This output is read in a terminal and pasted into tickets."""
    async with session_maker() as session:
        async with session.begin():
            await _seed(session, suffix="1")

    async with session_maker() as session:
        report = await run_preflight(
            session,
            client=FakeReader(
                _api(
                    is_canceled=True,
                    customer={"name": "Anna Müller", "phone": "+491701234567"},
                    service_name="Wimpernverlängerung",
                )
            ),
            sleep=_noop_sleep,
        )

    text = str(report.as_safe_dict())
    for leak in ("Anna", "+4917", "Wimpernverlängerung", str(BOOKING), LOCATION_UUID, "easyweek.io", "Bearer"):
        assert leak not in text, f"the report leaked {leak!r}"
    assert "canceled" in text and "ready" in text
    assert report.as_safe_dict()["mode"] == "read-only"


async def test_calls_are_sequential_and_paced_for_the_rate_limit(session_maker) -> None:
    """EasyWeek allows 60/min; the pause is per additional call, not per call."""
    async with session_maker() as session:
        async with session.begin():
            for index in range(3):
                await _seed(
                    session,
                    booking=uuid.UUID(f"1111111{index}-2222-4333-8444-555555555555"),
                    suffix=str(index + 1),
                )

    waits: list[float] = []

    async def _record_sleep(seconds: float) -> None:
        waits.append(seconds)

    reader = FakeReader()
    async with session_maker() as session:
        report = await run_preflight(session, client=reader, pause_sec=0.25, sleep=_record_sleep)

    assert report.checked_count == 3
    assert len(reader.calls) == 3
    assert waits == [0.25, 0.25], "one pause between calls, none before the first"


# ---------------------------------------------------------------------------
# The command line
# ---------------------------------------------------------------------------


def test_the_defaults_are_bounded_and_paced() -> None:
    args = _parse_args([])
    assert args.limit > 0
    assert args.pause_sec > 0


@pytest.mark.parametrize("argv", [["--limit", "0"], ["--pause-sec", "-1"]])
def test_nonsensical_bounds_are_refused(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        _parse_args(argv)
