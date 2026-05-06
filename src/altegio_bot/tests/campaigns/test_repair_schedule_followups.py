"""Tests for repair_schedule_campaign_followups script.

Coverage:
  A. dry-run does not create jobs
  B. apply creates jobs for unread delivered recipients
  C. read recipient skipped
  D. booked_after recipient skipped
  E. future record recipient skipped via final eligibility guard
  F. create-event after campaign skipped via final eligibility guard
  G. existing followup_message_job_id skipped
  H. existing MessageJob with same run/recipient skipped
  I. created job payload contains campaign_run_id and campaign_recipient_id
  J. run_at uses recipient.sent_at + delay
  K. --apply writes followup_status for skipped booked/future recipient
  L. Anna-like case: delivered/unread + record create-event after sent_at + future record;
     no job created; on --apply recipient gets followup_status and booked_after_at.

P2 hardening (status allow-list + time freeze):
  M. failed recipient with provider_message_id is not scheduled
  N. cleanup_failed recipient is not scheduled
  O. provider_accepted recipient can be scheduled
  P. excluded_reason=provider_error blocks scheduling
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select

import altegio_bot.scripts.repair_schedule_campaign_followups as repair
from altegio_bot.campaigns.runner import FOLLOWUP_JOB_TYPE
from altegio_bot.models.models import (
    AltegioEvent,
    CampaignRecipient,
    CampaignRun,
    Client,
    MessageJob,
    Record,
)
from altegio_bot.scripts.repair_schedule_campaign_followups import (
    _repair_dedupe_key,
    schedule_followups,
)

COMPANY = 758285
PHONE = "+49999000111"
NOW = datetime(2026, 5, 6, 12, 0, 0, tzinfo=timezone.utc)
SENT_AT = NOW - timedelta(days=20)
COMPLETED_AT = NOW - timedelta(days=20)
DELAY_DAYS = 14
EXPECTED_RUN_AT = SENT_AT + timedelta(days=DELAY_DAYS)


# ---------------------------------------------------------------------------
# Time freeze: patch repair.utcnow so all tests use a fixed 'now'.
# Without this, tests that rely on future records (starts_at=NOW+N days)
# become flaky once wall-clock time overtakes the fixed NOW.
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def freeze_repair_now(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(repair, "utcnow", lambda: NOW)


# ---------------------------------------------------------------------------
# Shared builders
# ---------------------------------------------------------------------------


def _make_run(session, **kw) -> CampaignRun:
    defaults = dict(
        campaign_code="new_clients_monthly",
        mode="send-real",
        status="completed",
        company_ids=[COMPANY],
        location_id=1,
        period_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        period_end=datetime(2026, 2, 1, tzinfo=timezone.utc),
        followup_enabled=True,
        followup_delay_days=DELAY_DAYS,
        followup_policy="unread_only",
        followup_template_name="repair_test_tpl",
        completed_at=COMPLETED_AT,
        meta={},
    )
    defaults.update(kw)
    run = CampaignRun(**defaults)
    session.add(run)
    return run


def _make_client(session, **kw) -> Client:
    defaults = dict(
        company_id=COMPANY,
        altegio_client_id=77001,
        phone_e164=PHONE,
        display_name="Repair Tester",
        raw={},
        wa_opted_out=False,
    )
    defaults.update(kw)
    client = Client(**defaults)
    session.add(client)
    return client


def _make_recipient(session, run_id: int, client_id: int | None, **kw) -> CampaignRecipient:
    defaults = dict(
        campaign_run_id=run_id,
        company_id=COMPANY,
        client_id=client_id,
        altegio_client_id=77001,
        phone_e164=PHONE,
        display_name="Repair Tester",
        status="delivered",
        provider_message_id="wamid.repair001",
        sent_at=SENT_AT,
        followup_status=None,
        followup_message_job_id=None,
        meta={},
    )
    defaults.update(kw)
    r = CampaignRecipient(**defaults)
    session.add(r)
    return r


def _make_existing_followup_job(
    session,
    *,
    run_id: int,
    recipient_id: int,
    client_id: int | None,
    dedupe_suffix: str = "",
) -> MessageJob:
    job = MessageJob(
        company_id=COMPANY,
        client_id=client_id,
        record_id=None,
        job_type=FOLLOWUP_JOB_TYPE,
        run_at=NOW,
        status="queued",
        attempts=0,
        max_attempts=5,
        dedupe_key=f"repair-test-existing:{run_id}:{recipient_id}{dedupe_suffix}",
        payload={
            "kind": FOLLOWUP_JOB_TYPE,
            "campaign_run_id": run_id,
            "campaign_recipient_id": recipient_id,
            "template_name": "repair_test_tpl",
        },
    )
    session.add(job)
    return job


def _make_record(
    session,
    *,
    client_id: int | None,
    altegio_record_id: int,
    starts_at: datetime,
    altegio_client_id: int = 77001,
    company_id: int = COMPANY,
    is_deleted: bool = False,
) -> Record:
    r = Record(
        company_id=company_id,
        altegio_record_id=altegio_record_id,
        client_id=client_id,
        altegio_client_id=altegio_client_id,
        starts_at=starts_at,
        is_deleted=is_deleted,
        raw={},
    )
    session.add(r)
    return r


def _make_altegio_event(
    session,
    *,
    resource_id: int,
    received_at: datetime,
    company_id: int = COMPANY,
    dedupe: str = "repair-ev-1",
) -> AltegioEvent:
    ev = AltegioEvent(
        dedupe_key=dedupe,
        company_id=company_id,
        resource="record",
        event_status="create",
        resource_id=resource_id,
        received_at=received_at,
        query={},
        headers={},
        payload={},
    )
    session.add(ev)
    return ev


# ---------------------------------------------------------------------------
# A. dry-run does not create jobs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dry_run_does_not_create_jobs(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            _make_recipient(session, run.id, client.id)
            await session.flush()
            run_id = run.id

    stats = await schedule_followups(run_id, dry_run=True, session_factory=session_maker)

    assert stats.candidates == 1
    assert stats.created == 0

    async with session_maker() as session:
        count = await session.scalar(
            select(func.count()).select_from(MessageJob).where(MessageJob.job_type == FOLLOWUP_JOB_TYPE)
        )
    assert count == 0


# ---------------------------------------------------------------------------
# B. apply creates jobs for unread delivered recipients
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_creates_jobs_for_unread_delivered(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id, status="delivered")
            await session.flush()
            run_id = run.id
            recipient_id = recipient.id

    stats = await schedule_followups(run_id, dry_run=False, session_factory=session_maker)

    assert stats.created == 1
    assert stats.candidates == 1

    async with session_maker() as session:
        job = await session.scalar(
            select(MessageJob).where(MessageJob.dedupe_key == _repair_dedupe_key(run_id, recipient_id))
        )
        db_recipient = await session.get(CampaignRecipient, recipient_id)

    assert job is not None
    assert job.status == "queued"
    assert job.job_type == FOLLOWUP_JOB_TYPE
    assert db_recipient is not None
    assert db_recipient.followup_status == "followup_queued"
    assert db_recipient.followup_message_job_id == job.id


# ---------------------------------------------------------------------------
# C. read recipient is skipped
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_recipient_skipped(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            _make_recipient(
                session,
                run.id,
                client.id,
                status="read",
                read_at=NOW - timedelta(days=5),
            )
            await session.flush()
            run_id = run.id

    stats = await schedule_followups(run_id, dry_run=True, session_factory=session_maker)

    assert stats.candidates == 0
    assert stats.skipped_read >= 1


# ---------------------------------------------------------------------------
# D. booked_after recipient is skipped
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_booked_after_recipient_skipped(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            _make_recipient(
                session,
                run.id,
                client.id,
                status="booked_after_campaign",
                booked_after_at=NOW - timedelta(days=3),
            )
            await session.flush()
            run_id = run.id

    stats = await schedule_followups(run_id, dry_run=True, session_factory=session_maker)

    assert stats.candidates == 0
    assert stats.skipped_read + stats.skipped_booked_after >= 1


# ---------------------------------------------------------------------------
# E. future record blocks follow-up via final eligibility guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_future_record_skipped_via_final_guard(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            _make_recipient(session, run.id, client.id, status="delivered")
            await session.flush()

            _make_record(
                session,
                client_id=client.id,
                altegio_record_id=8001,
                starts_at=NOW + timedelta(days=10),
                is_deleted=False,
            )
            await session.flush()
            run_id = run.id

    stats = await schedule_followups(run_id, dry_run=True, session_factory=session_maker)

    assert stats.candidates == 0
    assert stats.skipped_future_record == 1


# ---------------------------------------------------------------------------
# F. create-event after campaign skipped via final eligibility guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_event_after_campaign_skipped_via_final_guard(session_maker) -> None:
    """AltegioEvent record create after sent_at blocks follow-up (skipped_booked_after)."""
    altegio_record_id = 9501
    event_received_at = SENT_AT + timedelta(days=2)

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            _make_recipient(session, run.id, client.id, status="delivered", sent_at=SENT_AT)
            await session.flush()

            _make_record(
                session,
                client_id=client.id,
                altegio_record_id=altegio_record_id,
                starts_at=SENT_AT + timedelta(days=10),
                is_deleted=False,
            )
            _make_altegio_event(
                session,
                resource_id=altegio_record_id,
                received_at=event_received_at,
                dedupe="repair-test-f-ev",
            )
            await session.flush()
            run_id = run.id

    stats = await schedule_followups(run_id, dry_run=True, session_factory=session_maker)

    assert stats.candidates == 0
    assert stats.skipped_booked_after >= 1


# ---------------------------------------------------------------------------
# G. existing followup_message_job_id skipped
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_existing_followup_job_id_skipped(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            existing_job = _make_existing_followup_job(session, run_id=run.id, recipient_id=999, client_id=client.id)
            await session.flush()
            _make_recipient(
                session,
                run.id,
                client.id,
                followup_message_job_id=existing_job.id,
            )
            await session.flush()
            run_id = run.id

    stats = await schedule_followups(run_id, dry_run=True, session_factory=session_maker)

    assert stats.candidates == 0
    assert stats.skipped_existing_job >= 1


# ---------------------------------------------------------------------------
# H. existing MessageJob with same run_id + recipient_id skipped
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_existing_message_job_same_run_recipient_skipped(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id)
            await session.flush()
            recipient_id = recipient.id

            _make_existing_followup_job(
                session,
                run_id=run.id,
                recipient_id=recipient_id,
                client_id=client.id,
                dedupe_suffix=":h",
            )
            await session.flush()
            run_id = run.id

    stats = await schedule_followups(run_id, dry_run=True, session_factory=session_maker)

    assert stats.candidates == 0
    assert stats.skipped_existing_job >= 1


# ---------------------------------------------------------------------------
# I. created job payload contains campaign_run_id and campaign_recipient_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_created_job_payload_contains_run_and_recipient_ids(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id)
            await session.flush()
            run_id = run.id
            recipient_id = recipient.id

    await schedule_followups(run_id, dry_run=False, session_factory=session_maker)

    async with session_maker() as session:
        job = await session.scalar(
            select(MessageJob).where(MessageJob.dedupe_key == _repair_dedupe_key(run_id, recipient_id))
        )

    assert job is not None
    assert job.payload["campaign_run_id"] == run_id
    assert job.payload["campaign_recipient_id"] == recipient_id


# ---------------------------------------------------------------------------
# J. run_at uses recipient.sent_at + delay
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_at_uses_sent_at_plus_delay(session_maker) -> None:
    custom_sent_at = datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc)
    expected_run_at = custom_sent_at + timedelta(days=DELAY_DAYS)

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id, sent_at=custom_sent_at)
            await session.flush()
            run_id = run.id
            recipient_id = recipient.id

    await schedule_followups(run_id, dry_run=False, session_factory=session_maker)

    async with session_maker() as session:
        job = await session.scalar(
            select(MessageJob).where(MessageJob.dedupe_key == _repair_dedupe_key(run_id, recipient_id))
        )

    assert job is not None
    diff = abs((job.run_at - expected_run_at).total_seconds())
    assert diff < 2, f"run_at={job.run_at!r}, expected≈{expected_run_at!r}"


# ---------------------------------------------------------------------------
# K. --apply writes followup_status for skipped booked/future recipient
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_writes_followup_status_for_skipped_future_record(session_maker) -> None:
    """--apply persists followup_status='skipped_future_record' on ineligible recipient."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id, status="delivered")
            await session.flush()
            recipient_id = recipient.id

            _make_record(
                session,
                client_id=client.id,
                altegio_record_id=8002,
                starts_at=NOW + timedelta(days=7),
                is_deleted=False,
            )
            await session.flush()
            run_id = run.id

    stats = await schedule_followups(run_id, dry_run=False, session_factory=session_maker)

    assert stats.candidates == 0
    assert stats.skipped_future_record == 1
    assert stats.created == 0

    async with session_maker() as session:
        db_recipient = await session.get(CampaignRecipient, recipient_id)

    assert db_recipient is not None
    assert db_recipient.followup_status == "skipped_future_record"


@pytest.mark.asyncio
async def test_apply_writes_followup_status_for_skipped_booked_after(session_maker) -> None:
    """--apply persists followup_status='skipped_booked_after' and backfills booked_after_at."""
    altegio_record_id = 9502
    event_received_at = SENT_AT + timedelta(days=3)

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id, status="delivered", sent_at=SENT_AT)
            await session.flush()
            recipient_id = recipient.id

            _make_record(
                session,
                client_id=client.id,
                altegio_record_id=altegio_record_id,
                starts_at=SENT_AT + timedelta(days=10),
            )
            _make_altegio_event(
                session,
                resource_id=altegio_record_id,
                received_at=event_received_at,
                dedupe="repair-test-k-ev",
            )
            await session.flush()
            run_id = run.id

    stats = await schedule_followups(run_id, dry_run=False, session_factory=session_maker)

    assert stats.candidates == 0
    assert stats.skipped_booked_after >= 1
    assert stats.created == 0

    async with session_maker() as session:
        db_recipient = await session.get(CampaignRecipient, recipient_id)

    assert db_recipient is not None
    assert db_recipient.followup_status == "skipped_booked_after"
    assert db_recipient.booked_after_at is not None
    assert abs((db_recipient.booked_after_at - event_received_at).total_seconds()) < 2


# ---------------------------------------------------------------------------
# L. Anna-like case
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_anna_like_case_dry_run_no_job(session_maker) -> None:
    """Delivered/unread recipient with record create-event after sent_at + future record:
    dry-run → no job created, skipped counter incremented.
    """
    altegio_record_id = 9600
    event_received_at = SENT_AT + timedelta(days=1)  # booked the day after the campaign

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            _make_recipient(
                session,
                run.id,
                client.id,
                status="delivered",
                sent_at=SENT_AT,
                read_at=None,
            )
            await session.flush()

            # Record created after campaign (appointment exists in DB)
            _make_record(
                session,
                client_id=client.id,
                altegio_record_id=altegio_record_id,
                starts_at=NOW + timedelta(days=1),  # future appointment
                is_deleted=False,
            )
            # AltegioEvent: booking received after campaign
            _make_altegio_event(
                session,
                resource_id=altegio_record_id,
                received_at=event_received_at,
                dedupe="repair-test-l-ev",
            )
            await session.flush()
            run_id = run.id

    stats = await schedule_followups(run_id, dry_run=True, session_factory=session_maker)

    assert stats.candidates == 0
    assert stats.created == 0
    assert stats.skipped_booked_after + stats.skipped_future_record >= 1

    # No MessageJob created
    async with session_maker() as session:
        count = await session.scalar(
            select(func.count()).select_from(MessageJob).where(MessageJob.job_type == FOLLOWUP_JOB_TYPE)
        )
    assert count == 0


@pytest.mark.asyncio
async def test_anna_like_case_apply_sets_status_and_booked_after_at(session_maker) -> None:
    """--apply: Anna-like recipient gets followup_status set and booked_after_at backfilled."""
    altegio_record_id = 9601
    event_received_at = SENT_AT + timedelta(days=1)

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(
                session,
                run.id,
                client.id,
                status="delivered",
                sent_at=SENT_AT,
                read_at=None,
            )
            await session.flush()
            recipient_id = recipient.id

            _make_record(
                session,
                client_id=client.id,
                altegio_record_id=altegio_record_id,
                starts_at=NOW + timedelta(days=1),
                is_deleted=False,
            )
            _make_altegio_event(
                session,
                resource_id=altegio_record_id,
                received_at=event_received_at,
                dedupe="repair-test-l2-ev",
            )
            await session.flush()
            run_id = run.id

    stats = await schedule_followups(run_id, dry_run=False, session_factory=session_maker)

    assert stats.created == 0
    # Guard fired: either skipped_booked_after (create-event path) or skipped_future_record
    assert stats.skipped_booked_after + stats.skipped_future_record >= 1

    async with session_maker() as session:
        db_recipient = await session.get(CampaignRecipient, recipient_id)

    assert db_recipient is not None
    # followup_status must be set (not None) to one of the skip statuses
    assert db_recipient.followup_status in {"skipped_booked_after", "skipped_future_record"}
    # If the guard returned skipped_booked_after, booked_after_at must be backfilled
    if db_recipient.followup_status == "skipped_booked_after":
        assert db_recipient.booked_after_at is not None
        assert abs((db_recipient.booked_after_at - event_received_at).total_seconds()) < 2


# ---------------------------------------------------------------------------
# M. failed recipient with provider_message_id is not scheduled (P2-1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_failed_recipient_status_is_not_scheduled(session_maker) -> None:
    """status='failed' must be blocked even when provider_message_id and sent_at are set."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            _make_recipient(
                session,
                run.id,
                client.id,
                status="failed",
                provider_message_id="wamid.failed-test",
                sent_at=SENT_AT,
            )
            await session.flush()
            run_id = run.id

    stats = await schedule_followups(run_id, dry_run=True, session_factory=session_maker)

    assert stats.candidates == 0
    assert stats.skipped_not_sent >= 1
    skip_rows = [r for r in stats.rows if r.decision == "skip"]
    assert any("non_positive_status" in (r.reason or "") for r in skip_rows)


# ---------------------------------------------------------------------------
# N. cleanup_failed recipient is not scheduled (P2-1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cleanup_failed_recipient_status_is_not_scheduled(session_maker) -> None:
    """status='cleanup_failed' must be blocked regardless of provider_message_id."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            _make_recipient(
                session,
                run.id,
                client.id,
                status="cleanup_failed",
                provider_message_id="wamid.cleanup-test",
                sent_at=SENT_AT,
            )
            await session.flush()
            run_id = run.id

    stats = await schedule_followups(run_id, dry_run=True, session_factory=session_maker)

    assert stats.candidates == 0
    assert stats.skipped_not_sent >= 1


# ---------------------------------------------------------------------------
# O. provider_accepted recipient can be scheduled (P2-1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_provider_accepted_recipient_can_be_scheduled(session_maker) -> None:
    """status='provider_accepted' is in the allow-list → should reach candidates."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            _make_recipient(
                session,
                run.id,
                client.id,
                status="provider_accepted",
                provider_message_id="wamid.pa-test",
                sent_at=SENT_AT,
            )
            await session.flush()
            run_id = run.id

    stats = await schedule_followups(run_id, dry_run=True, session_factory=session_maker)

    assert stats.candidates == 1
    assert stats.skipped_not_sent == 0


# ---------------------------------------------------------------------------
# P. excluded_reason=provider_error blocks scheduling even with positive status (P2-1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_excluded_reason_hard_failure_is_not_scheduled(session_maker) -> None:
    """excluded_reason='provider_error' must be blocked even if status='delivered'."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            _make_recipient(
                session,
                run.id,
                client.id,
                status="delivered",
                provider_message_id="wamid.excluded-test",
                sent_at=SENT_AT,
                excluded_reason="provider_error",
            )
            await session.flush()
            run_id = run.id

    stats = await schedule_followups(run_id, dry_run=True, session_factory=session_maker)

    assert stats.candidates == 0
    assert stats.skipped_not_sent >= 1
