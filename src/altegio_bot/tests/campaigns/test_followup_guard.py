"""Tests: final eligibility guard for follow-up campaigns.

Required guard-level tests:
  1. test_followup_guard_skips_read_status_without_read_at
  2. test_followup_guard_skips_read_at
  3. test_followup_guard_skips_existing_booked_after_at
  4. test_followup_guard_skips_record_create_after_campaign_even_if_deleted
  5. test_followup_guard_skips_future_active_record
  6. test_followup_guard_ignores_deleted_future_record
  7. test_followup_guard_ignores_different_client_or_company_record
  8. test_followup_guard_skips_opt_out_by_phone_fallback
  9. test_followup_guard_skips_opt_out_by_phone_when_client_id_stale
 10. test_followup_guard_allows_unread_without_booking_or_optout

Required outbox worker integration tests:
  1. test_outbox_worker_followup_missing_recipient_id_does_not_send
  2. test_outbox_worker_followup_missing_run_id_does_not_send
  3. test_outbox_worker_followup_recipient_not_found_does_not_send
  4. test_outbox_worker_followup_run_not_found_does_not_send
  5. test_outbox_worker_followup_future_record_does_not_send

Supplementary integration tests:
  - create-event after campaign sets booked_after_at
  - opted-out client blocks follow-up via outbox_worker
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

import altegio_bot.workers.outbox_worker as ow
from altegio_bot.campaigns.followup import check_followup_final_eligibility
from altegio_bot.campaigns.runner import FOLLOWUP_JOB_TYPE
from altegio_bot.models.models import (
    AltegioEvent,
    CampaignRecipient,
    CampaignRun,
    Client,
    MessageJob,
    Record,
)

COMPANY = 758285
PHONE = "+49111222333"
PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 2, 1, tzinfo=timezone.utc)
NOW = datetime(2026, 5, 5, 12, 0, 0, tzinfo=timezone.utc)
CAMPAIGN_SENT_AT = NOW - timedelta(days=15)
CAMPAIGN_COMPLETED_AT = NOW - timedelta(days=15)


# ---------------------------------------------------------------------------
# Shared builders
# ---------------------------------------------------------------------------


def _make_run(session, *, completed_at: datetime | None = None) -> CampaignRun:
    run = CampaignRun(
        campaign_code="new_clients_monthly",
        mode="send-real",
        status="completed",
        company_ids=[COMPANY],
        location_id=1,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        followup_enabled=True,
        followup_delay_days=14,
        followup_policy="unread_only",
        followup_template_name=FOLLOWUP_JOB_TYPE,
        completed_at=completed_at or CAMPAIGN_COMPLETED_AT,
        meta={},
    )
    session.add(run)
    return run


def _make_client(
    session,
    *,
    company_id: int = COMPANY,
    phone_e164: str = PHONE,
    wa_opted_out: bool = False,
) -> Client:
    client = Client(
        company_id=company_id,
        altegio_client_id=99001,
        phone_e164=phone_e164,
        display_name="Test Client",
        raw={},
        wa_opted_out=wa_opted_out,
    )
    session.add(client)
    return client


def _make_recipient(
    session,
    run_id: int,
    client_id: int | None,
    *,
    status: str = "queued",
    read_at: datetime | None = None,
    booked_after_at: datetime | None = None,
    sent_at: datetime | None = None,
    altegio_client_id: int | None = None,
    phone_e164: str = PHONE,
) -> CampaignRecipient:
    r = CampaignRecipient(
        campaign_run_id=run_id,
        company_id=COMPANY,
        client_id=client_id,
        altegio_client_id=altegio_client_id or 99001,
        phone_e164=phone_e164,
        display_name="Test Client",
        status=status,
        read_at=read_at,
        booked_after_at=booked_after_at,
        sent_at=sent_at or CAMPAIGN_SENT_AT,
        followup_status="followup_planned",
        meta={},
    )
    session.add(r)
    return r


def _make_followup_job(
    session,
    *,
    run_id: int,
    recipient_id: int,
    client_id: int | None,
    company_id: int = COMPANY,
    run_at: datetime | None = None,
    include_recipient_id: bool = True,
    include_run_id: bool = True,
) -> MessageJob:
    payload: dict = {
        "kind": FOLLOWUP_JOB_TYPE,
        "phone_e164": PHONE,
        "contact_name": "Test Client",
    }
    if include_recipient_id:
        payload["campaign_recipient_id"] = recipient_id
    if include_run_id:
        payload["campaign_run_id"] = run_id
    job = MessageJob(
        company_id=company_id,
        client_id=client_id,
        record_id=None,
        job_type=FOLLOWUP_JOB_TYPE,
        run_at=run_at or NOW,
        status="queued",
        attempts=0,
        max_attempts=5,
        dedupe_key=f"followup-guard-test:{run_id}:{recipient_id}",
        payload=payload,
    )
    session.add(job)
    return job


def _make_record(
    session,
    *,
    company_id: int = COMPANY,
    client_id: int | None,
    altegio_client_id: int = 99001,
    altegio_record_id: int = 7001,
    starts_at: datetime,
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
    company_id: int = COMPANY,
    resource_id: int,
    received_at: datetime,
    dedupe: str = "ev-guard-test-1",
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
# Fixture: patch outbox_worker.SessionLocal with the test session_maker
# ---------------------------------------------------------------------------


@pytest.fixture
def ow_session(session_maker, monkeypatch):
    monkeypatch.setattr(ow, "SessionLocal", session_maker)
    return session_maker


# ---------------------------------------------------------------------------
# Helper: lock a job so process_job_in_session can pick it up
# ---------------------------------------------------------------------------


async def _lock_job(session_maker, job_id: int) -> None:
    async with session_maker() as session:
        async with session.begin():
            job = await session.get(MessageJob, job_id)
            job.status = "processing"


# ---------------------------------------------------------------------------
# 1. status='read' and read_at is None → skipped_read
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_followup_guard_skips_read_status_without_read_at(session_maker) -> None:
    """status='read' blocks follow-up even when read_at is None."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id, status="read", read_at=None)
            await session.flush()

            result = await check_followup_final_eligibility(session, recipient, run, NOW)

    assert result.eligible is False
    assert result.followup_status == "skipped_read"
    assert result.skip_reason is not None


# ---------------------------------------------------------------------------
# 2. read_at is not None → skipped_read
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_followup_guard_skips_read_at(session_maker) -> None:
    """read_at set blocks follow-up even when status is 'delivered'."""
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
                read_at=NOW - timedelta(days=1),
            )
            await session.flush()

            result = await check_followup_final_eligibility(session, recipient, run, NOW)

    assert result.eligible is False
    assert result.followup_status == "skipped_read"


# ---------------------------------------------------------------------------
# 3. booked_after_at is not None → skipped_booked_after
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_followup_guard_skips_existing_booked_after_at(session_maker) -> None:
    """booked_after_at set blocks follow-up regardless of read status."""
    booked_ts = NOW - timedelta(days=5)
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
                booked_after_at=booked_ts,
            )
            await session.flush()

            result = await check_followup_final_eligibility(session, recipient, run, NOW)

    assert result.eligible is False
    assert result.followup_status == "skipped_booked_after"
    assert result.booked_after_at == booked_ts


# ---------------------------------------------------------------------------
# 4. P1 regression: record create event EVEN IF RECORD IS DELETED → skip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_followup_guard_skips_record_create_after_campaign_even_if_deleted(
    session_maker,
) -> None:
    """A create-event for a record later deleted still blocks follow-up.

    Regression test for P1: _find_record_create_event() must NOT filter by
    is_deleted.  Even if the appointment was cancelled, the client booked —
    the marketing goal was achieved.
    """
    altegio_record_id = 9901
    event_received_at = CAMPAIGN_SENT_AT + timedelta(days=2)

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id, sent_at=CAMPAIGN_SENT_AT)
            await session.flush()

            # Record was created after the campaign but subsequently deleted/cancelled
            _make_record(
                session,
                client_id=client.id,
                altegio_record_id=altegio_record_id,
                starts_at=CAMPAIGN_SENT_AT + timedelta(days=10),
                is_deleted=True,  # <-- deleted after creation
            )
            _make_altegio_event(
                session,
                resource_id=altegio_record_id,
                received_at=event_received_at,
            )
            await session.flush()

            result = await check_followup_final_eligibility(session, recipient, run, NOW)

    assert result.eligible is False, "Deleted record create-event must still block follow-up (P1 regression)"
    assert result.followup_status == "skipped_booked_after"
    assert result.booked_after_at is not None
    assert abs((result.booked_after_at - event_received_at).total_seconds()) < 2


# ---------------------------------------------------------------------------
# 5. future active record → skipped_future_record
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_followup_guard_skips_future_active_record(session_maker) -> None:
    """A non-deleted future record blocks follow-up (skipped_future_record)."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id)
            await session.flush()

            _make_record(
                session,
                client_id=client.id,
                starts_at=NOW + timedelta(days=7),
                is_deleted=False,
            )
            await session.flush()

            result = await check_followup_final_eligibility(session, recipient, run, NOW)

    assert result.eligible is False
    assert result.followup_status == "skipped_future_record"


# ---------------------------------------------------------------------------
# 6. deleted future record → NOT blocking
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_followup_guard_ignores_deleted_future_record(session_maker) -> None:
    """A deleted future record must not block follow-up."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id)
            await session.flush()

            _make_record(
                session,
                client_id=client.id,
                starts_at=NOW + timedelta(days=5),
                is_deleted=True,
            )
            await session.flush()

            result = await check_followup_final_eligibility(session, recipient, run, NOW)

    assert result.eligible is True, f"Deleted future record must not block; got {result}"


# ---------------------------------------------------------------------------
# 7. different company / different client record → NOT blocking
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_followup_guard_ignores_different_client_or_company_record(session_maker) -> None:
    """Future records for other company or different client must not block."""
    OTHER_COMPANY = 999999
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session, company_id=COMPANY)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id)
            await session.flush()

            # Record for a different company and a different altegio_client_id
            _make_record(
                session,
                company_id=OTHER_COMPANY,
                client_id=None,
                altegio_client_id=77777,
                altegio_record_id=7777,
                starts_at=NOW + timedelta(days=3),
                is_deleted=False,
            )
            await session.flush()

            result = await check_followup_final_eligibility(session, recipient, run, NOW)

    assert result.eligible is True, f"Other-company record must not block; got {result}"


# ---------------------------------------------------------------------------
# 8. P3: missing campaign_recipient_id → job canceled, no send
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_outbox_worker_followup_missing_recipient_id_does_not_send(
    ow_session,
    session_maker,
) -> None:
    """Follow-up job with no campaign_recipient_id must be canceled (fail-closed)."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            # Create a recipient so we have a valid run_id, but DON'T include it in payload
            recipient = _make_recipient(session, run.id, client.id)
            await session.flush()

            job = _make_followup_job(
                session,
                run_id=run.id,
                recipient_id=recipient.id,
                client_id=client.id,
                include_recipient_id=False,  # <-- key: payload missing campaign_recipient_id
            )
            await session.flush()
            job_id = job.id

    provider = MagicMock()
    provider.send_template = AsyncMock()
    provider.send = AsyncMock()

    await _lock_job(session_maker, job_id)

    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, job_id, provider=provider)

    async with session_maker() as session:
        db_job = await session.get(MessageJob, job_id)

    assert db_job is not None
    assert db_job.status == "canceled", f"Expected canceled, got {db_job.status!r}"
    assert db_job.last_error is not None
    assert "campaign_recipient_id" in db_job.last_error

    provider.send_template.assert_not_called()
    provider.send.assert_not_called()


# ---------------------------------------------------------------------------
# 9. Future record → job canceled via outbox_worker, no send
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_outbox_worker_followup_future_record_does_not_send(
    ow_session,
    session_maker,
) -> None:
    """Follow-up job with a future active record must be canceled; provider not called."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id)
            await session.flush()
            recipient_id = recipient.id

            job = _make_followup_job(
                session,
                run_id=run.id,
                recipient_id=recipient.id,
                client_id=client.id,
            )
            await session.flush()
            job_id = job.id

            _make_record(
                session,
                client_id=client.id,
                starts_at=NOW + timedelta(days=7),
                is_deleted=False,
            )
            await session.flush()

    provider = MagicMock()
    provider.send_template = AsyncMock()

    await _lock_job(session_maker, job_id)

    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, job_id, provider=provider)

    async with session_maker() as session:
        db_job = await session.get(MessageJob, job_id)
        db_recipient = await session.get(CampaignRecipient, recipient_id)

    assert db_job is not None
    assert db_job.status == "canceled"
    assert db_recipient is not None
    assert db_recipient.followup_status == "skipped_future_record"
    provider.send_template.assert_not_called()


# ---------------------------------------------------------------------------
# 10. unread, no booking, no opt-out → eligible
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_followup_guard_allows_unread_without_booking_or_optout(session_maker) -> None:
    """Clean unread recipient with no future record → guard returns eligible=True."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session, wa_opted_out=False)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id)
            await session.flush()

            result = await check_followup_final_eligibility(session, recipient, run, NOW)

    assert result.eligible is True
    assert result.skip_reason is None
    assert result.followup_status is None
    assert result.booked_after_at is None


# ---------------------------------------------------------------------------
# Supplementary: create-event after campaign sets booked_after_at on recipient
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_outbox_worker_create_event_after_campaign_sets_booked_after_at(
    ow_session,
    session_maker,
) -> None:
    """AltegioEvent record create after campaign → job canceled, recipient.booked_after_at set."""
    altegio_record_id = 8801
    event_received_at = CAMPAIGN_SENT_AT + timedelta(days=3)

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id, sent_at=CAMPAIGN_SENT_AT)
            await session.flush()
            recipient_id = recipient.id

            job = _make_followup_job(
                session,
                run_id=run.id,
                recipient_id=recipient.id,
                client_id=client.id,
            )
            await session.flush()
            job_id = job.id

            _make_record(
                session,
                client_id=client.id,
                altegio_record_id=altegio_record_id,
                starts_at=CAMPAIGN_SENT_AT - timedelta(days=30),
                is_deleted=False,
            )
            _make_altegio_event(
                session,
                resource_id=altegio_record_id,
                received_at=event_received_at,
            )
            await session.flush()

    provider = MagicMock()
    provider.send_template = AsyncMock()

    await _lock_job(session_maker, job_id)

    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, job_id, provider=provider)

    async with session_maker() as session:
        db_job = await session.get(MessageJob, job_id)
        db_recipient = await session.get(CampaignRecipient, recipient_id)

    assert db_job is not None
    assert db_job.status == "canceled"
    assert db_recipient is not None
    assert db_recipient.followup_status == "skipped_booked_after"
    assert db_recipient.booked_after_at is not None
    assert abs((db_recipient.booked_after_at - event_received_at).total_seconds()) < 2
    provider.send_template.assert_not_called()


# ---------------------------------------------------------------------------
# Supplementary: opted-out client → job canceled via outbox_worker
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_outbox_worker_followup_opted_out_does_not_send(
    ow_session,
    session_maker,
) -> None:
    """Opted-out client → job canceled with skipped_opted_out; provider not called."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session, wa_opted_out=True)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id)
            await session.flush()
            recipient_id = recipient.id

            job = _make_followup_job(
                session,
                run_id=run.id,
                recipient_id=recipient.id,
                client_id=client.id,
            )
            await session.flush()
            job_id = job.id

    provider = MagicMock()
    provider.send_template = AsyncMock()

    await _lock_job(session_maker, job_id)

    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, job_id, provider=provider)

    async with session_maker() as session:
        db_job = await session.get(MessageJob, job_id)
        db_recipient = await session.get(CampaignRecipient, recipient_id)

    assert db_job is not None
    assert db_job.status == "canceled"
    assert db_recipient is not None
    assert db_recipient.followup_status == "skipped_opted_out"
    provider.send_template.assert_not_called()


# ---------------------------------------------------------------------------
# 8. Opt-out by phone fallback (client_id=None)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_followup_guard_skips_opt_out_by_phone_fallback(session_maker) -> None:
    """CRM-only recipient (client_id=None): phone lookup finds opted-out client → skipped_opted_out."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            opted_out = _make_client(session, phone_e164=PHONE, wa_opted_out=True)
            await session.flush()
            recipient = _make_recipient(
                session,
                run.id,
                client_id=None,  # CRM-only
                altegio_client_id=None,
                phone_e164=PHONE,
            )
            await session.flush()

            result = await check_followup_final_eligibility(session, recipient, run, NOW)

    assert result.eligible is False
    assert result.followup_status == "skipped_opted_out"
    assert opted_out is not None  # silence unused-variable warning


# ---------------------------------------------------------------------------
# 9. Opt-out by phone when client_id stale (primary lookup returns None)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_followup_guard_skips_opt_out_by_phone_when_client_id_stale() -> None:
    """client_id set but Client not found in DB → phone fallback finds opted-out client."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    # Build a fake recipient with a stale client_id
    recipient = SimpleNamespace(
        status="delivered",
        read_at=None,
        booked_after_at=None,
        client_id=999999,  # nonexistent in DB
        phone_e164=PHONE,
        altegio_client_id=None,
        company_id=COMPANY,
        sent_at=CAMPAIGN_SENT_AT,
    )
    opted_out_client = SimpleNamespace(wa_opted_out=True)

    # Fake session: get(Client, 999999) → None; execute(phone query) → opted-out client
    session = MagicMock()
    session.get = AsyncMock(return_value=None)

    async def _fake_execute(stmt):
        result = MagicMock()
        result.scalar_one_or_none = MagicMock(return_value=opted_out_client)
        return result

    session.execute = _fake_execute

    run = SimpleNamespace(completed_at=CAMPAIGN_COMPLETED_AT)

    result = await check_followup_final_eligibility(session, recipient, run, NOW)

    assert result.eligible is False
    assert result.followup_status == "skipped_opted_out"


# ---------------------------------------------------------------------------
# P5-2: missing campaign_run_id → job canceled, no send
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_outbox_worker_followup_missing_run_id_does_not_send(
    ow_session,
    session_maker,
) -> None:
    """Follow-up job with campaign_recipient_id but no campaign_run_id must be canceled."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id)
            await session.flush()

            job = _make_followup_job(
                session,
                run_id=run.id,
                recipient_id=recipient.id,
                client_id=client.id,
                include_run_id=False,  # <-- key: payload missing campaign_run_id
            )
            await session.flush()
            job_id = job.id

    provider = MagicMock()
    provider.send_template = AsyncMock()
    provider.send = AsyncMock()

    await _lock_job(session_maker, job_id)

    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, job_id, provider=provider)

    async with session_maker() as session:
        db_job = await session.get(MessageJob, job_id)

    assert db_job is not None
    assert db_job.status == "canceled", f"Expected canceled, got {db_job.status!r}"
    assert db_job.last_error is not None
    assert "campaign_run_id" in db_job.last_error

    provider.send_template.assert_not_called()
    provider.send.assert_not_called()


# ---------------------------------------------------------------------------
# P5-3: recipient not found → job canceled, no send
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_outbox_worker_followup_recipient_not_found_does_not_send(
    ow_session,
    session_maker,
) -> None:
    """Follow-up job with nonexistent campaign_recipient_id must be canceled (fail-closed)."""
    BOGUS_RECIPIENT_ID = 999_999_999

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()

            job = MessageJob(
                company_id=COMPANY,
                client_id=client.id,
                record_id=None,
                job_type=FOLLOWUP_JOB_TYPE,
                run_at=NOW,
                status="queued",
                attempts=0,
                max_attempts=5,
                dedupe_key="followup-guard-test:recipient-not-found",
                payload={
                    "kind": FOLLOWUP_JOB_TYPE,
                    "campaign_run_id": run.id,
                    "campaign_recipient_id": BOGUS_RECIPIENT_ID,
                    "phone_e164": PHONE,
                    "contact_name": "Ghost",
                },
            )
            session.add(job)
            await session.flush()
            job_id = job.id

    provider = MagicMock()
    provider.send_template = AsyncMock()
    provider.send = AsyncMock()

    await _lock_job(session_maker, job_id)

    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, job_id, provider=provider)

    async with session_maker() as session:
        db_job = await session.get(MessageJob, job_id)

    assert db_job is not None
    assert db_job.status == "canceled", f"Expected canceled, got {db_job.status!r}"
    assert db_job.last_error is not None
    assert "campaign_recipient_id" in db_job.last_error

    provider.send_template.assert_not_called()
    provider.send.assert_not_called()


# ---------------------------------------------------------------------------
# P5-4: run not found → job canceled, no send
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_outbox_worker_followup_run_not_found_does_not_send(
    ow_session,
    session_maker,
) -> None:
    """Follow-up job with nonexistent campaign_run_id must be canceled (fail-closed)."""
    BOGUS_RUN_ID = 999_999_998

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id)
            await session.flush()

            job = MessageJob(
                company_id=COMPANY,
                client_id=client.id,
                record_id=None,
                job_type=FOLLOWUP_JOB_TYPE,
                run_at=NOW,
                status="queued",
                attempts=0,
                max_attempts=5,
                dedupe_key="followup-guard-test:run-not-found",
                payload={
                    "kind": FOLLOWUP_JOB_TYPE,
                    "campaign_run_id": BOGUS_RUN_ID,
                    "campaign_recipient_id": recipient.id,
                    "phone_e164": PHONE,
                    "contact_name": "Ghost",
                },
            )
            session.add(job)
            await session.flush()
            job_id = job.id

    provider = MagicMock()
    provider.send_template = AsyncMock()
    provider.send = AsyncMock()

    await _lock_job(session_maker, job_id)

    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, job_id, provider=provider)

    async with session_maker() as session:
        db_job = await session.get(MessageJob, job_id)

    assert db_job is not None
    assert db_job.status == "canceled", f"Expected canceled, got {db_job.status!r}"
    assert db_job.last_error is not None
    assert "campaign_run_id" in db_job.last_error

    provider.send_template.assert_not_called()
    provider.send.assert_not_called()


# ==========================================================================
# Live Altegio guard for follow-up jobs
# ==========================================================================
#
# A. DB guard fires (recipient read) → live check NOT called, provider NOT called
# B. Eligible in DB + Altegio has future record → canceled, skipped_future_record
# C. Eligible in DB + no future Altegio record → provider called (send path reached)
# D. Altegio API error → retry with delay progression (1m/5m/25m/1h)
# E. max_attempts exhausted → job failed
# F. Non-followup job → live check NOT called
# G. DB guard fires (booked_after) → live check NOT called
# ==========================================================================


@pytest.mark.asyncio
async def test_live_altegio_guard_not_called_when_db_guard_fires_read(
    ow_session,
    session_maker,
    monkeypatch,
) -> None:
    """A. DB guard fires (recipient read) → live Altegio check not called, provider not called."""
    live_check = AsyncMock()
    monkeypatch.setattr(ow, "client_has_any_future_record", live_check)

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id, status="read", read_at=NOW - timedelta(days=1))
            await session.flush()
            recipient_id = recipient.id

            job = _make_followup_job(session, run_id=run.id, recipient_id=recipient.id, client_id=client.id)
            await session.flush()
            job_id = job.id

    provider = MagicMock()
    provider.send_template = AsyncMock()
    provider.send = AsyncMock()

    await _lock_job(session_maker, job_id)

    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, job_id, provider=provider)

    async with session_maker() as session:
        db_job = await session.get(MessageJob, job_id)
        db_recipient = await session.get(CampaignRecipient, recipient_id)

    live_check.assert_not_called()
    provider.send_template.assert_not_called()
    provider.send.assert_not_called()
    assert db_job.status == "canceled"
    assert db_recipient.followup_status == "skipped_read"


@pytest.mark.asyncio
async def test_live_altegio_guard_cancels_when_future_record_found(
    ow_session,
    session_maker,
    monkeypatch,
) -> None:
    """B. Eligible in DB + Altegio API has future record → canceled, skipped_future_record."""
    monkeypatch.setattr(ow, "client_has_any_future_record", AsyncMock(return_value=True))

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id, status="delivered")
            await session.flush()
            recipient_id = recipient.id

            job = _make_followup_job(session, run_id=run.id, recipient_id=recipient.id, client_id=client.id)
            await session.flush()
            recipient.followup_message_job_id = job.id
            job_id = job.id

    provider = MagicMock()
    provider.send_template = AsyncMock()
    provider.send = AsyncMock()

    await _lock_job(session_maker, job_id)

    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, job_id, provider=provider)

    async with session_maker() as session:
        db_job = await session.get(MessageJob, job_id)
        db_recipient = await session.get(CampaignRecipient, recipient_id)

    assert db_job.status == "canceled"
    assert db_job.last_error is not None
    assert "future Altegio record" in db_job.last_error
    assert db_recipient.followup_status == "skipped_future_record"
    assert db_recipient.followup_message_job_id is None
    provider.send_template.assert_not_called()
    provider.send.assert_not_called()


@pytest.mark.asyncio
async def test_live_altegio_guard_passes_when_no_future_record(
    ow_session,
    session_maker,
    monkeypatch,
) -> None:
    """C. Eligible in DB + no future Altegio record → send path reached (provider called)."""
    monkeypatch.setattr(ow, "client_has_any_future_record", AsyncMock(return_value=False))
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "text")
    monkeypatch.setattr(ow.settings, "wa_131026_suppression_enabled", False)
    monkeypatch.setattr(ow, "_apply_rate_limit", AsyncMock(return_value=None))

    send_mock = AsyncMock(return_value=("wamid.live-guard-c-001", None))
    monkeypatch.setattr(ow, "safe_send", send_mock)

    async def _fake_render(session, *, company_id, template_code, record, client):
        return ("Hallo Test!", None, "de", {"client_name": "Test"})

    monkeypatch.setattr(ow, "_render_message", _fake_render)

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id, status="delivered")
            await session.flush()

            job = _make_followup_job(session, run_id=run.id, recipient_id=recipient.id, client_id=client.id)
            await session.flush()
            job_id = job.id

    await _lock_job(session_maker, job_id)

    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, job_id, provider=MagicMock())

    async with session_maker() as session:
        db_job = await session.get(MessageJob, job_id)

    assert "future Altegio record" not in (db_job.last_error or "")
    send_mock.assert_awaited()


@pytest.mark.asyncio
async def test_live_altegio_guard_retries_on_api_error(
    ow_session,
    session_maker,
    monkeypatch,
) -> None:
    """D. Altegio API error → job requeued with 1-minute delay on first failure."""
    import httpx

    async def _api_error(*_a, **_kw):
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(ow, "client_has_any_future_record", _api_error)
    monkeypatch.setattr(ow, "utcnow", lambda: NOW)

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id, status="delivered")
            await session.flush()

            job = _make_followup_job(session, run_id=run.id, recipient_id=recipient.id, client_id=client.id)
            await session.flush()
            job_id = job.id

    await _lock_job(session_maker, job_id)

    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, job_id, provider=MagicMock())

    async with session_maker() as session:
        db_job = await session.get(MessageJob, job_id)

    assert db_job.status == "queued"
    assert db_job.attempts == 0
    assert (db_job.payload or {}).get(ow._FOLLOWUP_LIVE_GUARD_ATTEMPTS_KEY) == 1
    assert db_job.run_at is not None
    assert abs((db_job.run_at - (NOW + timedelta(seconds=60))).total_seconds()) < 5
    assert "Follow-up delayed" in (db_job.last_error or "")


@pytest.mark.asyncio
async def test_live_altegio_guard_fails_after_max_attempts(
    ow_session,
    session_maker,
    monkeypatch,
) -> None:
    """E. max_attempts exhausted after repeated Altegio errors → job permanently failed."""
    import httpx

    async def _api_error(*_a, **_kw):
        raise httpx.ConnectError("network down")

    monkeypatch.setattr(ow, "client_has_any_future_record", _api_error)

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id, status="delivered")
            await session.flush()

            pre_attempts = ow.MAX_FOLLOWUP_LIVE_GUARD_ATTEMPTS - 1
            job = _make_followup_job(session, run_id=run.id, recipient_id=recipient.id, client_id=client.id)
            job.payload = {**job.payload, ow._FOLLOWUP_LIVE_GUARD_ATTEMPTS_KEY: pre_attempts}
            await session.flush()
            job_id = job.id

    await _lock_job(session_maker, job_id)

    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, job_id, provider=MagicMock())

    async with session_maker() as session:
        db_job = await session.get(MessageJob, job_id)

    assert db_job.status == "failed"
    assert db_job.attempts == 0
    assert (db_job.payload or {}).get(ow._FOLLOWUP_LIVE_GUARD_ATTEMPTS_KEY) == ow.MAX_FOLLOWUP_LIVE_GUARD_ATTEMPTS
    assert "max attempts" in (db_job.last_error or "").lower()


@pytest.mark.asyncio
async def test_live_altegio_guard_not_called_for_non_followup_job(
    ow_session,
    session_maker,
    monkeypatch,
) -> None:
    """F. Non-followup job type → live Altegio check not called."""
    from altegio_bot.models.models import Record as _Record

    live_check = AsyncMock()
    monkeypatch.setattr(ow, "client_has_any_future_record", live_check)
    monkeypatch.setattr(ow, "client_has_future_appointments", AsyncMock(return_value=False))
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "text")
    monkeypatch.setattr(ow.settings, "wa_131026_suppression_enabled", False)
    monkeypatch.setattr(ow, "_apply_rate_limit", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "safe_send", AsyncMock(return_value=("wamid.f-001", None)))

    async def _fake_render(session, *, company_id, template_code, record, client):
        return ("msg", None, "de", {})

    monkeypatch.setattr(ow, "_render_message", _fake_render)

    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=99001,
                phone_e164=PHONE,
                display_name="Test",
                raw={},
                wa_opted_out=False,
            )
            session.add(client)
            await session.flush()

            record = _Record(
                company_id=COMPANY,
                altegio_record_id=5001,
                client_id=client.id,
                altegio_client_id=99001,
                starts_at=NOW - timedelta(days=10),
                attendance=1,
                visit_attendance=0,
                is_deleted=False,
                raw={},
            )
            session.add(record)
            await session.flush()

            job = MessageJob(
                company_id=COMPANY,
                client_id=client.id,
                record_id=record.id,
                job_type="repeat_10d",
                run_at=NOW,
                status="queued",
                attempts=0,
                max_attempts=5,
                dedupe_key="live-guard-f-repeat10d-001",
                payload={},
            )
            session.add(job)
            await session.flush()
            job_id = job.id

    await _lock_job(session_maker, job_id)

    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, job_id, provider=MagicMock())

    live_check.assert_not_called()


@pytest.mark.asyncio
async def test_live_altegio_guard_not_called_when_db_guard_fires_booked(
    ow_session,
    session_maker,
    monkeypatch,
) -> None:
    """G. DB guard fires (recipient booked_after_at set) → live Altegio check not called."""
    live_check = AsyncMock()
    monkeypatch.setattr(ow, "client_has_any_future_record", live_check)

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(
                session,
                run.id,
                client.id,
                status="booked_after_campaign",
                booked_after_at=NOW - timedelta(days=3),
            )
            await session.flush()
            recipient_id = recipient.id

            job = _make_followup_job(session, run_id=run.id, recipient_id=recipient.id, client_id=client.id)
            await session.flush()
            job_id = job.id

    provider = MagicMock()
    provider.send_template = AsyncMock()
    provider.send = AsyncMock()

    await _lock_job(session_maker, job_id)

    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, job_id, provider=provider)

    async with session_maker() as session:
        db_job = await session.get(MessageJob, job_id)
        db_recipient = await session.get(CampaignRecipient, recipient_id)

    live_check.assert_not_called()
    provider.send_template.assert_not_called()
    provider.send.assert_not_called()
    assert db_job.status == "canceled"
    assert db_recipient.followup_status == "skipped_booked_after"


# ---------------------------------------------------------------------------
# H. Live guard fails closed when altegio_client_id is missing from both
#    CampaignRecipient and Client (fail-closed safety fix)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_live_altegio_guard_fails_closed_without_altegio_client_id(
    ow_session,
    session_maker,
    monkeypatch,
) -> None:
    """H. No altegio_client_id on recipient or client → job fails permanently (fail-closed)."""
    live_check = AsyncMock()
    monkeypatch.setattr(ow, "client_has_any_future_record", live_check)

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            # CRM-only: no client row, so _load_client returns None
            recipient = _make_recipient(session, run.id, client_id=None, status="delivered")
            await session.flush()
            # Override the helper's default of 99001 — make it truly None
            recipient.altegio_client_id = None
            await session.flush()
            recipient_id = recipient.id

            job = _make_followup_job(session, run_id=run.id, recipient_id=recipient.id, client_id=None)
            await session.flush()
            job_id = job.id
            # Simulate the link that execute_followup sets in production.
            recipient.followup_message_job_id = job_id

    provider = MagicMock()
    provider.send_template = AsyncMock()
    provider.send = AsyncMock()

    await _lock_job(session_maker, job_id)

    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, job_id, provider=provider)

    async with session_maker() as session:
        db_job = await session.get(MessageJob, job_id)
        db_recipient = await session.get(CampaignRecipient, recipient_id)

    live_check.assert_not_called()
    provider.send_template.assert_not_called()
    provider.send.assert_not_called()
    assert db_job.status == "failed"
    assert db_job.last_error is not None
    assert "missing Altegio client id" in db_job.last_error
    assert db_recipient.followup_status == "followup_failed"
    assert db_recipient.followup_message_job_id == job_id


# ---------------------------------------------------------------------------
# I. Live guard falls back to client.altegio_client_id when recipient has None
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_live_altegio_guard_uses_client_altegio_id_fallback(
    ow_session,
    session_maker,
    monkeypatch,
) -> None:
    """I. recipient.altegio_client_id=None + client.altegio_client_id=99001 → guard uses client id."""
    captured_calls: list[int] = []

    async def _live_check(*, company_id: int, altegio_client_id: int, **_kw):
        captured_calls.append(altegio_client_id)
        return False  # no future record → send path continues

    monkeypatch.setattr(ow, "client_has_any_future_record", _live_check)
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "text")
    monkeypatch.setattr(ow.settings, "wa_131026_suppression_enabled", False)
    monkeypatch.setattr(ow, "_apply_rate_limit", AsyncMock(return_value=None))

    send_mock = AsyncMock(return_value=("wamid.fallback-i-001", None))
    monkeypatch.setattr(ow, "safe_send", send_mock)

    async def _fake_render(session, *, company_id, template_code, record, client):
        return ("Hallo Test!", None, "de", {"client_name": "Test"})

    monkeypatch.setattr(ow, "_render_message", _fake_render)

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            # _make_client always sets altegio_client_id=99001
            client = _make_client(session)
            await session.flush()
            recipient = _make_recipient(session, run.id, client.id, status="delivered")
            await session.flush()
            # Nullify recipient's altegio_client_id to force fallback to client row
            recipient.altegio_client_id = None
            await session.flush()

            job = _make_followup_job(session, run_id=run.id, recipient_id=recipient.id, client_id=client.id)
            await session.flush()
            job_id = job.id

    await _lock_job(session_maker, job_id)

    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, job_id, provider=MagicMock())

    async with session_maker() as session:
        db_job = await session.get(MessageJob, job_id)

    assert captured_calls == [99001], f"Expected live guard called with 99001, got {captured_calls}"
    send_mock.assert_awaited()
    assert "future Altegio record" not in (db_job.last_error or "")
    assert "missing Altegio client id" not in (db_job.last_error or "")
