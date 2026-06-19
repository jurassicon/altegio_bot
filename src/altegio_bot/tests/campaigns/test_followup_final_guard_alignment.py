"""Plan/execute must honour the real final guard, not just the local classifier.

Covers the P2 review finding: recipients that pass the cheap local classifier
but fail check_followup_final_eligibility() (current opt-out, post-campaign
booking event, future record) must NOT be planned or queued, and no follow-up
MessageJob may be created for them.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

import altegio_bot.campaigns.followup as followup_module
import altegio_bot.workers.followup_worker as followup_worker_module
from altegio_bot.campaigns.followup import execute_followup, plan_followup
from altegio_bot.campaigns.runner import FOLLOWUP_JOB_TYPE
from altegio_bot.models.models import (
    AltegioEvent,
    CampaignRecipient,
    CampaignRun,
    Client,
    MessageJob,
    Record,
)
from altegio_bot.workers.followup_worker import process_run

COMPANY = 758285
PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 2, 1, tzinfo=timezone.utc)
TEMPLATE = "kitilash_ka_newsletter_new_clients_followup_v1"


@pytest.fixture
def patched_followup(session_maker, monkeypatch):
    """execute_followup / process_run open their own sessions via SessionLocal."""
    monkeypatch.setattr(followup_module, "SessionLocal", session_maker)
    monkeypatch.setattr(followup_worker_module, "SessionLocal", session_maker)
    return session_maker


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def _followup_jobs(session) -> list[MessageJob]:
    return list(
        (await session.execute(select(MessageJob).where(MessageJob.job_type == FOLLOWUP_JOB_TYPE))).scalars().all()
    )


async def _build_run(session_maker, *, completed_days_ago: int = 20) -> int:
    now = _now()
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="send-real",
                company_ids=[COMPANY],
                location_id=1,
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
                followup_enabled=True,
                followup_delay_days=14,
                followup_policy="unread_or_not_booked",
                followup_template_name=TEMPLATE,
                completed_at=now - timedelta(days=completed_days_ago),
                meta={},
            )
            session.add(run)
            await session.flush()
            return run.id


async def _add_client(session, *, altegio_id: int, phone: str, opted_out: bool = False) -> int:
    client = Client(
        company_id=COMPANY,
        altegio_client_id=altegio_id,
        phone_e164=phone,
        display_name=f"Client {altegio_id}",
        raw={},
        wa_opted_out=opted_out,
    )
    session.add(client)
    await session.flush()
    return client.id


def _add_recipient(session, run_id: int, *, client_id: int, altegio_id: int, phone: str, sent_days_ago: int = 20):
    r = CampaignRecipient(
        campaign_run_id=run_id,
        company_id=COMPANY,
        client_id=client_id,
        altegio_client_id=altegio_id,
        phone_e164=phone,
        display_name=f"Client {altegio_id}",
        status="delivered",
        followup_status=None,
        read_at=None,
        replied_at=None,
        booked_after_at=None,
        sent_at=_now() - timedelta(days=sent_days_ago),
        meta={},
    )
    session.add(r)
    return r


# ---------------------------------------------------------------------------
# plan_followup honours final guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_plan_followup_respects_final_guard(patched_followup, session_maker) -> None:
    run_id = await _build_run(session_maker)
    now = _now()

    async with session_maker() as session:
        async with session.begin():
            ok_cid = await _add_client(session, altegio_id=1001, phone="+49100000001")
            opt_cid = await _add_client(session, altegio_id=1002, phone="+49100000002", opted_out=True)
            fut_cid = await _add_client(session, altegio_id=1003, phone="+49100000003")
            evt_cid = await _add_client(session, altegio_id=1004, phone="+49100000004")

            r_ok = _add_recipient(session, run_id, client_id=ok_cid, altegio_id=1001, phone="+49100000001")
            r_opt = _add_recipient(session, run_id, client_id=opt_cid, altegio_id=1002, phone="+49100000002")
            r_fut = _add_recipient(session, run_id, client_id=fut_cid, altegio_id=1003, phone="+49100000003")
            r_evt = _add_recipient(session, run_id, client_id=evt_cid, altegio_id=1004, phone="+49100000004")
            await session.flush()
            ids = {"ok": r_ok.id, "opt": r_opt.id, "fut": r_fut.id, "evt": r_evt.id}

            # Future active record for r_fut.
            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=7003,
                    client_id=fut_cid,
                    altegio_client_id=1003,
                    starts_at=now + timedelta(days=365),
                    is_deleted=False,
                    raw={},
                )
            )
            # Post-campaign record-create event for r_evt (record in the past +
            # an AltegioEvent create after the campaign completed).
            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=7004,
                    client_id=evt_cid,
                    altegio_client_id=1004,
                    starts_at=now - timedelta(days=3),
                    is_deleted=False,
                    raw={},
                )
            )
            session.add(
                AltegioEvent(
                    dedupe_key="evt-align-7004",
                    company_id=COMPANY,
                    resource="record",
                    event_status="create",
                    resource_id=7004,
                    received_at=now - timedelta(days=5),
                    query={},
                    headers={},
                    payload={},
                )
            )

    async with session_maker() as session:
        async with session.begin():
            planned = await plan_followup(session, run_id)

    assert planned == 1, "only the clean recipient passes the final guard"

    async with session_maker() as session:
        recips = {k: await session.get(CampaignRecipient, rid) for k, rid in ids.items()}
        jobs = await _followup_jobs(session)

    assert recips["ok"].followup_status == "followup_planned"
    assert recips["opt"].followup_status == "skipped_opted_out"
    assert recips["fut"].followup_status == "skipped_future_record"
    assert recips["evt"].followup_status == "skipped_booked_after"
    # booked_after_at backfilled from the event timestamp.
    assert recips["evt"].booked_after_at is not None
    # plan_followup never creates jobs.
    assert jobs == []


# ---------------------------------------------------------------------------
# execute_followup honours final guard for state that changed after planning
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_followup_skips_optout_appearing_after_plan(patched_followup, session_maker) -> None:
    run_id = await _build_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            cid = await _add_client(session, altegio_id=2001, phone="+49200000001")
            r = _add_recipient(session, run_id, client_id=cid, altegio_id=2001, phone="+49200000001")
            await session.flush()
            recipient_id, client_id = r.id, cid

    async with session_maker() as session:
        async with session.begin():
            planned = await plan_followup(session, run_id)
    assert planned == 1

    # Client opts out AFTER planning, before execute.
    async with session_maker() as session:
        async with session.begin():
            client = await session.get(Client, client_id)
            client.wa_opted_out = True

    stats = await execute_followup(run_id)

    assert stats["queued"] == 0
    assert stats["skipped"] == 1

    async with session_maker() as session:
        r = await session.get(CampaignRecipient, recipient_id)
        jobs = await _followup_jobs(session)

    assert r.followup_status == "skipped_opted_out"
    assert r.followup_message_job_id is None
    assert jobs == []


@pytest.mark.asyncio
async def test_execute_followup_skips_future_record_appearing_after_plan(patched_followup, session_maker) -> None:
    run_id = await _build_run(session_maker)
    now = _now()

    async with session_maker() as session:
        async with session.begin():
            cid = await _add_client(session, altegio_id=2002, phone="+49200000002")
            r = _add_recipient(session, run_id, client_id=cid, altegio_id=2002, phone="+49200000002")
            await session.flush()
            recipient_id = r.id

    async with session_maker() as session:
        async with session.begin():
            planned = await plan_followup(session, run_id)
    assert planned == 1

    # Future record appears AFTER planning, before execute.
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=9002,
                    client_id=cid,
                    altegio_client_id=2002,
                    starts_at=now + timedelta(days=365),
                    is_deleted=False,
                    raw={},
                )
            )

    stats = await execute_followup(run_id)

    assert stats["queued"] == 0
    assert stats["skipped"] == 1

    async with session_maker() as session:
        r = await session.get(CampaignRecipient, recipient_id)
        jobs = await _followup_jobs(session)

    assert r.followup_status == "skipped_future_record"
    assert jobs == []


# ---------------------------------------------------------------------------
# Worker meta counts reflect plan-time guard skips
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worker_meta_counts_include_guard_skips(patched_followup, session_maker) -> None:
    run_id = await _build_run(session_maker)
    now = _now()

    async with session_maker() as session:
        async with session.begin():
            ok_cid = await _add_client(session, altegio_id=3001, phone="+49300000001")
            opt_cid = await _add_client(session, altegio_id=3002, phone="+49300000002", opted_out=True)
            fut_cid = await _add_client(session, altegio_id=3003, phone="+49300000003")
            evt_cid = await _add_client(session, altegio_id=3004, phone="+49300000004")

            r_ok = _add_recipient(session, run_id, client_id=ok_cid, altegio_id=3001, phone="+49300000001")
            _add_recipient(session, run_id, client_id=opt_cid, altegio_id=3002, phone="+49300000002")
            _add_recipient(session, run_id, client_id=fut_cid, altegio_id=3003, phone="+49300000003")
            _add_recipient(session, run_id, client_id=evt_cid, altegio_id=3004, phone="+49300000004")
            await session.flush()
            ok_id = r_ok.id

            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=8003,
                    client_id=fut_cid,
                    altegio_client_id=3003,
                    starts_at=now + timedelta(days=365),
                    is_deleted=False,
                    raw={},
                )
            )
            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=8004,
                    client_id=evt_cid,
                    altegio_client_id=3004,
                    starts_at=now - timedelta(days=3),
                    is_deleted=False,
                    raw={},
                )
            )
            session.add(
                AltegioEvent(
                    dedupe_key="evt-align-8004",
                    company_id=COMPANY,
                    resource="record",
                    event_status="create",
                    resource_id=8004,
                    received_at=now - timedelta(days=5),
                    query={},
                    headers={},
                    payload={},
                )
            )

    await process_run(run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        r_ok = await session.get(CampaignRecipient, ok_id)
        jobs = await _followup_jobs(session)

    meta = run.meta or {}
    assert meta["followup_auto_status"] == "completed"
    assert meta["followup_auto_planned_count"] == 1
    assert meta["followup_auto_queued_count"] == 1
    assert meta["followup_auto_failed_count"] == 0
    # The 3 guard-skipped recipients are counted as skipped.
    assert meta["followup_auto_skipped_count"] == 3

    # Exactly one job, for the clean recipient.
    assert len(jobs) == 1
    assert jobs[0].payload.get("campaign_recipient_id") == ok_id
    assert r_ok.followup_status == "followup_queued"
