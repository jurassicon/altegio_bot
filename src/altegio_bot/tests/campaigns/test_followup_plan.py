"""Integration tests for plan_followup() + execute_followup() eligibility.

Reproduces the Run #23 mix of recipients and asserts that only real
candidates (in sent pipeline, unread, not replied, not booked) are planned
and queued — read/booked recipients must never get a follow-up job.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

import altegio_bot.campaigns.followup as followup_module
from altegio_bot.campaigns.followup import execute_followup, plan_followup
from altegio_bot.campaigns.runner import FOLLOWUP_JOB_TYPE
from altegio_bot.models.models import CampaignRecipient, CampaignRun, MessageJob

COMPANY = 758285
PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 2, 1, tzinfo=timezone.utc)
TEMPLATE = "kitilash_ka_newsletter_new_clients_followup_v1"


@pytest.fixture
def patched_followup(session_maker, monkeypatch):
    """execute_followup opens its own sessions via SessionLocal — patch it."""
    monkeypatch.setattr(followup_module, "SessionLocal", session_maker)
    return session_maker


async def _make_run_with_mixed_recipients(session_maker) -> tuple[int, dict[str, int]]:
    """Create a completed send-real run with the Run #23 recipient mix."""
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="send-real",
                company_ids=[COMPANY],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
                followup_enabled=True,
                followup_delay_days=14,
                followup_policy="unread_or_not_booked",
                followup_template_name=TEMPLATE,
                completed_at=now - timedelta(days=15),
                meta={},
            )
            session.add(run)
            await session.flush()
            run_id = run.id

            objs: dict[str, CampaignRecipient] = {}

            def add(label: str, **kw) -> None:
                fields = dict(
                    campaign_run_id=run_id,
                    company_id=COMPANY,
                    client_id=1,
                    phone_e164="+10000000001",
                    followup_status=None,
                    read_at=None,
                    replied_at=None,
                    booked_after_at=None,
                )
                fields.update(kw)
                r = CampaignRecipient(**fields)
                session.add(r)
                objs[label] = r

            # Two delivered recipients are the only real candidates; the rest
            # are skipped (not delivered / read / booked).
            add("delivered", status="delivered")
            add("delivered2", status="delivered")
            add("provider_accepted", status="provider_accepted")
            add("queued", status="queued")
            add("read", status="read", read_at=now)
            add("booked", status="delivered", booked_after_at=now)
            await session.flush()
            ids = {label: obj.id for label, obj in objs.items()}

    return run_id, ids


@pytest.mark.asyncio
async def test_plan_followup_plans_only_real_candidates(patched_followup, session_maker) -> None:
    run_id, ids = await _make_run_with_mixed_recipients(session_maker)

    async with session_maker() as session:
        async with session.begin():
            planned = await plan_followup(session, run_id)

    assert planned == 2, "only the delivered, unread, non-booked recipients are planned"

    async with session_maker() as session:
        recips = {label: await session.get(CampaignRecipient, rid) for label, rid in ids.items()}

    assert recips["delivered"].followup_status == "followup_planned"
    assert recips["delivered2"].followup_status == "followup_planned"
    # Not delivered → not eligible.
    assert recips["provider_accepted"].followup_status == "skipped_not_delivered"
    assert recips["queued"].followup_status == "skipped_not_delivered"
    # Read/booked must NOT be planned, and get specific skip statuses.
    assert recips["read"].followup_status == "skipped_read"
    assert recips["booked"].followup_status == "skipped_booked_after"


@pytest.mark.asyncio
async def test_execute_followup_queues_only_planned(patched_followup, session_maker) -> None:
    run_id, ids = await _make_run_with_mixed_recipients(session_maker)

    async with session_maker() as session:
        async with session.begin():
            await plan_followup(session, run_id)

    stats = await execute_followup(run_id)

    assert stats["queued"] == 2
    assert stats["failed"] == 0

    async with session_maker() as session:
        recips = {label: await session.get(CampaignRecipient, rid) for label, rid in ids.items()}
        # All follow-up jobs must reference only the planned (eligible) recipients.
        jobs = (
            (await session.execute(select(MessageJob).where(MessageJob.job_type == FOLLOWUP_JOB_TYPE))).scalars().all()
        )

    planned_ids = {ids["delivered"], ids["delivered2"]}
    skipped_ids = {ids["provider_accepted"], ids["queued"], ids["read"], ids["booked"]}

    for label in ("delivered", "delivered2"):
        assert recips[label].followup_status == "followup_queued"
        assert recips[label].followup_message_job_id is not None

    assert recips["provider_accepted"].followup_status == "skipped_not_delivered"
    assert recips["queued"].followup_status == "skipped_not_delivered"
    assert recips["read"].followup_status == "skipped_read"
    assert recips["booked"].followup_status == "skipped_booked_after"

    job_recipient_ids = {row.payload.get("campaign_recipient_id") for row in jobs}
    assert job_recipient_ids <= planned_ids, "follow-up jobs only for planned recipients"
    assert job_recipient_ids.isdisjoint(skipped_ids), "no follow-up jobs for non-delivered/read/booked"


@pytest.mark.asyncio
async def test_run23_shape_only_delivered_considered(patched_followup, session_maker) -> None:
    """Run #23 shape: 2 delivered + 5 provider_accepted + 4 queued, all with records.

    Only the delivered recipients are ever planned/queued; provider_accepted and
    queued (original delivery not proven) get skipped_not_delivered and no jobs.
    (Marketing 131026/131049 suppression is verified separately at send time.)
    """
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="send-real",
                company_ids=[COMPANY],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
                followup_enabled=True,
                followup_delay_days=14,
                followup_policy="unread_or_not_booked",
                followup_template_name=TEMPLATE,
                completed_at=now - timedelta(days=15),
                meta={},
            )
            session.add(run)
            await session.flush()
            run_id = run.id

            delivered_ids: list[int] = []
            other_ids: list[int] = []

            def _add(status: str, aid: int) -> CampaignRecipient:
                r = CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=COMPANY,
                    client_id=1,
                    altegio_client_id=aid,
                    phone_e164="+10000000001",
                    status=status,
                    followup_status=None,
                    read_at=None,
                    replied_at=None,
                    booked_after_at=None,
                )
                session.add(r)
                return r

            recs = []
            recs += [("delivered", _add("delivered", 8000)), ("delivered", _add("delivered", 8001))]
            recs += [("pa", _add("provider_accepted", 8100 + i)) for i in range(5)]
            recs += [("q", _add("queued", 8200 + i)) for i in range(4)]
            await session.flush()
            for label, r in recs:
                (delivered_ids if label == "delivered" else other_ids).append(r.id)

    async with session_maker() as session:
        async with session.begin():
            planned = await plan_followup(session, run_id)
    assert planned == 2, "only the 2 delivered recipients are planned"

    stats = await execute_followup(run_id)
    assert stats["queued"] == 2

    async with session_maker() as session:
        delivered = [await session.get(CampaignRecipient, rid) for rid in delivered_ids]
        others = [await session.get(CampaignRecipient, rid) for rid in other_ids]
        jobs = (
            (await session.execute(select(MessageJob).where(MessageJob.job_type == FOLLOWUP_JOB_TYPE))).scalars().all()
        )

    assert all(r.followup_status == "followup_queued" for r in delivered)
    assert all(r.followup_status == "skipped_not_delivered" for r in others)
    job_recipient_ids = {row.payload.get("campaign_recipient_id") for row in jobs}
    assert job_recipient_ids == set(delivered_ids)
    assert job_recipient_ids.isdisjoint(set(other_ids))
