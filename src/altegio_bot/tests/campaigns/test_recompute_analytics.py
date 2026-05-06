"""Tests for recompute analytics: read_at sync, booked-after attribution,
follow-up eligibility, loyalty cleanup smoke test, and recompute endpoint.

All DB tests use the shared `session_maker` fixture from conftest.py.
Fake loyalty client is used instead of real Altegio API.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

import altegio_bot.ops.campaigns_api as campaigns_api_module
import altegio_bot.ops.router as ops_router_module
from altegio_bot.campaigns.followup import _is_eligible_for_followup
from altegio_bot.campaigns.loyalty_cleanup import (
    cleanup_campaign_cards,
    resolve_or_issue_loyalty_card,
)
from altegio_bot.campaigns.runner import (
    recompute_campaign_run_stats,
)
from altegio_bot.main import app
from altegio_bot.models.models import (
    AltegioEvent,
    CampaignRecipient,
    CampaignRun,
    MessageJob,
    OutboxMessage,
    Record,
    RecordService,
)
from altegio_bot.ops.auth import require_ops_auth
from altegio_bot.service_filter import _LRU_CACHE, ServiceLookupError, _cache_put

_UTC = timezone.utc
_NOW = datetime(2026, 5, 4, 12, 0, 0, tzinfo=_UTC)
_COMPLETED_AT = datetime(2026, 4, 16, 11, 47, 46, tzinfo=_UTC)

# ---------------------------------------------------------------------------
# Lash service test constants — pre-populated into the process-local LRU
# cache so no Altegio API calls happen during tests.
# 10707687 is in LASH_CATEGORY_IDS_BY_COMPANY[758285].
# ---------------------------------------------------------------------------
_LASH_SVC_ID = 42001
_NON_LASH_SVC_ID = 42002
_LASH_CATEGORY_ID = 10707687  # valid lash category for company 758285
_ERROR_SVC_ID = 42999  # used for ServiceLookupError tests; never pre-cached
_UNCACHED_SVC_ID = 43001  # cache-miss with no API tokens → strict lookup failure (P3)


def _seed_lash_cache() -> None:
    """Pre-populate LRU cache so no Altegio API calls happen in booked-after tests."""
    _cache_put((758285, _LASH_SVC_ID), _LASH_CATEGORY_ID)
    _cache_put((758285, _NON_LASH_SVC_ID), 99999)  # non-lash category


# ---------------------------------------------------------------------------
# Fake Altegio loyalty client (no real API calls)
# ---------------------------------------------------------------------------


class _FakeLoyaltyClient:
    def __init__(self) -> None:
        self.deleted_card_ids: list[int] = []
        self.issued_cards: list[dict[str, Any]] = []
        self._next_id = 9900

    async def delete_card(self, location_id: int, card_id: int) -> None:
        self.deleted_card_ids.append(card_id)

    async def issue_card(
        self,
        location_id: int,
        *,
        loyalty_card_number: str,
        loyalty_card_type_id: str,
        phone: int,
    ) -> dict[str, Any]:
        card: dict[str, Any] = {
            "id": str(self._next_id),
            "loyalty_card_number": loyalty_card_number,
            "loyalty_card_type_id": loyalty_card_type_id,
        }
        self._next_id += 1
        self.issued_cards.append(card)
        return card

    async def aclose(self) -> None:
        pass


# ---------------------------------------------------------------------------
# HTTP client fixture (same pattern as test_ops_campaigns.py)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def http_client(session_maker, monkeypatch) -> AsyncClient:
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    monkeypatch.setattr(campaigns_api_module, "SessionLocal", session_maker)
    monkeypatch.setitem(app.dependency_overrides, require_ops_auth, lambda: None)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# Helper: create a minimal send-real run with completed_at set
# ---------------------------------------------------------------------------


async def _make_run(session_maker, *, completed_at=_COMPLETED_AT, attribution_window_days: int = 30) -> int:
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="send-real",
                company_ids=[758285],
                period_start=_COMPLETED_AT.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
                period_end=_COMPLETED_AT,
                status="completed",
                completed_at=completed_at,
                attribution_window_days=attribution_window_days,
                total_clients_seen=1,
                candidates_count=1,
                queued_count=0,
                sent_count=0,
                provider_accepted_count=0,
                delivered_count=0,
                read_count=0,
                booked_after_count=0,
                cards_issued_count=0,
                cards_deleted_count=0,
                cleanup_failed_count=0,
                followup_enabled=False,
                meta={},
            )
            session.add(run)
            await session.flush()
            return run.id


# ---------------------------------------------------------------------------
# 1. recompute sets read_at from outbox meta timestamp
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recompute_sets_read_at_from_outbox_meta(session_maker) -> None:
    """recompute fills recipient.read_at from outbox meta wa_status_read timestamp."""
    wa_ts = 1745000000  # Unix epoch
    expected_read_at = datetime.fromtimestamp(wa_ts, tz=_UTC)

    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            outbox = OutboxMessage(
                company_id=758285,
                phone_e164="+4915199990001",
                template_code="newsletter_new_clients_monthly",
                language="de",
                body="Test",
                status="read",
                scheduled_at=_COMPLETED_AT,
                meta={"wa_status_read": {"timestamp": str(wa_ts)}},
            )
            session.add(outbox)
            await session.flush()
            outbox_id = outbox.id

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                phone_e164="+4915199990001",
                display_name="Read Client",
                status="queued",
                outbox_message_id=outbox_id,
                sent_at=_COMPLETED_AT,
                read_at=None,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.read_at is not None
    assert recip.read_at == expected_read_at
    assert run.read_count == 1


# ---------------------------------------------------------------------------
# 2. recompute counts delivered when outbox.status='delivered'
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recompute_counts_delivered(session_maker) -> None:
    """delivered_count == 1 when outbox.status='delivered'."""
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            outbox = OutboxMessage(
                company_id=758285,
                phone_e164="+4915199990002",
                template_code="newsletter_new_clients_monthly",
                language="de",
                body="Test",
                status="delivered",
                scheduled_at=_COMPLETED_AT,
                meta={},
            )
            session.add(outbox)
            await session.flush()

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    phone_e164="+4915199990002",
                    display_name="Delivered Client",
                    status="queued",
                    outbox_message_id=outbox.id,
                    sent_at=_COMPLETED_AT,
                )
            )

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run.delivered_count == 1
    assert run.read_count == 0


# ---------------------------------------------------------------------------
# 3. recompute counts read as both read_count and delivered_count
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recompute_counts_read_also_as_delivered(session_maker) -> None:
    """read counts in both read_count and delivered_count (cumulative funnel)."""
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            outbox = OutboxMessage(
                company_id=758285,
                phone_e164="+4915199990003",
                template_code="newsletter_new_clients_monthly",
                language="de",
                body="Test",
                status="read",
                scheduled_at=_COMPLETED_AT,
                meta={"wa_status_read": {"timestamp": "1745000000"}},
            )
            session.add(outbox)
            await session.flush()

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    phone_e164="+4915199990003",
                    display_name="Read Client 2",
                    status="queued",
                    outbox_message_id=outbox.id,
                    sent_at=_COMPLETED_AT,
                )
            )

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run.read_count == 1
    assert run.delivered_count == 1, "read status implies delivered (cumulative funnel)"


# ---------------------------------------------------------------------------
# 4. recompute sets booked_after_at from altegio_events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recompute_sets_booked_after_at_from_altegio_events(session_maker) -> None:
    """booked_after_at is set when there is a lash record/create event in attribution window."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker, attribution_window_days=30)
    book_event_at = _COMPLETED_AT + timedelta(days=18)  # inside 30-day window

    async with session_maker() as session:
        async with session.begin():
            # client_id=1 exists in conftest
            record = Record(
                company_id=758285,
                altegio_record_id=9001,
                client_id=1,
                starts_at=_COMPLETED_AT + timedelta(days=60),  # far future
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            ae = AltegioEvent(
                dedupe_key="test-booked-after-001",
                company_id=758285,
                resource="record",
                resource_id=9001,
                event_status="create",
                received_at=book_event_at,
            )
            session.add(ae)

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=1,
                phone_e164="+10000000001",
                display_name="Özelm",
                status="provider_accepted",
                sent_at=_COMPLETED_AT,
                booked_after_at=None,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at == book_event_at
    assert run.booked_after_count == 1


# ---------------------------------------------------------------------------
# 5. booked-after uses create event received_at, not appointment starts_at
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_booked_after_uses_event_received_at_not_starts_at(session_maker) -> None:
    """booked_after_at reflects when booking was made, not when appointment starts."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker, attribution_window_days=30)
    event_received_at = _COMPLETED_AT + timedelta(days=18)  # inside window
    appointment_starts_at = _COMPLETED_AT + timedelta(days=90)  # far outside window

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=9002,
                client_id=1,
                starts_at=appointment_starts_at,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-booked-after-002",
                    company_id=758285,
                    resource="record",
                    resource_id=9002,
                    event_status="create",
                    received_at=event_received_at,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=1,
                phone_e164="+10000000001",
                display_name="Özelm",
                status="provider_accepted",
                sent_at=_COMPLETED_AT,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at == event_received_at


# ---------------------------------------------------------------------------
# 6. create event before campaign completed_at is ignored
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_booked_after_ignores_events_before_campaign_completed(session_maker) -> None:
    """AltegioEvent received before run.completed_at does not count as booked-after."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    event_before = _COMPLETED_AT - timedelta(hours=1)  # before campaign ended

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=9003,
                client_id=1,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-booked-after-003",
                    company_id=758285,
                    resource="record",
                    resource_id=9003,
                    event_status="create",
                    received_at=event_before,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=1,
                phone_e164="+10000000001",
                display_name="Client Early",
                status="provider_accepted",
                sent_at=_COMPLETED_AT,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at is None
    assert run.booked_after_count == 0


# ---------------------------------------------------------------------------
# 7. create event outside attribution window is ignored
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_booked_after_ignores_events_outside_attribution_window(session_maker) -> None:
    """AltegioEvent received after attribution window end does not count."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker, attribution_window_days=30)
    event_too_late = _COMPLETED_AT + timedelta(days=31)  # 1 day past window

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=9004,
                client_id=1,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-booked-after-004",
                    company_id=758285,
                    resource="record",
                    resource_id=9004,
                    event_status="create",
                    received_at=event_too_late,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=1,
                phone_e164="+10000000001",
                display_name="Client Late",
                status="provider_accepted",
                sent_at=_COMPLETED_AT,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at is None


# ---------------------------------------------------------------------------
# 8. provider_accepted recipient (sent_at only) is eligible for booked-after
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_provider_accepted_recipient_eligible_for_booked_after(session_maker) -> None:
    """Recipient with only sent_at (no delivered/read) qualifies for booked-after."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=1)

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=9005,
                client_id=1,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-booked-after-005",
                    company_id=758285,
                    resource="record",
                    resource_id=9005,
                    event_status="create",
                    received_at=event_at,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=1,
                phone_e164="+10000000001",
                display_name="Provider Accepted Only",
                status="provider_accepted",
                # Only sent_at — no outbox_message_id, no delivered/read
                sent_at=_COMPLETED_AT,
                outbox_message_id=None,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at == event_at


# ---------------------------------------------------------------------------
# 9. missing altegio booking events does not crash recompute
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_booking_events_does_not_crash_recompute(session_maker) -> None:
    """recompute handles zero AltegioEvent rows gracefully."""
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=1,
                    phone_e164="+10000000001",
                    display_name="No Event Client",
                    status="provider_accepted",
                    sent_at=_COMPLETED_AT,
                )
            )

    summary = None
    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    assert summary is not None
    assert summary["booked_after_count"] == 0


# ---------------------------------------------------------------------------
# 10. recompute does not re-enable canceled follow-up jobs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recompute_does_not_reenable_canceled_followup_jobs(session_maker) -> None:
    """Canceled follow-up jobs remain unchanged after recompute."""
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            job = MessageJob(
                company_id=758285,
                job_type="newsletter_new_clients_followup",
                run_at=_COMPLETED_AT,
                status="canceled",
                dedupe_key="test-canceled-followup-001",
                payload={"campaign_run_id": run_id},
            )
            session.add(job)
            await session.flush()
            job_id = job.id

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=1,
                phone_e164="+10000000001",
                display_name="Followup Client",
                status="provider_accepted",
                sent_at=_COMPLETED_AT,
                followup_status="followup_queued",
                followup_message_job_id=job_id,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)
        job_after = await session.get(MessageJob, job_id)

    assert job_after.status == "canceled", "recompute must not re-enable canceled jobs"
    assert recip.followup_status == "followup_queued", "recompute must not modify followup_status"
    assert recip.followup_message_job_id == job_id, "recompute must not clear followup_message_job_id"


# ---------------------------------------------------------------------------
# 11. follow-up eligibility respects read_at and booked_after_at
# ---------------------------------------------------------------------------


def _make_recipient(**kw: Any) -> CampaignRecipient:
    defaults: dict[str, Any] = dict(
        campaign_run_id=1,
        company_id=758285,
        status="provider_accepted",
        excluded_reason=None,
        read_at=None,
        booked_after_at=None,
        followup_status=None,
    )
    defaults.update(kw)
    return CampaignRecipient(**defaults)


def test_followup_unread_only_excludes_recipient_with_read_at() -> None:
    """unread_only: recipient with read_at is NOT eligible."""
    r = _make_recipient(status="read", read_at=_NOW)
    assert _is_eligible_for_followup(r, "unread_only") is False


def test_followup_unread_only_includes_recipient_without_read_at() -> None:
    """unread_only: recipient without read_at IS eligible."""
    r = _make_recipient(status="provider_accepted", read_at=None)
    assert _is_eligible_for_followup(r, "unread_only") is True


def test_followup_unread_or_not_booked_excludes_when_both_read_and_booked() -> None:
    """unread_or_not_booked: read AND booked → NOT eligible."""
    r = _make_recipient(status="booked_after_campaign", read_at=_NOW, booked_after_at=_NOW)
    assert _is_eligible_for_followup(r, "unread_or_not_booked") is False


def test_followup_unread_or_not_booked_includes_when_not_read() -> None:
    """unread_or_not_booked: not read (even if booked) → eligible."""
    r = _make_recipient(status="provider_accepted", read_at=None, booked_after_at=_NOW)
    assert _is_eligible_for_followup(r, "unread_or_not_booked") is True


def test_followup_unread_or_not_booked_includes_when_read_but_not_booked() -> None:
    """unread_or_not_booked: read but not booked → eligible."""
    r = _make_recipient(status="read", read_at=_NOW, booked_after_at=None)
    assert _is_eligible_for_followup(r, "unread_or_not_booked") is True


# ---------------------------------------------------------------------------
# 12. loyalty cleanup smoke test (fake Altegio client)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_loyalty_cleanup_deletes_old_card_and_issues_new(session_maker) -> None:
    """cleanup_campaign_cards deletes old card; resolve_or_issue issues new card for fresh client."""
    old_card_id = "47719627"
    phone_with_history = "+10000000001"  # client_id=1 (has old card)
    phone_fresh = "+10000000010"  # client_id=10 (no prior card)
    campaign_code = "new_clients_monthly"
    location_id = 100
    card_type_id = "46454"
    company_id = 758285

    # Previous run: client 1 has a loyalty card from a prior campaign run
    async with session_maker() as session:
        async with session.begin():
            old_run = CampaignRun(
                campaign_code=campaign_code,
                mode="send-real",
                company_ids=[company_id],
                period_start=_COMPLETED_AT.replace(month=3, day=1),
                period_end=_COMPLETED_AT.replace(month=3, day=31),
                status="completed",
                completed_at=_COMPLETED_AT.replace(month=3, day=31),
                meta={},
            )
            session.add(old_run)
            await session.flush()

            session.add(
                CampaignRecipient(
                    campaign_run_id=old_run.id,
                    company_id=company_id,
                    client_id=1,
                    phone_e164=phone_with_history,
                    display_name="Cleanup Client",
                    status="read",
                    loyalty_card_id=old_card_id,
                    loyalty_card_number="0010000000001",
                    loyalty_card_type_id=card_type_id,
                    cleanup_card_ids=[],
                    sent_at=_COMPLETED_AT.replace(month=3),
                )
            )

    fake_loyalty = _FakeLoyaltyClient()

    # Part A: cleanup finds and deletes the old campaign card via Altegio API
    async with session_maker() as session:
        cleanup_result = await cleanup_campaign_cards(
            session,
            fake_loyalty,  # type: ignore[arg-type]
            location_id=location_id,
            client_id=1,
            campaign_code=campaign_code,
        )

    assert cleanup_result.ok is True
    assert old_card_id in cleanup_result.deleted_ids
    assert int(old_card_id) in fake_loyalty.deleted_card_ids, "delete_card must be called with old card id"

    # Part B: after marking cleanup_card_ids on the old recipient, find_campaign_card_ids
    # returns [] — idempotent protection (no double-delete on next recompute).
    from altegio_bot.campaigns.loyalty_cleanup import find_campaign_card_ids

    async with session_maker() as session:
        async with session.begin():
            stmt = select(CampaignRecipient).where(CampaignRecipient.loyalty_card_id == old_card_id)
            old_recip = (await session.execute(stmt)).scalars().first()
            assert old_recip is not None
            old_recip.cleanup_card_ids = [old_card_id]

    async with session_maker() as session:
        pending = await find_campaign_card_ids(
            session,
            client_id=1,
            campaign_code=campaign_code,
        )
    assert pending == [], "already-deleted card must not appear in pending list"

    # Part C: resolve_or_issue for a fresh client (no prior cards) → issued_new
    async with session_maker() as session:
        resolution = await resolve_or_issue_loyalty_card(
            session,
            fake_loyalty,  # type: ignore[arg-type]
            phone_e164=phone_fresh,
            location_id=location_id,
            card_type_id=card_type_id,
            campaign_code=campaign_code,
            company_id=company_id,
        )

    assert resolution.outcome == "issued_new"
    assert len(fake_loyalty.issued_cards) == 1
    issued = fake_loyalty.issued_cards[0]
    assert issued["loyalty_card_type_id"] == card_type_id
    assert resolution.loyalty_card_id != ""


# ---------------------------------------------------------------------------
# 13. recompute endpoint returns summary with read_count and booked_after_count
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recompute_endpoint_returns_analytics_stats(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """POST /runs/{run_id}/recompute returns JSON with read_count and booked_after_count."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            outbox = OutboxMessage(
                company_id=758285,
                phone_e164="+4915199990013",
                template_code="newsletter_new_clients_monthly",
                language="de",
                body="Test",
                status="read",
                scheduled_at=_COMPLETED_AT,
                meta={"wa_status_read": {"timestamp": "1745000000"}},
            )
            session.add(outbox)
            await session.flush()

            record = Record(
                company_id=758285,
                altegio_record_id=9013,
                client_id=1,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-recompute-endpoint-013",
                    company_id=758285,
                    resource="record",
                    resource_id=9013,
                    event_status="create",
                    received_at=_COMPLETED_AT + timedelta(days=18),
                )
            )

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=1,
                    phone_e164="+4915199990013",
                    display_name="Endpoint Client",
                    status="queued",
                    outbox_message_id=outbox.id,
                    sent_at=_COMPLETED_AT,
                )
            )

    response = await http_client.post(f"/ops/campaigns/runs/{run_id}/recompute")
    assert response.status_code == 200

    data = response.json()
    assert data["recomputed"] is True
    assert data["run_id"] == run_id

    stats = data["stats"]
    assert "read_count" in stats
    assert "booked_after_count" in stats
    assert stats["read_count"] == 1
    assert stats["booked_after_count"] == 1


# ---------------------------------------------------------------------------
# Blocker 1: per-recipient attribution window (sent_at, not completed_at)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_booked_after_null_when_event_between_completed_and_sent(session_maker) -> None:
    """Event after run.completed_at but before recipient.sent_at → booked_after_at=None.

    Lash service present so NULL is proven to come from timing, not missing service.
    """
    _seed_lash_cache()
    run_id = await _make_run(session_maker)

    # sent_at is 5 seconds after completed_at
    sent_at = _COMPLETED_AT + timedelta(seconds=5)
    # event is 2 seconds after completed_at — before sent_at
    event_at = _COMPLETED_AT + timedelta(seconds=2)

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=10001,
                client_id=1,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-per-recip-window-001",
                    company_id=758285,
                    resource="record",
                    resource_id=10001,
                    event_status="create",
                    received_at=event_at,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=1,
                phone_e164="+10000000001",
                display_name="Per-Window Client",
                status="provider_accepted",
                sent_at=sent_at,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at is None, "event before sent_at must not count as booked-after"
    assert run.booked_after_count == 0


@pytest.mark.asyncio
async def test_booked_after_set_when_event_after_sent_at(session_maker) -> None:
    """Event 1 second after recipient.sent_at → booked_after_at is set."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker)

    sent_at = _COMPLETED_AT + timedelta(seconds=5)
    event_at = _COMPLETED_AT + timedelta(seconds=6)  # 1s after sent_at

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=10002,
                client_id=1,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-per-recip-window-002",
                    company_id=758285,
                    resource="record",
                    resource_id=10002,
                    event_status="create",
                    received_at=event_at,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=1,
                phone_e164="+10000000001",
                display_name="Per-Window Client 2",
                status="provider_accepted",
                sent_at=sent_at,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at == event_at


# ---------------------------------------------------------------------------
# Blocker 2: positive send signal required
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_booked_after_null_for_job_only_recipient(session_maker) -> None:
    """Recipient with only message_job_id (no sent_at/provider_message_id) → booked_after_at=None."""
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=1)

    async with session_maker() as session:
        async with session.begin():
            job = MessageJob(
                company_id=758285,
                job_type="newsletter_new_clients_monthly",
                run_at=_COMPLETED_AT,
                status="queued",
                dedupe_key="test-job-only-001",
                payload={"campaign_run_id": run_id},
            )
            session.add(job)
            await session.flush()

            record = Record(
                company_id=758285,
                altegio_record_id=10003,
                client_id=1,
                raw={},
            )
            session.add(record)
            await session.flush()

            session.add(
                AltegioEvent(
                    dedupe_key="test-job-only-event-001",
                    company_id=758285,
                    resource="record",
                    resource_id=10003,
                    event_status="create",
                    received_at=event_at,
                )
            )

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=1,
                    phone_e164="+10000000001",
                    display_name="Job Only Client",
                    status="queued",
                    message_job_id=job.id,
                    outbox_message_id=None,
                    provider_message_id=None,
                    sent_at=None,
                )
            )
            await session.flush()

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run.booked_after_count == 0, "message_job_id alone is not a positive send signal"


@pytest.mark.asyncio
async def test_booked_after_null_for_failed_outbox_recipient(session_maker) -> None:
    """Recipient with outbox.status='failed' → booked_after_at=None (no positive send)."""
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=1)

    async with session_maker() as session:
        async with session.begin():
            # Production-realistic: failed outbox still has sent_at/provider_message_id
            # because _backfill_recipient_links copies them from the outbox row even
            # when status='failed'. The hard block must come from ob.status, not copied fields.
            outbox = OutboxMessage(
                company_id=758285,
                phone_e164="+10000000001",
                template_code="newsletter_new_clients_monthly",
                language="de",
                body="Test",
                status="failed",
                scheduled_at=_COMPLETED_AT,
                sent_at=_COMPLETED_AT,
                provider_message_id="wamid.test-failed",
                meta={},
            )
            session.add(outbox)
            await session.flush()

            record = Record(
                company_id=758285,
                altegio_record_id=10004,
                client_id=1,
                raw={},
            )
            session.add(record)
            await session.flush()

            session.add(
                AltegioEvent(
                    dedupe_key="test-failed-outbox-event-001",
                    company_id=758285,
                    resource="record",
                    resource_id=10004,
                    event_status="create",
                    received_at=event_at,
                )
            )

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=1,
                    phone_e164="+10000000001",
                    display_name="Failed Outbox Client",
                    status="queued",
                    outbox_message_id=outbox.id,
                    provider_message_id=None,
                    sent_at=None,
                )
            )

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run.booked_after_count == 0, "failed outbox is not a positive send signal"


@pytest.mark.asyncio
async def test_booked_after_set_for_outbox_sent_recipient(session_maker) -> None:
    """Recipient with outbox.status='sent' → booked_after_at is set (positive send signal)."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=1)

    async with session_maker() as session:
        async with session.begin():
            outbox = OutboxMessage(
                company_id=758285,
                phone_e164="+10000000001",
                template_code="newsletter_new_clients_monthly",
                language="de",
                body="Test",
                status="sent",
                scheduled_at=_COMPLETED_AT,
                sent_at=_COMPLETED_AT,
                meta={},
            )
            session.add(outbox)
            await session.flush()

            record = Record(
                company_id=758285,
                altegio_record_id=10005,
                client_id=1,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-outbox-sent-event-001",
                    company_id=758285,
                    resource="record",
                    resource_id=10005,
                    event_status="create",
                    received_at=event_at,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=1,
                phone_e164="+10000000001",
                display_name="Sent Outbox Client",
                status="queued",
                outbox_message_id=outbox.id,
                provider_message_id=None,
                sent_at=None,  # will be backfilled from outbox.sent_at
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at == event_at, "outbox.status='sent' is a positive send signal"


@pytest.mark.asyncio
async def test_booked_after_null_for_skipped_recipient(session_maker) -> None:
    """Skipped recipient (excluded_reason set) → booked_after_at=None even with sent_at."""
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=1)

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=10006,
                client_id=1,
                raw={},
            )
            session.add(record)
            await session.flush()

            session.add(
                AltegioEvent(
                    dedupe_key="test-skipped-event-001",
                    company_id=758285,
                    resource="record",
                    resource_id=10006,
                    event_status="create",
                    received_at=event_at,
                )
            )

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=1,
                    phone_e164="+10000000001",
                    display_name="Skipped Client",
                    status="skipped",
                    excluded_reason="provider_error",
                    # provider_message_id and sent_at are intentionally not None
                    # to confirm that excluded_reason blocks attribution regardless
                    provider_message_id="wamid.test-skipped",
                    sent_at=_COMPLETED_AT,
                )
            )

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run.booked_after_count == 0, "excluded_reason blocks booked-after attribution"


# ---------------------------------------------------------------------------
# Blocker 3: deleted records are excluded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_booked_after_null_for_deleted_record_without_lash_service(session_maker) -> None:
    """Record with is_deleted=True and no lash service → booked_after_at=None.

    is_deleted is no longer filtered (create event proves booking regardless of
    later status). The NULL here is because the record has no RecordService with
    a lash category. See test_booked_after_set_for_deleted_lash_record for the
    case where a deleted record WITH a lash service still counts.
    """
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=1)

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=10007,
                client_id=1,
                is_deleted=True,
                raw={},
            )
            session.add(record)
            await session.flush()

            session.add(
                AltegioEvent(
                    dedupe_key="test-deleted-record-001",
                    company_id=758285,
                    resource="record",
                    resource_id=10007,
                    event_status="create",
                    received_at=event_at,
                )
            )

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=1,
                    phone_e164="+10000000001",
                    display_name="Deleted Record Client",
                    status="provider_accepted",
                    sent_at=_COMPLETED_AT,
                )
            )

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run.booked_after_count == 0, "no lash service → booked_after_count stays 0"


# ---------------------------------------------------------------------------
# Blocker 4: read_at must not be fabricated via utcnow()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_at_stays_null_when_no_meta_timestamp(session_maker) -> None:
    """read_at=None when outbox.status='read' but meta has no timestamp.

    read_count must still be 1 (from outbox_status_counts).
    recipient.status must be 'read' (from _sync_recipient_statuses).
    follow-up unread_only must NOT pick this recipient as unread.
    """
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            outbox = OutboxMessage(
                company_id=758285,
                phone_e164="+4915199990050",
                template_code="newsletter_new_clients_monthly",
                language="de",
                body="Test",
                status="read",
                scheduled_at=_COMPLETED_AT,
                meta={},  # no wa_status_read key at all
            )
            session.add(outbox)
            await session.flush()

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                phone_e164="+4915199990050",
                display_name="No Timestamp Client",
                status="queued",
                outbox_message_id=outbox.id,
                sent_at=_COMPLETED_AT,
                read_at=None,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.read_at is None, "no timestamp must not produce a fabricated read_at"
    assert run.read_count == 1, "read_count must still be 1 from outbox status"
    assert recip.status == "read", "recipient.status must be 'read' from status sync"

    # follow-up eligibility: unread_only must not pick this recipient
    assert _is_eligible_for_followup(recip, "unread_only") is False, (
        "status='read' must block unread_only follow-up even without read_at"
    )


@pytest.mark.asyncio
async def test_read_at_stays_null_when_bad_meta_timestamp(session_maker) -> None:
    """read_at=None when wa_status_read timestamp is present but unparseable."""
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            outbox = OutboxMessage(
                company_id=758285,
                phone_e164="+4915199990051",
                template_code="newsletter_new_clients_monthly",
                language="de",
                body="Test",
                status="read",
                scheduled_at=_COMPLETED_AT,
                meta={"wa_status_read": {"timestamp": "bad"}},
            )
            session.add(outbox)
            await session.flush()

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                phone_e164="+4915199990051",
                display_name="Bad Timestamp Client",
                status="queued",
                outbox_message_id=outbox.id,
                sent_at=_COMPLETED_AT,
                read_at=None,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.read_at is None, "bad timestamp must not produce a fabricated read_at"
    assert run.read_count == 1


def test_followup_unread_only_excludes_recipient_with_read_status_but_no_read_at() -> None:
    """unread_only: status='read' without read_at → still NOT eligible (status wins)."""
    r = _make_recipient(status="read", read_at=None)
    assert _is_eligible_for_followup(r, "unread_only") is False


def test_followup_unread_or_not_booked_excludes_read_status_and_booked() -> None:
    """unread_or_not_booked: status='read' (no read_at) + booked_after_at set → excluded."""
    r = _make_recipient(status="read", read_at=None, booked_after_at=_NOW)
    assert _is_eligible_for_followup(r, "unread_or_not_booked") is False


# ---------------------------------------------------------------------------
# Blocker 5: CRM-only fallback attribution via altegio_client_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_booked_after_crm_only_via_altegio_client_id(session_maker) -> None:
    """booked_after_at set via Record.altegio_client_id when recipient.client_id=None."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=5)
    altegio_client_id = 99887

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=10008,
                client_id=None,  # no local client
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-crm-only-fallback-001",
                    company_id=758285,
                    resource="record",
                    resource_id=10008,
                    event_status="create",
                    received_at=event_at,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,  # CRM-only: no local Client row
                altegio_client_id=altegio_client_id,
                phone_e164="+4915199990099",
                display_name="CRM-only Client",
                status="provider_accepted",
                provider_message_id="wamid.test-crm-only",
                sent_at=_COMPLETED_AT,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at == event_at, "CRM-only fallback via altegio_client_id must work"
    assert run.booked_after_count == 1


# ---------------------------------------------------------------------------
# P1: linked failed/non-positive outbox hard-blocks attribution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_booked_after_null_for_failed_outbox_with_copied_sent_at(session_maker) -> None:
    """outbox.status='failed' with sent_at/provider_message_id → booked_after_at=None.

    _backfill_recipient_links copies sent_at and provider_message_id from the
    outbox row to the recipient. The eligibility check must use ONLY ob.status
    to prevent that from creating a false positive.
    """
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=1)
    altegio_client_id = 99100

    async with session_maker() as session:
        async with session.begin():
            outbox = OutboxMessage(
                company_id=758285,
                phone_e164="+10000000100",
                template_code="newsletter_new_clients_monthly",
                language="de",
                body="Test",
                status="failed",
                scheduled_at=_COMPLETED_AT,
                sent_at=_COMPLETED_AT,
                provider_message_id="wamid.test-p1-failed",
                meta={},
            )
            session.add(outbox)
            await session.flush()

            record = Record(
                company_id=758285,
                altegio_record_id=10090,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(record)
            await session.flush()

            session.add(
                AltegioEvent(
                    dedupe_key="test-p1-failed-event-001",
                    company_id=758285,
                    resource="record",
                    resource_id=10090,
                    event_status="create",
                    received_at=event_at,
                )
            )

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=None,
                    altegio_client_id=altegio_client_id,
                    phone_e164="+10000000100",
                    display_name="P1 Failed",
                    status="queued",
                    outbox_message_id=outbox.id,
                    provider_message_id=None,
                    sent_at=None,
                )
            )

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run.booked_after_count == 0, "failed outbox must hard-block attribution even with sent_at copied"


@pytest.mark.asyncio
async def test_booked_after_null_for_queued_outbox(session_maker) -> None:
    """outbox.status='queued' (not a positive status) → booked_after_at=None."""
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=1)
    altegio_client_id = 99101

    async with session_maker() as session:
        async with session.begin():
            outbox = OutboxMessage(
                company_id=758285,
                phone_e164="+10000000101",
                template_code="newsletter_new_clients_monthly",
                language="de",
                body="Test",
                status="queued",
                scheduled_at=_COMPLETED_AT,
                meta={},
            )
            session.add(outbox)
            await session.flush()

            record = Record(
                company_id=758285,
                altegio_record_id=10091,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(record)
            await session.flush()

            session.add(
                AltegioEvent(
                    dedupe_key="test-p1-queued-event-001",
                    company_id=758285,
                    resource="record",
                    resource_id=10091,
                    event_status="create",
                    received_at=event_at,
                )
            )

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=None,
                    altegio_client_id=altegio_client_id,
                    phone_e164="+10000000101",
                    display_name="P1 Queued",
                    status="queued",
                    outbox_message_id=outbox.id,
                    provider_message_id=None,
                    sent_at=None,
                )
            )

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run.booked_after_count == 0, "queued outbox is not a positive send signal"


# ---------------------------------------------------------------------------
# P2: attribution window starts at max(completed_at, sent_at)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_booked_after_null_when_sent_before_completed_and_event_between(session_maker) -> None:
    """sent_at < completed_at; event falls between sent_at and completed_at → NULL.

    With the max() guard, attr_start=completed_at, so an event between
    sent_at and completed_at is outside the attribution window.
    Lash service present to confirm NULL is due to timing, not missing service.
    """
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    sent_at = _COMPLETED_AT - timedelta(hours=2)
    event_at = _COMPLETED_AT - timedelta(hours=1)
    altegio_client_id = 99102

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=10092,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-p2-before-completed-001",
                    company_id=758285,
                    resource="record",
                    resource_id=10092,
                    event_status="create",
                    received_at=event_at,
                )
            )

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=None,
                    altegio_client_id=altegio_client_id,
                    phone_e164="+10000000102",
                    display_name="P2 Early Sent",
                    status="provider_accepted",
                    provider_message_id="wamid.test-p2-early",
                    sent_at=sent_at,
                )
            )

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run.booked_after_count == 0, "event before completed_at must not count when sent_at < completed_at"


@pytest.mark.asyncio
async def test_booked_after_set_when_sent_before_completed_and_event_after(session_maker) -> None:
    """sent_at < completed_at; event after completed_at → booked_after_at set.

    attr_start = max(completed_at, sent_at) = completed_at; event is after
    completed_at so it is within the attribution window.
    """
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    sent_at = _COMPLETED_AT - timedelta(hours=2)
    event_at = _COMPLETED_AT + timedelta(days=1)
    altegio_client_id = 99103

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=10093,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-p2-after-completed-001",
                    company_id=758285,
                    resource="record",
                    resource_id=10093,
                    event_status="create",
                    received_at=event_at,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=altegio_client_id,
                phone_e164="+10000000103",
                display_name="P2 After Completed",
                status="provider_accepted",
                provider_message_id="wamid.test-p2-after",
                sent_at=sent_at,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)
        run = await session.get(CampaignRun, run_id)

    assert recip.booked_after_at == event_at
    assert run.booked_after_count == 1


# ---------------------------------------------------------------------------
# P3: Record.is_deleted=NULL — COALESCE fix in runner.py is a defensive
# measure for production rows predating the NOT NULL constraint. The test
# schema enforces NOT NULL so no DB-level test is possible here; the fix
# is covered by code review of runner.py line with func.coalesce().
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# P4: follow-up read guard covers replied / booked_after_campaign
# ---------------------------------------------------------------------------


def test_followup_unread_only_excludes_replied_status() -> None:
    """unread_only: status='replied' → considered read → not eligible for follow-up."""
    r = _make_recipient(status="replied", read_at=None)
    assert _is_eligible_for_followup(r, "unread_only") is False


def test_followup_unread_only_excludes_booked_after_campaign_status() -> None:
    """unread_only: status='booked_after_campaign' → considered read → not eligible."""
    r = _make_recipient(status="booked_after_campaign", read_at=None)
    assert _is_eligible_for_followup(r, "unread_only") is False


def test_followup_unread_or_not_booked_excludes_replied_with_booking() -> None:
    """unread_or_not_booked: status='replied' + booked_after_at set → not eligible."""
    r = _make_recipient(status="replied", read_at=None, booked_after_at=_NOW)
    assert _is_eligible_for_followup(r, "unread_or_not_booked") is False


def test_followup_unread_or_not_booked_eligible_when_booked_after_campaign_no_booking() -> None:
    """unread_or_not_booked: status='booked_after_campaign' + booked_after_at=None.

    Business rule: status may be set before booked_after_at is backfilled.
    In that edge case, booked_after_at=None means 'not booked yet in our DB',
    so the recipient is still eligible per the not-booked branch.
    """
    r = _make_recipient(status="booked_after_campaign", read_at=None, booked_after_at=None)
    assert _is_eligible_for_followup(r, "unread_or_not_booked") is True


# ---------------------------------------------------------------------------
# P5: queue_failed / card_issue_failed block attribution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_booked_after_null_for_queue_failed_recipient(session_maker) -> None:
    """status='queue_failed' → booked_after_at=None (failure status blocks attribution)."""
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=1)
    altegio_client_id = 99105

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=10095,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(record)
            await session.flush()

            session.add(
                AltegioEvent(
                    dedupe_key="test-p5-queue-failed-001",
                    company_id=758285,
                    resource="record",
                    resource_id=10095,
                    event_status="create",
                    received_at=event_at,
                )
            )

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=None,
                    altegio_client_id=altegio_client_id,
                    phone_e164="+10000000105",
                    display_name="P5 Queue Failed",
                    status="queue_failed",
                    provider_message_id="wamid.test-p5-q",
                    sent_at=_COMPLETED_AT,
                )
            )

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run.booked_after_count == 0, "queue_failed must block booked-after attribution"


@pytest.mark.asyncio
async def test_booked_after_null_for_card_issue_failed_recipient(session_maker) -> None:
    """status='card_issue_failed' → booked_after_at=None (failure status blocks attribution)."""
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=1)
    altegio_client_id = 99106

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=10096,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(record)
            await session.flush()

            session.add(
                AltegioEvent(
                    dedupe_key="test-p5-card-failed-001",
                    company_id=758285,
                    resource="record",
                    resource_id=10096,
                    event_status="create",
                    received_at=event_at,
                )
            )

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=None,
                    altegio_client_id=altegio_client_id,
                    phone_e164="+10000000106",
                    display_name="P5 Card Issue Failed",
                    status="card_issue_failed",
                    provider_message_id="wamid.test-p5-c",
                    sent_at=_COMPLETED_AT,
                )
            )

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run.booked_after_count == 0, "card_issue_failed must block booked-after attribution"


# ===========================================================================
# Lash-only booked-after attribution (new business rule)
# ===========================================================================

# ---------------------------------------------------------------------------
# Test 1: lash booking inside window → booked_after_at set
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lash_booking_after_campaign_counts(session_maker) -> None:
    """Lash record/create inside attribution window → booked_after_at set."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=3)
    altegio_client_id = 99200

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=20001,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-lash-001",
                    company_id=758285,
                    resource="record",
                    resource_id=20001,
                    event_status="create",
                    received_at=event_at,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=altegio_client_id,
                phone_e164="+49151000200",
                display_name="Lash Client",
                status="provider_accepted",
                provider_message_id="wamid.lash-001",
                sent_at=_COMPLETED_AT,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)
        run = await session.get(CampaignRun, run_id)

    assert recip.booked_after_at == event_at
    assert run.booked_after_count == 1


# ---------------------------------------------------------------------------
# Test 2: non-lash booking → booked_after_at stays None
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_lash_booking_after_campaign_does_not_count(session_maker) -> None:
    """Non-lash record/create inside attribution window → booked_after_at=None.

    Regression: Özelm-style case — Hygienische Pediküre, Gesichtsreinigung
    must NOT count as booked_after for a lash welcome campaign.
    """
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=3)
    altegio_client_id = 99201

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=20002,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_NON_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-nonlash-001",
                    company_id=758285,
                    resource="record",
                    resource_id=20002,
                    event_status="create",
                    received_at=event_at,
                )
            )

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=None,
                    altegio_client_id=altegio_client_id,
                    phone_e164="+49151000201",
                    display_name="Non-Lash Client",
                    status="provider_accepted",
                    provider_message_id="wamid.nonlash-001",
                    sent_at=_COMPLETED_AT,
                )
            )

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run.booked_after_count == 0, "non-lash booking must not count as booked_after"


# ---------------------------------------------------------------------------
# Test 3: mixed services (non-lash + lash) → booked_after_at set
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mixed_services_booking_counts_when_lash_present(session_maker) -> None:
    """Record with one non-lash + one lash service → booked_after_at is set."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=2)
    altegio_client_id = 99202

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=20003,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(record)
            await session.flush()
            # Both a non-lash and a lash service — lash wins
            session.add(RecordService(record_id=record.id, service_id=_NON_LASH_SVC_ID))
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-mixed-svc-001",
                    company_id=758285,
                    resource="record",
                    resource_id=20003,
                    event_status="create",
                    received_at=event_at,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=altegio_client_id,
                phone_e164="+49151000202",
                display_name="Mixed Services Client",
                status="provider_accepted",
                provider_message_id="wamid.mixed-001",
                sent_at=_COMPLETED_AT,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at == event_at, "record with ≥1 lash service must count"


# ---------------------------------------------------------------------------
# Test 4: future appointment date doesn't block attribution (event time matters)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lash_booking_with_future_appointment_still_counts(session_maker) -> None:
    """Record.starts_at far in future; event.received_at inside window → booked_after_at set."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker, attribution_window_days=30)
    event_at = _COMPLETED_AT + timedelta(days=10)
    altegio_client_id = 99203

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=20004,
                client_id=None,
                altegio_client_id=altegio_client_id,
                starts_at=_COMPLETED_AT + timedelta(days=180),  # far future
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-future-appt-001",
                    company_id=758285,
                    resource="record",
                    resource_id=20004,
                    event_status="create",
                    received_at=event_at,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=altegio_client_id,
                phone_e164="+49151000203",
                display_name="Future Appt Client",
                status="provider_accepted",
                provider_message_id="wamid.future-001",
                sent_at=_COMPLETED_AT,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at == event_at, "appointment start date must not affect attribution"


# ---------------------------------------------------------------------------
# Test 5 & 6: attendance/confirmed/is_deleted are NOT checked
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lash_booking_counts_regardless_of_attendance(session_maker) -> None:
    """attendance=0, confirmed=0, visit_attendance=0 → booked_after_at still set for lash."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=4)
    altegio_client_id = 99204

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=20005,
                client_id=None,
                altegio_client_id=altegio_client_id,
                attendance=0,
                confirmed=0,
                visit_attendance=0,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-attendance-ignored-001",
                    company_id=758285,
                    resource="record",
                    resource_id=20005,
                    event_status="create",
                    received_at=event_at,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=altegio_client_id,
                phone_e164="+49151000204",
                display_name="No-Show Client",
                status="provider_accepted",
                provider_message_id="wamid.noshow-001",
                sent_at=_COMPLETED_AT,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at == event_at, "visit attendance must not block lash attribution"


@pytest.mark.asyncio
async def test_booked_after_set_for_deleted_lash_record(session_maker) -> None:
    """Lash record with is_deleted=True still counts — create event proves booking happened.

    Business rule: we count the booking creation event, not the current record
    status. Cancellation in Altegio uses confirmed=0; is_deleted=True may indicate
    physical deletion. Either way, the marketing action (booking intent) occurred.
    """
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=2)
    altegio_client_id = 99205

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=20006,
                client_id=None,
                altegio_client_id=altegio_client_id,
                is_deleted=True,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-deleted-lash-001",
                    company_id=758285,
                    resource="record",
                    resource_id=20006,
                    event_status="create",
                    received_at=event_at,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=altegio_client_id,
                phone_e164="+49151000205",
                display_name="Deleted Lash Client",
                status="provider_accepted",
                provider_message_id="wamid.deleted-001",
                sent_at=_COMPLETED_AT,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)
        run = await session.get(CampaignRun, run_id)

    assert recip.booked_after_at == event_at, "deleted lash record must count — create proves booking"
    assert run.booked_after_count == 1


# ---------------------------------------------------------------------------
# Test 7: first lash booking wins over earlier non-lash booking
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_first_lash_event_wins_over_earlier_non_lash(session_maker) -> None:
    """Earlier non-lash event is ignored; later lash event sets booked_after_at."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    non_lash_event_at = _COMPLETED_AT + timedelta(days=1)
    lash_event_at = _COMPLETED_AT + timedelta(days=3)
    altegio_client_id = 99206

    async with session_maker() as session:
        async with session.begin():
            non_lash_record = Record(
                company_id=758285,
                altegio_record_id=20007,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(non_lash_record)
            await session.flush()
            session.add(RecordService(record_id=non_lash_record.id, service_id=_NON_LASH_SVC_ID))

            lash_record = Record(
                company_id=758285,
                altegio_record_id=20008,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(lash_record)
            await session.flush()
            session.add(RecordService(record_id=lash_record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-nonlash-first-001",
                    company_id=758285,
                    resource="record",
                    resource_id=20007,
                    event_status="create",
                    received_at=non_lash_event_at,
                )
            )
            session.add(
                AltegioEvent(
                    dedupe_key="test-lash-second-001",
                    company_id=758285,
                    resource="record",
                    resource_id=20008,
                    event_status="create",
                    received_at=lash_event_at,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=altegio_client_id,
                phone_e164="+49151000206",
                display_name="Mixed Order Client",
                status="provider_accepted",
                provider_message_id="wamid.mixed-order-001",
                sent_at=_COMPLETED_AT,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at == lash_event_at, "booked_after_at must be the LASH event time"


# ---------------------------------------------------------------------------
# Test 8: lash event outside window ignored; lash event inside window counts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lash_event_outside_window_ignored_inside_counts(session_maker) -> None:
    """Lash event outside attribution window is excluded; lash event inside is counted."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker, attribution_window_days=7)
    inside_event_at = _COMPLETED_AT + timedelta(days=5)
    outside_event_at = _COMPLETED_AT + timedelta(days=8)  # > 7-day window
    altegio_client_id = 99207

    async with session_maker() as session:
        async with session.begin():
            record_inside = Record(
                company_id=758285,
                altegio_record_id=20009,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(record_inside)
            await session.flush()
            session.add(RecordService(record_id=record_inside.id, service_id=_LASH_SVC_ID))

            record_outside = Record(
                company_id=758285,
                altegio_record_id=20010,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(record_outside)
            await session.flush()
            session.add(RecordService(record_id=record_outside.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-inside-window-001",
                    company_id=758285,
                    resource="record",
                    resource_id=20009,
                    event_status="create",
                    received_at=inside_event_at,
                )
            )
            session.add(
                AltegioEvent(
                    dedupe_key="test-outside-window-001",
                    company_id=758285,
                    resource="record",
                    resource_id=20010,
                    event_status="create",
                    received_at=outside_event_at,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=altegio_client_id,
                phone_e164="+49151000207",
                display_name="Window Client",
                status="provider_accepted",
                provider_message_id="wamid.window-001",
                sent_at=_COMPLETED_AT,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at == inside_event_at, "booked_after_at must use inside-window event"


# ---------------------------------------------------------------------------
# Test 9: failed outbox hard-blocks even with lash booking present
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_failed_outbox_blocks_lash_attribution(session_maker) -> None:
    """failed outbox + lash record → booked_after_at=None (eligibility blocks attribution)."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=2)
    altegio_client_id = 99208

    async with session_maker() as session:
        async with session.begin():
            outbox = OutboxMessage(
                company_id=758285,
                phone_e164="+49151000208",
                template_code="newsletter_new_clients_monthly",
                language="de",
                body="Test",
                status="failed",
                scheduled_at=_COMPLETED_AT,
                sent_at=_COMPLETED_AT,
                provider_message_id="wamid.failed-lash-001",
                meta={},
            )
            session.add(outbox)
            await session.flush()

            record = Record(
                company_id=758285,
                altegio_record_id=20011,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-failed-lash-001",
                    company_id=758285,
                    resource="record",
                    resource_id=20011,
                    event_status="create",
                    received_at=event_at,
                )
            )

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=None,
                    altegio_client_id=altegio_client_id,
                    phone_e164="+49151000208",
                    display_name="Failed+Lash Client",
                    status="queued",
                    outbox_message_id=outbox.id,
                    provider_message_id=None,
                    sent_at=None,
                )
            )

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run.booked_after_count == 0, "failed outbox must block attribution regardless of lash service"


# ---------------------------------------------------------------------------
# Test 10: ServiceLookupError → no booked_after_at, warning logged, no crash
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_service_lookup_error_does_not_count_as_lash(session_maker, monkeypatch) -> None:
    """ServiceLookupError → record excluded from attribution; recompute does not crash."""
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=2)
    altegio_client_id = 99209

    async def _raise_lookup_error(company_id, service_id, http_client=None, *, strict_lookup=False):
        raise ServiceLookupError(f"mock error for service_id={service_id}")

    monkeypatch.setattr("altegio_bot.service_filter.is_lash_service", _raise_lookup_error)

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=20012,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_ERROR_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-lookup-error-001",
                    company_id=758285,
                    resource="record",
                    resource_id=20012,
                    event_status="create",
                    received_at=event_at,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=altegio_client_id,
                phone_e164="+49151000209",
                display_name="Error Client",
                status="provider_accepted",
                provider_message_id="wamid.error-001",
                sent_at=_COMPLETED_AT,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    # Must not raise; ServiceLookupError is caught inside filter_lash_record_ids
    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)
        run = await session.get(CampaignRun, run_id)

    assert recip.booked_after_at is None, "ServiceLookupError must not silently count as lash"
    assert run.booked_after_count == 0
    # P2: lookup failure must appear in summary return value
    assert summary["booked_after_service_lookup_failed_count"] == 1
    assert _ERROR_SVC_ID in summary["booked_after_service_lookup_failed_service_ids"]
    # P2: lookup failure must be persisted in run.meta for operator visibility
    assert run.meta is not None
    lr = run.meta.get("last_recompute", {})
    assert lr.get("booked_after_service_lookup_failed_count", 0) == 1
    assert _ERROR_SVC_ID in lr.get("booked_after_service_lookup_failed_service_ids", [])


# ---------------------------------------------------------------------------
# Test 11: CRM-only fallback via altegio_client_id with lash service
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lash_attribution_crm_only_fallback(session_maker) -> None:
    """CRM-only recipient (client_id=None) matched via altegio_client_id + lash service → set."""
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=6)
    altegio_client_id = 99210

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=20013,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-crm-lash-001",
                    company_id=758285,
                    resource="record",
                    resource_id=20013,
                    event_status="create",
                    received_at=event_at,
                )
            )

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=altegio_client_id,
                phone_e164="+49151000210",
                display_name="CRM-only Lash Client",
                status="provider_accepted",
                provider_message_id="wamid.crm-lash-001",
                sent_at=_COMPLETED_AT,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)
        run = await session.get(CampaignRun, run_id)

    assert recip.booked_after_at == event_at
    assert run.booked_after_count == 1


# ---------------------------------------------------------------------------
# Test 12: Özelm regression — non-lash bookings after campaign → count stays 0
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_oezelm_regression_non_lash_bookings_do_not_count(session_maker) -> None:
    """Production regression: multiple non-lash bookings (Pediküre, Gesichtsreinigung) → 0.

    Before the lash filter was added, Campaign Run #16 incorrectly attributed
    non-lash records for Özelm as booked_after. This test prevents regression.
    """
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    altegio_client_id = 99211

    async with session_maker() as session:
        async with session.begin():
            pedikure_record = Record(
                company_id=758285,
                altegio_record_id=20014,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(pedikure_record)
            await session.flush()
            session.add(RecordService(record_id=pedikure_record.id, service_id=_NON_LASH_SVC_ID))

            gesicht_record = Record(
                company_id=758285,
                altegio_record_id=20015,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(gesicht_record)
            await session.flush()
            session.add(RecordService(record_id=gesicht_record.id, service_id=_NON_LASH_SVC_ID))

            # Both events inside attribution window
            session.add(
                AltegioEvent(
                    dedupe_key="test-oezelm-pedikure-001",
                    company_id=758285,
                    resource="record",
                    resource_id=20014,
                    event_status="create",
                    received_at=_COMPLETED_AT + timedelta(days=2),
                )
            )
            session.add(
                AltegioEvent(
                    dedupe_key="test-oezelm-gesicht-001",
                    company_id=758285,
                    resource="record",
                    resource_id=20015,
                    event_status="create",
                    received_at=_COMPLETED_AT + timedelta(days=3),
                )
            )

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=None,
                    altegio_client_id=altegio_client_id,
                    phone_e164="+49151000211",
                    display_name="Özelm",
                    status="provider_accepted",
                    provider_message_id="wamid.oezelm-001",
                    sent_at=_COMPLETED_AT,
                )
            )

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run.booked_after_count == 0, "Özelm: non-lash bookings must not count as booked_after"


# ---------------------------------------------------------------------------
# P1: lash lookup called only for records matched to eligible recipients
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lash_lookup_skips_unrelated_record(session_maker, monkeypatch) -> None:
    """filter_lash_record_ids receives only related recipient's record_id, not unrelated ones.

    Two records exist inside the broad overall attribution window:
    - record A belongs to the eligible campaign recipient (lash service, cached).
    - record B belongs to an unrelated client with no CampaignRecipient row.

    After Phase-1 filtering (P1 fix), filter_lash_record_ids must receive only
    record A's id. If record B's id were also passed, the spy would capture it.
    """
    _seed_lash_cache()
    run_id = await _make_run(session_maker)

    related_altegio_client_id = 79100
    unrelated_altegio_client_id = 79200

    async with session_maker() as session:
        async with session.begin():
            # Record A — belongs to the eligible campaign recipient
            record_a = Record(
                company_id=758285,
                altegio_record_id=30001,
                client_id=None,
                altegio_client_id=related_altegio_client_id,
                raw={},
            )
            session.add(record_a)
            await session.flush()
            session.add(RecordService(record_id=record_a.id, service_id=_LASH_SVC_ID))

            # Record B — unrelated client, no CampaignRecipient
            record_b = Record(
                company_id=758285,
                altegio_record_id=30002,
                client_id=None,
                altegio_client_id=unrelated_altegio_client_id,
                raw={},
            )
            session.add(record_b)
            await session.flush()
            session.add(RecordService(record_id=record_b.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-p1-spy-related-001",
                    company_id=758285,
                    resource="record",
                    resource_id=30001,
                    event_status="create",
                    received_at=_COMPLETED_AT + timedelta(days=3),
                )
            )
            # Unrelated event is within the broad window but has no matching recipient
            session.add(
                AltegioEvent(
                    dedupe_key="test-p1-spy-unrelated-001",
                    company_id=758285,
                    resource="record",
                    resource_id=30002,
                    event_status="create",
                    received_at=_COMPLETED_AT + timedelta(days=1),
                )
            )

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=None,
                    altegio_client_id=related_altegio_client_id,
                    phone_e164="+4915179100001",
                    display_name="Related Client",
                    status="provider_accepted",
                    provider_message_id="wamid.p1-spy-001",
                    sent_at=_COMPLETED_AT,
                )
            )

    # Spy: capture all record_ids passed to filter_lash_record_ids
    from altegio_bot.service_filter import filter_lash_record_ids as _real_filter

    captured_record_ids: list[int] = []

    async def _spy_filter(session, *, company_id, record_ids, http_client=None):
        captured_record_ids.extend(record_ids)
        return await _real_filter(session, company_id=company_id, record_ids=record_ids, http_client=http_client)

    monkeypatch.setattr("altegio_bot.campaigns.runner.filter_lash_record_ids", _spy_filter)

    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        record_a_db = await session.get(Record, record_a.id)
        record_b_db = await session.get(Record, record_b.id)

    assert record_a_db.id in captured_record_ids, "related record must be passed to lash lookup"
    assert record_b_db.id not in captured_record_ids, "unrelated record must NOT be passed to lash lookup"
    assert summary["booked_after_count"] == 1
    assert summary["booked_after_service_lookup_failed_count"] == 0


# ---------------------------------------------------------------------------
# P1+P2: unrelated record with error service is filtered before lookup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unrelated_error_service_filtered_before_lookup(session_maker, monkeypatch) -> None:
    """Unrelated client with ServiceLookupError service does not affect failure diagnostics.

    The unrelated client's record is inside the broad overall window, but has no
    matching CampaignRecipient. After Phase-1 filtering (P1 fix), it is excluded
    before lash lookup. is_lash_service is never called for it, so:
    - recompute does not crash.
    - booked_after_count == 1 (valid related lash booking is attributed).
    - booked_after_service_lookup_failed_count == 0 (no failures for candidates).
    - run.meta has no last_recompute key (no failures to persist).
    """
    _seed_lash_cache()
    run_id = await _make_run(session_maker)

    related_altegio_client_id = 79300
    unrelated_altegio_client_id = 79400
    unrelated_svc_id = 55001  # would raise ServiceLookupError if ever looked up

    async with session_maker() as session:
        async with session.begin():
            # Related record — belongs to the eligible campaign recipient
            record_rel = Record(
                company_id=758285,
                altegio_record_id=30003,
                client_id=None,
                altegio_client_id=related_altegio_client_id,
                raw={},
            )
            session.add(record_rel)
            await session.flush()
            session.add(RecordService(record_id=record_rel.id, service_id=_LASH_SVC_ID))

            # Unrelated record — no CampaignRecipient; its service would error
            record_unrel = Record(
                company_id=758285,
                altegio_record_id=30004,
                client_id=None,
                altegio_client_id=unrelated_altegio_client_id,
                raw={},
            )
            session.add(record_unrel)
            await session.flush()
            session.add(RecordService(record_id=record_unrel.id, service_id=unrelated_svc_id))

            session.add(
                AltegioEvent(
                    dedupe_key="test-p1-errsvc-related-001",
                    company_id=758285,
                    resource="record",
                    resource_id=30003,
                    event_status="create",
                    received_at=_COMPLETED_AT + timedelta(days=4),
                )
            )
            session.add(
                AltegioEvent(
                    dedupe_key="test-p1-errsvc-unrelated-001",
                    company_id=758285,
                    resource="record",
                    resource_id=30004,
                    event_status="create",
                    received_at=_COMPLETED_AT + timedelta(days=2),
                )
            )

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=None,
                    altegio_client_id=related_altegio_client_id,
                    phone_e164="+4915179300001",
                    display_name="Related Client P1",
                    status="provider_accepted",
                    provider_message_id="wamid.p1-errsvc-001",
                    sent_at=_COMPLETED_AT,
                )
            )

    # Monkeypatch: raise ServiceLookupError for the unrelated service_id.
    # If P1 fix is working, is_lash_service is never called for unrelated_svc_id
    # (the unrelated record is filtered out in Phase 1 before lash lookup).
    async def _selective_error_is_lash(company_id, service_id, http_client=None, *, strict_lookup=False):
        if service_id == unrelated_svc_id:
            raise ServiceLookupError(f"must not be called for service_id={service_id}")
        return service_id == _LASH_SVC_ID and company_id == 758285

    monkeypatch.setattr("altegio_bot.service_filter.is_lash_service", _selective_error_is_lash)

    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert summary["booked_after_count"] == 1
    assert summary["booked_after_service_lookup_failed_count"] == 0, (
        "unrelated record filtered before lookup — failure count must be 0"
    )
    lr = (run.meta or {}).get("last_recompute", {})
    assert lr.get("booked_after_service_lookup_failed_count", 0) == 0


# ---------------------------------------------------------------------------
# P2: API endpoint returns diagnostics field when lookup failures occurred
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recompute_endpoint_includes_diagnostics_on_lookup_failure(
    http_client: AsyncClient,
    session_maker,
    monkeypatch,
) -> None:
    """POST /runs/{run_id}/recompute returns booked_after_service_lookup_failed_count in stats."""
    run_id = await _make_run(session_maker)

    async def _always_raise(company_id, service_id, http_client=None, *, strict_lookup=False):
        raise ServiceLookupError(f"endpoint-diag mock error service_id={service_id}")

    monkeypatch.setattr("altegio_bot.service_filter.is_lash_service", _always_raise)

    altegio_client_id = 79500

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=30005,
                client_id=None,
                altegio_client_id=altegio_client_id,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_ERROR_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-p2-endpoint-diag-001",
                    company_id=758285,
                    resource="record",
                    resource_id=30005,
                    event_status="create",
                    received_at=_COMPLETED_AT + timedelta(days=5),
                )
            )

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=None,
                    altegio_client_id=altegio_client_id,
                    phone_e164="+4915179500001",
                    display_name="Diag Client",
                    status="provider_accepted",
                    provider_message_id="wamid.p2-diag-001",
                    sent_at=_COMPLETED_AT,
                )
            )

    response = await http_client.post(f"/ops/campaigns/runs/{run_id}/recompute")
    assert response.status_code == 200

    stats = response.json()["stats"]
    assert "booked_after_service_lookup_failed_count" in stats, (
        "recompute endpoint must expose lookup failure diagnostics"
    )
    assert stats["booked_after_service_lookup_failed_count"] == 1
    assert _ERROR_SVC_ID in stats["booked_after_service_lookup_failed_service_ids"]
    assert stats["booked_after_count"] == 0


# ==========================================================================
# P1: Full recompute semantics — stale non-lash attributions are corrected
# ==========================================================================


@pytest.mark.asyncio
async def test_stale_non_lash_booked_after_cleared_by_recompute(session_maker) -> None:
    """Recipient has old booked_after_at from non-lash booking; only non-lash events exist.

    After lash-only recompute: booked_after_at → None, booked_after_count → 0.
    """
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    stale_ts = _COMPLETED_AT + timedelta(days=1)

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=40001,
                client_id=None,
                altegio_client_id=80001,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_NON_LASH_SVC_ID))
            session.add(
                AltegioEvent(
                    dedupe_key="test-p1-stale-clear-001",
                    company_id=758285,
                    resource="record",
                    resource_id=40001,
                    event_status="create",
                    received_at=stale_ts,
                )
            )
            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=80001,
                phone_e164="+4915180001001",
                display_name="Stale NonLash",
                status="provider_accepted",
                provider_message_id="wamid.stale-clear-001",
                sent_at=_COMPLETED_AT,
                booked_after_at=stale_ts,  # stale non-lash attribution
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)
        run = await session.get(CampaignRun, run_id)

    assert recip.booked_after_at is None, "stale non-lash booked_after_at must be cleared"
    assert run.booked_after_count == 0


@pytest.mark.asyncio
async def test_stale_non_lash_replaced_by_lash_event(session_maker) -> None:
    """Recipient has old non-lash booked_after_at; recompute finds non-lash first, lash later.

    After recompute: booked_after_at is updated to the lash event timestamp.
    """
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    non_lash_at = _COMPLETED_AT + timedelta(days=2)
    lash_at = _COMPLETED_AT + timedelta(days=5)

    async with session_maker() as session:
        async with session.begin():
            record_nl = Record(
                company_id=758285,
                altegio_record_id=40002,
                client_id=None,
                altegio_client_id=80002,
                raw={},
            )
            session.add(record_nl)
            await session.flush()
            session.add(RecordService(record_id=record_nl.id, service_id=_NON_LASH_SVC_ID))

            record_l = Record(
                company_id=758285,
                altegio_record_id=40003,
                client_id=None,
                altegio_client_id=80002,
                raw={},
            )
            session.add(record_l)
            await session.flush()
            session.add(RecordService(record_id=record_l.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-p1-stale-replace-nl-001",
                    company_id=758285,
                    resource="record",
                    resource_id=40002,
                    event_status="create",
                    received_at=non_lash_at,
                )
            )
            session.add(
                AltegioEvent(
                    dedupe_key="test-p1-stale-replace-l-001",
                    company_id=758285,
                    resource="record",
                    resource_id=40003,
                    event_status="create",
                    received_at=lash_at,
                )
            )
            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=80002,
                phone_e164="+4915180002001",
                display_name="Stale Then Lash",
                status="provider_accepted",
                provider_message_id="wamid.stale-replace-001",
                sent_at=_COMPLETED_AT,
                booked_after_at=non_lash_at,  # stale attribution from non-lash
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)
        run = await session.get(CampaignRun, run_id)

    assert recip.booked_after_at == lash_at, "booked_after_at must be updated to lash event"
    assert run.booked_after_count == 1


@pytest.mark.asyncio
async def test_stale_later_booked_after_corrected_to_earlier_lash(session_maker) -> None:
    """Recipient has booked_after_at pointing to a later event; earlier lash event exists.

    After recompute: booked_after_at is updated to the earlier lash booking.
    """
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    earlier_lash_at = _COMPLETED_AT + timedelta(days=3)
    later_lash_at = _COMPLETED_AT + timedelta(days=10)

    async with session_maker() as session:
        async with session.begin():
            record_early = Record(
                company_id=758285,
                altegio_record_id=40004,
                client_id=None,
                altegio_client_id=80003,
                raw={},
            )
            session.add(record_early)
            await session.flush()
            session.add(RecordService(record_id=record_early.id, service_id=_LASH_SVC_ID))

            record_late = Record(
                company_id=758285,
                altegio_record_id=40005,
                client_id=None,
                altegio_client_id=80003,
                raw={},
            )
            session.add(record_late)
            await session.flush()
            session.add(RecordService(record_id=record_late.id, service_id=_LASH_SVC_ID))

            session.add(
                AltegioEvent(
                    dedupe_key="test-p1-earlier-lash-001",
                    company_id=758285,
                    resource="record",
                    resource_id=40004,
                    event_status="create",
                    received_at=earlier_lash_at,
                )
            )
            session.add(
                AltegioEvent(
                    dedupe_key="test-p1-later-lash-001",
                    company_id=758285,
                    resource="record",
                    resource_id=40005,
                    event_status="create",
                    received_at=later_lash_at,
                )
            )
            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=80003,
                phone_e164="+4915180003001",
                display_name="Later Overwrite",
                status="provider_accepted",
                provider_message_id="wamid.later-overwrite-001",
                sent_at=_COMPLETED_AT,
                booked_after_at=later_lash_at,  # stale: points to later booking
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)
        run = await session.get(CampaignRun, run_id)

    assert recip.booked_after_at == earlier_lash_at, "must correct to earlier lash event"
    assert run.booked_after_count == 1


# ==========================================================================
# P2: Lookup failure on candidate record → clear attribution + diagnostics
# ==========================================================================


@pytest.mark.asyncio
async def test_lookup_failure_clears_stale_booked_after_and_surfaces_diagnostics(session_maker, monkeypatch) -> None:
    """ServiceLookupError on candidate record clears stale booked_after_at.

    The recipient had a pre-existing booked_after_at. The candidate record's
    service raises ServiceLookupError. After recompute:
    - booked_after_at is None (undercount preferred over stale overcount)
    - summary failure count > 0
    - run.meta['last_recompute'] contains diagnostics
    """
    run_id = await _make_run(session_maker)
    stale_ts = _COMPLETED_AT + timedelta(days=3)

    async def _always_fail(company_id, service_id, http_client=None, *, strict_lookup=False):
        raise ServiceLookupError(f"p2-lookup-fail service_id={service_id}")

    monkeypatch.setattr("altegio_bot.service_filter.is_lash_service", _always_fail)

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=40006,
                client_id=None,
                altegio_client_id=80004,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_ERROR_SVC_ID))
            session.add(
                AltegioEvent(
                    dedupe_key="test-p2-lookup-fail-001",
                    company_id=758285,
                    resource="record",
                    resource_id=40006,
                    event_status="create",
                    received_at=stale_ts,
                )
            )
            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=80004,
                phone_e164="+4915180004001",
                display_name="Stale Lookup Fail",
                status="provider_accepted",
                provider_message_id="wamid.p2-lookup-fail-001",
                sent_at=_COMPLETED_AT,
                booked_after_at=stale_ts,  # stale; will be cleared on lookup failure
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)
        run = await session.get(CampaignRun, run_id)

    assert recip.booked_after_at is None, "lookup failure must clear stale booked_after_at"
    assert run.booked_after_count == 0
    assert summary["booked_after_service_lookup_failed_count"] == 1
    lr = run.meta["last_recompute"]
    assert lr["booked_after_service_lookup_failed_count"] == 1
    assert _ERROR_SVC_ID in lr["booked_after_service_lookup_failed_service_ids"]


# ==========================================================================
# P3: Missing API credentials on cache-miss → strict lookup failure
# ==========================================================================


@pytest.mark.asyncio
async def test_missing_credentials_on_cache_miss_surfaces_as_lookup_failure(session_maker, monkeypatch) -> None:
    """Cache-miss service + no API tokens → treated as lookup failure, not silent non-lash.

    In the test environment, ALTEGIO_PARTNER_TOKEN / ALTEGIO_USER_TOKEN are not
    set. filter_lash_record_ids calls is_lash_service with strict_lookup=True,
    which causes _fetch_service_category_id to raise ServiceLookupError when
    credentials are missing, rather than silently returning False.
    """
    monkeypatch.delenv("ALTEGIO_PARTNER_TOKEN", raising=False)
    monkeypatch.delenv("ALTEGIO_USER_TOKEN", raising=False)
    _LRU_CACHE.pop((758285, _UNCACHED_SVC_ID), None)
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=4)

    # _UNCACHED_SVC_ID is NOT pre-seeded in the LRU cache.
    # With no API tokens in the test env, strict_lookup will raise ServiceLookupError.

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=40007,
                client_id=None,
                altegio_client_id=80005,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_UNCACHED_SVC_ID))
            session.add(
                AltegioEvent(
                    dedupe_key="test-p3-missing-creds-001",
                    company_id=758285,
                    resource="record",
                    resource_id=40007,
                    event_status="create",
                    received_at=event_at,
                )
            )
            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=None,
                    altegio_client_id=80005,
                    phone_e164="+4915180005001",
                    display_name="Missing Creds",
                    status="provider_accepted",
                    provider_message_id="wamid.missing-creds-001",
                    sent_at=_COMPLETED_AT,
                )
            )

    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert summary["booked_after_service_lookup_failed_count"] == 1, (
        "missing credentials must surface as lookup failure, not silent non-lash"
    )
    assert _UNCACHED_SVC_ID in summary["booked_after_service_lookup_failed_service_ids"]
    lr = run.meta["last_recompute"]
    assert lr["booked_after_service_lookup_failed_count"] == 1
    assert summary["booked_after_count"] == 0


# ==========================================================================
# P1: Stale booked_after_at cleared for ineligible recipients
# ==========================================================================


@pytest.mark.asyncio
async def test_stale_booked_after_cleared_for_failed_outbox_recipient(session_maker) -> None:
    """OutboxMessage with status='failed' hard-blocks attribution; stale booked_after_at is cleared."""
    run_id = await _make_run(session_maker)
    stale_ts = _COMPLETED_AT + timedelta(days=1)

    async with session_maker() as session:
        async with session.begin():
            outbox = OutboxMessage(
                company_id=758285,
                phone_e164="+4915180007001",
                template_code="newsletter_new_clients_monthly",
                language="de",
                body="Test",
                status="failed",
                scheduled_at=_COMPLETED_AT,
            )
            session.add(outbox)
            await session.flush()

            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=80007,
                phone_e164="+4915180007001",
                display_name="Failed Outbox",
                status="provider_accepted",
                sent_at=_COMPLETED_AT,
                provider_message_id="wamid.p1-failed-001",
                outbox_message_id=outbox.id,
                booked_after_at=stale_ts,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at is None, "stale booked_after_at must be cleared when outbox.status='failed'"


@pytest.mark.asyncio
async def test_stale_booked_after_cleared_for_skipped_recipient(session_maker) -> None:
    """Recipient with status='skipped' is ineligible; stale booked_after_at is cleared."""
    run_id = await _make_run(session_maker)
    stale_ts = _COMPLETED_AT + timedelta(days=2)

    async with session_maker() as session:
        async with session.begin():
            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=80008,
                phone_e164="+4915180008001",
                display_name="Skipped Client",
                status="skipped",
                excluded_reason="no_phone",
                booked_after_at=stale_ts,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at is None, "stale booked_after_at must be cleared for skipped/excluded recipient"


@pytest.mark.asyncio
async def test_stale_booked_after_cleared_for_job_only_recipient(session_maker) -> None:
    """Recipient with no positive send signal (status='queued', no outbox/sent_at/provider_id)
    is ineligible; stale booked_after_at is cleared."""
    run_id = await _make_run(session_maker)
    stale_ts = _COMPLETED_AT + timedelta(days=3)

    async with session_maker() as session:
        async with session.begin():
            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=80009,
                phone_e164="+4915180009001",
                display_name="Job Only",
                status="queued",
                provider_message_id=None,
                sent_at=None,
                outbox_message_id=None,
                booked_after_at=stale_ts,
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert recip.booked_after_at is None, (
        "stale booked_after_at must be cleared when recipient has no positive send signal"
    )


@pytest.mark.asyncio
async def test_positive_send_attributed_alongside_ineligible_neighbor(session_maker) -> None:
    """Positive-send recipient is attributed; ineligible neighbor's stale booked_after_at is cleared.

    Regression: the ineligible-clearing code must not prevent attribution for
    eligible recipients that share the same run.
    """
    _seed_lash_cache()
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=5)
    stale_ts = _COMPLETED_AT + timedelta(days=1)

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=40009,
                client_id=None,
                altegio_client_id=80010,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=_LASH_SVC_ID))
            session.add(
                AltegioEvent(
                    dedupe_key="test-p1-regression-positive-001",
                    company_id=758285,
                    resource="record",
                    resource_id=40009,
                    event_status="create",
                    received_at=event_at,
                )
            )
            eligible_recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=80010,
                phone_e164="+4915180010001",
                display_name="Positive Send",
                status="provider_accepted",
                sent_at=_COMPLETED_AT,
                provider_message_id="wamid.p1-regression-001",
                booked_after_at=None,
            )
            ineligible_recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                client_id=None,
                altegio_client_id=80011,
                phone_e164="+4915180011001",
                display_name="Stale Ineligible",
                status="skipped",
                booked_after_at=stale_ts,
            )
            session.add(eligible_recipient)
            session.add(ineligible_recipient)
            await session.flush()
            eligible_id = eligible_recipient.id
            ineligible_id = ineligible_recipient.id

    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        eligible = await session.get(CampaignRecipient, eligible_id)
        ineligible = await session.get(CampaignRecipient, ineligible_id)

    assert eligible.booked_after_at == event_at, "eligible recipient must be attributed"
    assert ineligible.booked_after_at is None, "ineligible neighbor's stale booked_after_at must be cleared"
    assert summary["booked_after_count"] == 1


# ==========================================================================
# P4: last_recompute always written; successful recompute clears stale failures
# ==========================================================================


@pytest.mark.asyncio
async def test_last_recompute_meta_cleared_on_clean_recompute(session_maker, monkeypatch) -> None:
    """run.meta['last_recompute'] is written on every recompute.

    Recompute #1 with ServiceLookupError → failure count > 0.
    Recompute #2 after seeding cache (simulating resolution) → failure count == 0,
    stale failed_service_ids are cleared.
    """
    run_id = await _make_run(session_maker)
    event_at = _COMPLETED_AT + timedelta(days=7)
    svc_id_for_p4 = 44001  # distinct ID; NOT in LRU cache before recompute #1

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=758285,
                altegio_record_id=40008,
                client_id=None,
                altegio_client_id=80006,
                raw={},
            )
            session.add(record)
            await session.flush()
            session.add(RecordService(record_id=record.id, service_id=svc_id_for_p4))
            session.add(
                AltegioEvent(
                    dedupe_key="test-p4-meta-clear-001",
                    company_id=758285,
                    resource="record",
                    resource_id=40008,
                    event_status="create",
                    received_at=event_at,
                )
            )
            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=None,
                    altegio_client_id=80006,
                    phone_e164="+4915180006001",
                    display_name="P4 Meta Clear",
                    status="provider_accepted",
                    provider_message_id="wamid.p4-meta-001",
                    sent_at=_COMPLETED_AT,
                )
            )

    # Recompute #1: no tokens, cache miss → lookup failure
    async with session_maker() as session:
        async with session.begin():
            summary1 = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert summary1["booked_after_service_lookup_failed_count"] == 1
    assert run.meta["last_recompute"]["booked_after_service_lookup_failed_count"] == 1

    # Seed the cache so the service resolves as lash on the next recompute
    _cache_put((758285, svc_id_for_p4), _LASH_CATEGORY_ID)

    # Recompute #2: cache hit → no failure; meta must be refreshed
    async with session_maker() as session:
        async with session.begin():
            summary2 = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert summary2["booked_after_service_lookup_failed_count"] == 0, "clean recompute must reset failure count"
    lr2 = run.meta["last_recompute"]
    assert lr2["booked_after_service_lookup_failed_count"] == 0
    assert lr2["booked_after_service_lookup_failed_service_ids"] == []
    assert summary2["booked_after_count"] == 1, "lash booking now resolves correctly"


# ==========================================================================
# Relink to latest successful outbox (Run #21 production fix)
# ==========================================================================
#
# A. Recipient linked to failed outbox; latest successful is delivered
#    → recompute relinks; delivered_count updates.
# B. Recipient linked to failed outbox; latest successful is read
#    → recompute relinks; read_count updates.
# C. Recipient already linked to the latest successful outbox
#    → idempotent; relink count 0.
# D. Skipped recipient
#    → not relinked.
# E. No successful outbox exists
#    → not relinked.
# F. Outbox with provider_message_id but status='failed'
#    → not considered successful; not relinked.
# G. Multiple successful outboxes for one job
#    → latest by id wins (highest delivery rank, ties by id).
# ==========================================================================

_RELINK_COMPANY = 758285
_RELINK_PHONE = "+4915100000099"
_RELINK_SENT_AT = _COMPLETED_AT


def _make_relink_job(session, *, run_id: int, dedupe: str) -> MessageJob:
    job = MessageJob(
        company_id=_RELINK_COMPANY,
        job_type="newsletter_new_clients_monthly",
        run_at=_RELINK_SENT_AT,
        status="done",
        dedupe_key=dedupe,
        payload={"campaign_run_id": run_id},
    )
    session.add(job)
    return job


def _make_relink_outbox(
    session,
    *,
    job_id: int,
    status: str,
    provider_message_id: str | None = "wamid.relink001",
    sent_at: datetime | None = None,
) -> OutboxMessage:
    ob = OutboxMessage(
        company_id=_RELINK_COMPANY,
        phone_e164=_RELINK_PHONE,
        template_code="newsletter_new_clients_monthly",
        language="de",
        body="Relink test",
        status=status,
        scheduled_at=_RELINK_SENT_AT,
        provider_message_id=provider_message_id,
        sent_at=sent_at or _RELINK_SENT_AT,
        job_id=job_id,
        meta={},
    )
    session.add(ob)
    return ob


def _make_relink_recipient(
    session,
    *,
    run_id: int,
    job_id: int | None,
    outbox_id: int | None,
    status: str = "queued",
    provider_message_id: str | None = None,
) -> CampaignRecipient:
    r = CampaignRecipient(
        campaign_run_id=run_id,
        company_id=_RELINK_COMPANY,
        phone_e164=_RELINK_PHONE,
        display_name="Relink Client",
        status=status,
        message_job_id=job_id,
        outbox_message_id=outbox_id,
        provider_message_id=provider_message_id,
        sent_at=_RELINK_SENT_AT if outbox_id else None,
        meta={},
    )
    session.add(r)
    return r


@pytest.mark.asyncio
async def test_relink_to_latest_successful_outbox_delivered(session_maker) -> None:
    """A. Recipient linked to failed outbox; latest successful is 'delivered'.
    Recompute relinks recipient to delivered outbox; delivered_count == 1.
    """
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            job = _make_relink_job(session, run_id=run_id, dedupe="relink-a-job")
            await session.flush()

            failed_ob = _make_relink_outbox(session, job_id=job.id, status="failed", provider_message_id=None)
            delivered_ob = _make_relink_outbox(
                session, job_id=job.id, status="delivered", provider_message_id="wamid.a-delivered"
            )
            await session.flush()

            recipient = _make_relink_recipient(
                session,
                run_id=run_id,
                job_id=job.id,
                outbox_id=failed_ob.id,
                provider_message_id=None,
            )
            await session.flush()
            recipient_id = recipient.id
            delivered_ob_id = delivered_ob.id

    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        recip = await session.get(CampaignRecipient, recipient_id)

    assert summary["relinked_recipients_count"] == 1
    assert recip.outbox_message_id == delivered_ob_id
    assert recip.provider_message_id == "wamid.a-delivered"
    assert recip.status == "delivered"
    assert run.delivered_count == 1
    assert run.read_count == 0


@pytest.mark.asyncio
async def test_relink_to_latest_successful_outbox_read(session_maker) -> None:
    """B. Recipient linked to failed outbox; latest successful is 'read'.
    Recompute relinks to read outbox; read_count == 1.
    """
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            job = _make_relink_job(session, run_id=run_id, dedupe="relink-b-job")
            await session.flush()

            failed_ob = _make_relink_outbox(session, job_id=job.id, status="failed", provider_message_id=None)
            read_ob = _make_relink_outbox(
                session,
                job_id=job.id,
                status="read",
                provider_message_id="wamid.b-read",
            )
            await session.flush()

            recipient = _make_relink_recipient(session, run_id=run_id, job_id=job.id, outbox_id=failed_ob.id)
            await session.flush()
            recipient_id = recipient.id
            read_ob_id = read_ob.id

    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        recip = await session.get(CampaignRecipient, recipient_id)

    assert summary["relinked_recipients_count"] == 1
    assert recip.outbox_message_id == read_ob_id
    assert recip.provider_message_id == "wamid.b-read"
    assert recip.status == "read"
    assert run.read_count == 1


@pytest.mark.asyncio
async def test_relink_idempotent_when_already_on_successful_outbox(session_maker) -> None:
    """C. Recipient already linked to the latest successful outbox.
    Second recompute makes no changes; relinked_recipients_count == 0.
    """
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            job = _make_relink_job(session, run_id=run_id, dedupe="relink-c-job")
            await session.flush()

            delivered_ob = _make_relink_outbox(
                session,
                job_id=job.id,
                status="delivered",
                provider_message_id="wamid.c-delivered",
            )
            await session.flush()

            _make_relink_recipient(
                session,
                run_id=run_id,
                job_id=job.id,
                outbox_id=delivered_ob.id,
                provider_message_id="wamid.c-delivered",
                status="delivered",
            )
            await session.flush()

    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    assert summary["relinked_recipients_count"] == 0


@pytest.mark.asyncio
async def test_relink_skips_skipped_recipient(session_maker) -> None:
    """D. Skipped recipient is never relinked even if a successful outbox exists."""
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            job = _make_relink_job(session, run_id=run_id, dedupe="relink-d-job")
            await session.flush()

            _make_relink_outbox(session, job_id=job.id, status="delivered", provider_message_id="wamid.d")
            await session.flush()

            recipient = _make_relink_recipient(
                session,
                run_id=run_id,
                job_id=job.id,
                outbox_id=None,
                status="skipped",
            )
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert summary["relinked_recipients_count"] == 0
    assert recip.outbox_message_id is None
    assert recip.status == "skipped"


@pytest.mark.asyncio
async def test_relink_no_successful_outbox(session_maker) -> None:
    """E. No successful outbox exists for this job → recipient not touched."""
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            job = _make_relink_job(session, run_id=run_id, dedupe="relink-e-job")
            await session.flush()

            _make_relink_outbox(
                session,
                job_id=job.id,
                status="failed",
                provider_message_id=None,
            )
            await session.flush()

            recipient = _make_relink_recipient(session, run_id=run_id, job_id=job.id, outbox_id=None)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert summary["relinked_recipients_count"] == 0
    assert recip.outbox_message_id is None


@pytest.mark.asyncio
async def test_relink_failed_with_provider_message_id_not_successful(session_maker) -> None:
    """F. Outbox with provider_message_id but status='failed' is not considered successful."""
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            job = _make_relink_job(session, run_id=run_id, dedupe="relink-f-job")
            await session.flush()

            # Has provider_message_id but status is failed → must NOT qualify
            _make_relink_outbox(
                session,
                job_id=job.id,
                status="failed",
                provider_message_id="wamid.f-has-id-but-failed",
            )
            await session.flush()

            recipient = _make_relink_recipient(session, run_id=run_id, job_id=job.id, outbox_id=None)
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert summary["relinked_recipients_count"] == 0
    assert recip.outbox_message_id is None


@pytest.mark.asyncio
async def test_relink_multiple_successful_outboxes_latest_id_wins(session_maker) -> None:
    """G. Multiple successful outboxes for one job → highest delivery rank wins,
    ties broken by highest id (latest read outbox takes precedence over older delivered).
    """
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            job = _make_relink_job(session, run_id=run_id, dedupe="relink-g-job")
            await session.flush()

            failed_ob = _make_relink_outbox(session, job_id=job.id, status="failed", provider_message_id=None)
            _make_relink_outbox(
                session,
                job_id=job.id,
                status="delivered",
                provider_message_id="wamid.g-delivered",
            )
            # read outbox has highest id AND highest delivery rank → must win
            read_ob = _make_relink_outbox(
                session,
                job_id=job.id,
                status="read",
                provider_message_id="wamid.g-read",
            )
            await session.flush()

            recipient = _make_relink_recipient(session, run_id=run_id, job_id=job.id, outbox_id=failed_ob.id)
            await session.flush()
            recipient_id = recipient.id
            read_ob_id = read_ob.id

    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        recip = await session.get(CampaignRecipient, recipient_id)

    assert summary["relinked_recipients_count"] == 1
    assert recip.outbox_message_id == read_ob_id, "read outbox must win"
    assert recip.provider_message_id == "wamid.g-read"
    assert recip.status == "read"
    assert run.read_count == 1
    assert run.delivered_count == 1  # _DELIVERED includes 'read' (cumulative funnel)


@pytest.mark.asyncio
async def test_relink_preserves_read_status_but_updates_outbox_link(session_maker) -> None:
    """H. Recipient status='read' but outbox_message_id points to a failed row.
    A successful 'delivered' outbox exists for the same job.
    After recompute: outbox_message_id relinked to delivered outbox; status stays 'read'.
    """
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            job = _make_relink_job(session, run_id=run_id, dedupe="relink-h-job")
            await session.flush()

            failed_ob = _make_relink_outbox(session, job_id=job.id, status="failed", provider_message_id=None)
            delivered_ob = _make_relink_outbox(
                session,
                job_id=job.id,
                status="delivered",
                provider_message_id="wamid.h-delivered",
            )
            await session.flush()

            recipient = _make_relink_recipient(
                session,
                run_id=run_id,
                job_id=job.id,
                outbox_id=failed_ob.id,
                status="read",
                provider_message_id=None,
            )
            await session.flush()
            recipient_id = recipient.id
            delivered_ob_id = delivered_ob.id

    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert summary["relinked_recipients_count"] == 1
    assert recip.outbox_message_id == delivered_ob_id, "FK relinked even when status not advanced"
    assert recip.provider_message_id == "wamid.h-delivered"
    assert recip.status == "read", "read must not be downgraded to delivered"


@pytest.mark.asyncio
async def test_recompute_fallback_does_not_backfill_failed_outbox(session_maker) -> None:
    """I. Recipient has outbox_message_id=None and the only outbox is failed.
    After recompute: outbox_message_id stays None — failed rows must not be backfilled.
    """
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            job = _make_relink_job(session, run_id=run_id, dedupe="relink-i-job")
            await session.flush()

            _make_relink_outbox(
                session,
                job_id=job.id,
                status="failed",
                provider_message_id="wamid.i-failed",
            )
            await session.flush()

            recipient = _make_relink_recipient(
                session,
                run_id=run_id,
                job_id=job.id,
                outbox_id=None,
                status="queued",
            )
            await session.flush()
            recipient_id = recipient.id

    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        recip = await session.get(CampaignRecipient, recipient_id)

    assert summary["relinked_recipients_count"] == 0
    assert recip.outbox_message_id is None, "failed outbox must never be backfilled"
    assert recip.provider_message_id is None


@pytest.mark.asyncio
async def test_recompute_fallback_can_backfill_successful_outbox(session_maker) -> None:
    """J. Recipient has outbox_message_id=None and a successful 'delivered' outbox exists.
    After recompute: outbox_message_id is set and status advanced to 'delivered'.
    """
    run_id = await _make_run(session_maker)

    async with session_maker() as session:
        async with session.begin():
            job = _make_relink_job(session, run_id=run_id, dedupe="relink-j-job")
            await session.flush()

            delivered_ob = _make_relink_outbox(
                session,
                job_id=job.id,
                status="delivered",
                provider_message_id="wamid.j-delivered",
            )
            await session.flush()

            recipient = _make_relink_recipient(
                session,
                run_id=run_id,
                job_id=job.id,
                outbox_id=None,
                status="queued",
            )
            await session.flush()
            recipient_id = recipient.id
            delivered_ob_id = delivered_ob.id

    async with session_maker() as session:
        async with session.begin():
            summary = await recompute_campaign_run_stats(session, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        recip = await session.get(CampaignRecipient, recipient_id)

    assert summary["relinked_recipients_count"] == 1
    assert recip.outbox_message_id == delivered_ob_id
    assert recip.provider_message_id == "wamid.j-delivered"
    assert recip.status == "delivered"
    assert run.delivered_count == 1
