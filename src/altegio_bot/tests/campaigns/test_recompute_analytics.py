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
)
from altegio_bot.ops.auth import require_ops_auth

_UTC = timezone.utc
_NOW = datetime(2026, 5, 4, 12, 0, 0, tzinfo=_UTC)
_COMPLETED_AT = datetime(2026, 4, 16, 11, 47, 46, tzinfo=_UTC)


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
    """booked_after_at is set when there is a record/create event in attribution window."""
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
    """Event after run.completed_at but before recipient.sent_at → booked_after_at=None."""
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
            outbox = OutboxMessage(
                company_id=758285,
                phone_e164="+10000000001",
                template_code="newsletter_new_clients_monthly",
                language="de",
                body="Test",
                status="failed",
                scheduled_at=_COMPLETED_AT,
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
async def test_booked_after_null_for_deleted_record(session_maker) -> None:
    """Record with is_deleted=True → booked_after_at=None even with event in window."""
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

    assert run.booked_after_count == 0, "deleted records must not count as booked-after"


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
