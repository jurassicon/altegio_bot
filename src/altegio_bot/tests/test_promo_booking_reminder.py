"""Tests: promo card booking reminder campaign (hardened).

Coverage (per spec):
1.  dry-run selects Laura-like issued active-card lead
2.  dry-run excludes lead with status='issued' but no loyalty_card_id (Fix 1)
3.  dry-run excludes lead with loyalty_card_issued=false (Fix 1)
4.  dry-run includes lead only when all active-card fields present (Fix 1)
5.  dry-run excludes Malika-like manual_review lead
6.  dry-run excludes expired lead
7.  dry-run excludes already-reminded lead
8.  dry-run excludes opted-out phone (exact match)
9.  dry-run excludes opted-out phone when formatting differs (Fix 4)
10. apply creates job and updates PromoLead.meta with job_id/queued_at/template (Fix 6)
11. repeated apply does not create duplicate job; does not corrupt meta (Fix 6)
12. invalid promo_lead_id payload does not raise, does not call provider (Fix 5)
13. outbox handler sends template with params ['15', '25.06.2026', booking_link]
14. outbox cancels when lead.status='booked' (Fix 2)
15. outbox cancels when lead.meta.manual_review_required=true (Fix 2)
16. outbox cancels when lead.applied_at is not null (Fix 2)
17. outbox cancels when lead.meta.booking_reminder_sent_at exists (Fix 2)
18. outbox cancels when active card fields are missing (Fix 2)
19. outbox cancels opted-out phone (normalized, different company row) (Fix 4)
20. outbox cancels when 131026 suppressed (Fix 3)
21. success: OutboxMessage.body is non-empty and contains key strings (Fix 7)
22. success: PromoLead.meta contains sent_at / outbox_id / provider_id / template (Fix 8)
23. expiry display uses promo funnel calendar-month boundary logic (Fix 9)
24. _parse_positive_int_id unit: accepted / rejected cases (Fix 5 strict)
25. bool/float rejected even when PromoLead with matching int() value exists (Fix 5 strict)
26. digit-string id '001' resolves to the matching lead (Fix 5 strict)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from altegio_bot.models.models import (
    Client,
    MessageJob,
    OutboxMessage,
    PromoLead,
    WhatsAppSender,
)
from altegio_bot.workers.promo_lead_handler import PROMO_CARD_BOOKING_REMINDER_JOB_TYPE

_UTC = timezone.utc
_NOW = datetime(2026, 6, 1, 10, 0, 0, tzinfo=_UTC)
_PHONE = "+4917600000001"
_COMPANY = 758285  # KA
_CAMPAIGN = "sommer_2026"
_DISCOUNT = Decimal("15.00")
_EXPIRES = datetime(2026, 6, 25, 12, 0, 0, tzinfo=_UTC)  # noon UTC → 14:00 Berlin → still 25.06
_BOOKING_LINK = "https://n813709.alteg.io/"
_TEMPLATE = "kitilash_ka_promo_card_booking_reminder_v1"

# Active-card meta that passes all guards
_ACTIVE_META: dict = {"loyalty_card_issued": "true"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_lead(
    *,
    phone: str = _PHONE,
    company_id: int = _COMPANY,
    status: str = "issued",
    issued_at: datetime = _NOW - timedelta(days=1),
    expires_at: datetime = _EXPIRES,
    applied_at: datetime | None = None,
    used_at: datetime | None = None,
    cancelled_at: datetime | None = None,
    discount_amount: Decimal = _DISCOUNT,
    loyalty_card_id: str | None = "12345",
    loyalty_card_number: str | None = "CARD-001",
    location_id: int | None = 9001,
    discount_program_id: str | None = "dp_001",
    meta: dict | None = None,
) -> PromoLead:
    return PromoLead(
        company_id=company_id,
        phone_e164=phone,
        campaign_name=_CAMPAIGN,
        secret_code="aktion",
        discount_amount=discount_amount,
        discount_type="fixed",
        status=status,
        issued_at=issued_at,
        expires_at=expires_at,
        applied_at=applied_at,
        used_at=used_at,
        cancelled_at=cancelled_at,
        loyalty_card_id=loyalty_card_id,
        loyalty_card_number=loyalty_card_number,
        location_id=location_id,
        discount_program_id=discount_program_id,
        meta=meta if meta is not None else dict(_ACTIVE_META),
    )


async def _seed_sender(session, *, sender_id: int = 501, company_id: int = _COMPANY) -> None:
    session.add(
        WhatsAppSender(
            id=sender_id,
            company_id=company_id,
            sender_code="default",
            phone_number_id="PNID_PROMO_TEST",
            display_phone="+49",
            is_active=True,
        )
    )
    await session.flush()


# ---------------------------------------------------------------------------
# Fake providers
# ---------------------------------------------------------------------------


class _OkProvider:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def send_template(self, sender_id, phone_e164, template_name, language, params, **kwargs):
        self.calls.append(
            {
                "sender_id": sender_id,
                "phone": phone_e164,
                "template_name": template_name,
                "language": language,
                "params": params,
            }
        )
        return "wamid.PROMO_REMINDER_OK"

    async def send(self, *args, **kwargs):
        raise AssertionError("send() must not be called for template jobs")


class _FailProvider:
    async def send_template(self, *args, **kwargs):
        raise RuntimeError("simulated send failure")

    async def send(self, *args, **kwargs):
        raise AssertionError("unexpected send()")


# ---------------------------------------------------------------------------
# Import helpers
# ---------------------------------------------------------------------------


async def _run_fetch(session):
    from altegio_bot.scripts.enqueue_promo_booking_reminders import _fetch_eligible_leads

    return await _fetch_eligible_leads(session, now=_NOW)


async def _run_enqueue(session, lead, now=_NOW):
    from altegio_bot.scripts.enqueue_promo_booking_reminders import _enqueue_one

    return await _enqueue_one(session, lead, now)


async def _run_handler(session, job, provider):
    from altegio_bot.workers.outbox_worker import _process_promo_card_booking_reminder

    with patch("altegio_bot.workers.outbox_worker.utcnow", return_value=_NOW):
        return await _process_promo_card_booking_reminder(session, job, provider)


def _make_job(lead_id: int, *, job_id_hint: int = 1) -> MessageJob:
    return MessageJob(
        company_id=_COMPANY,
        job_type=PROMO_CARD_BOOKING_REMINDER_JOB_TYPE,
        run_at=_NOW,
        dedupe_key=f"{PROMO_CARD_BOOKING_REMINDER_JOB_TYPE}:{lead_id}",
        max_attempts=3,
        payload={"promo_lead_id": lead_id},
    )


# ===========================================================================
# Script — selection tests
# ===========================================================================


@pytest.mark.asyncio
async def test_dry_run_selects_eligible_lead(session_maker):
    """Active issued lead with all card fields is returned (Laura-like)."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id

    async with session_maker() as session:
        async with session.begin():
            eligible = await _run_fetch(session)

    assert any(item.id == lead_id for item in eligible)


@pytest.mark.asyncio
async def test_dry_run_excludes_lead_without_loyalty_card_id(session_maker):
    """Issued lead with no loyalty_card_id must be excluded (Fix 1)."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(loyalty_card_id=None)
            session.add(lead)
            await session.flush()
            lead_id = lead.id

    async with session_maker() as session:
        async with session.begin():
            eligible = await _run_fetch(session)

    assert not any(item.id == lead_id for item in eligible)


@pytest.mark.asyncio
async def test_dry_run_excludes_lead_with_card_not_issued(session_maker):
    """Issued lead with loyalty_card_issued=false in meta is excluded (Fix 1)."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(meta={"loyalty_card_issued": "false"})
            session.add(lead)
            await session.flush()
            lead_id = lead.id

    async with session_maker() as session:
        async with session.begin():
            eligible = await _run_fetch(session)

    assert not any(item.id == lead_id for item in eligible)


@pytest.mark.asyncio
async def test_dry_run_includes_only_complete_active_card_lead(session_maker):
    """Lead with all active-card fields present is selected; partial fields excluded (Fix 1)."""
    async with session_maker() as session:
        async with session.begin():
            complete = _make_lead()
            incomplete = _make_lead(phone="+4917600000002", discount_program_id=None)
            session.add(complete)
            session.add(incomplete)
            await session.flush()
            complete_id = complete.id
            incomplete_id = incomplete.id

    async with session_maker() as session:
        async with session.begin():
            eligible = await _run_fetch(session)

    ids = {item.id for item in eligible}
    assert complete_id in ids
    assert incomplete_id not in ids


@pytest.mark.asyncio
async def test_dry_run_excludes_manual_review_lead(session_maker):
    """Lead with manual_review_required=true is excluded (Malika-like)."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(meta={**_ACTIVE_META, "manual_review_required": True})
            session.add(lead)
            await session.flush()
            lead_id = lead.id

    async with session_maker() as session:
        async with session.begin():
            eligible = await _run_fetch(session)

    assert not any(item.id == lead_id for item in eligible)


@pytest.mark.asyncio
async def test_dry_run_excludes_expired_lead(session_maker):
    """Lead with expires_at in the past is excluded."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(expires_at=datetime(2020, 1, 1, tzinfo=_UTC))
            session.add(lead)
            await session.flush()
            lead_id = lead.id

    async with session_maker() as session:
        async with session.begin():
            eligible = await _run_fetch(session)

    assert not any(item.id == lead_id for item in eligible)


@pytest.mark.asyncio
async def test_dry_run_expiry_boundary_uses_fixed_clock(session_maker):
    """Lead is eligible only when expires_at is strictly greater than the fixed clock."""
    async with session_maker() as session:
        async with session.begin():
            active = _make_lead(phone="+4917600000101", expires_at=_NOW + timedelta(seconds=1))
            expired = _make_lead(phone="+4917600000102", expires_at=_NOW)
            session.add(active)
            session.add(expired)
            await session.flush()
            active_id = active.id
            expired_id = expired.id

    async with session_maker() as session:
        async with session.begin():
            eligible = await _run_fetch(session)

    ids = {item.id for item in eligible}
    assert active_id in ids
    assert expired_id not in ids


@pytest.mark.asyncio
async def test_dry_run_excludes_already_reminded_lead(session_maker):
    """Lead with booking_reminder_sent_at set in meta is excluded."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(meta={**_ACTIVE_META, "booking_reminder_sent_at": "2026-05-30T10:00:00+00:00"})
            session.add(lead)
            await session.flush()
            lead_id = lead.id

    async with session_maker() as session:
        async with session.begin():
            eligible = await _run_fetch(session)

    assert not any(item.id == lead_id for item in eligible)


@pytest.mark.asyncio
async def test_dry_run_excludes_opted_out_exact(session_maker):
    """Lead whose phone matches exactly an opted-out Client row is excluded."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    id=900,
                    company_id=_COMPANY,
                    altegio_client_id=900,
                    display_name="OptedOut",
                    phone_e164=_PHONE,
                    raw={},
                    wa_opted_out=True,
                )
            )
            lead = _make_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id

    async with session_maker() as session:
        async with session.begin():
            eligible = await _run_fetch(session)

    assert not any(item.id == lead_id for item in eligible)


@pytest.mark.asyncio
async def test_dry_run_excludes_opted_out_normalized_phone(session_maker):
    """Opt-out with different formatting is caught by digit normalization (Fix 4).

    Lead phone: +4917600000001
    Client opted-out phone: 4917600000001 (no +)
    Must still be excluded.
    """
    async with session_maker() as session:
        async with session.begin():
            # Store without leading + to test normalization
            session.add(
                Client(
                    id=901,
                    company_id=_COMPANY,
                    altegio_client_id=901,
                    display_name="OptedOutNorm",
                    phone_e164="4917600000001",  # no leading +
                    raw={},
                    wa_opted_out=True,
                )
            )
            lead = _make_lead()  # phone = "+4917600000001"
            session.add(lead)
            await session.flush()
            lead_id = lead.id

    async with session_maker() as session:
        async with session.begin():
            eligible = await _run_fetch(session)

    assert not any(item.id == lead_id for item in eligible)


# ===========================================================================
# Script — apply / enqueue tests
# ===========================================================================


@pytest.mark.asyncio
async def test_apply_creates_job_and_updates_lead_meta(session_maker):
    """apply creates one MessageJob and writes booking_reminder_* to lead.meta (Fix 6)."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id

    async with session_maker() as session:
        async with session.begin():
            leads = await _run_fetch(session)
            assert len(leads) == 1
            created = await _run_enqueue(session, leads[0])

    assert created is True

    async with session_maker() as session:
        async with session.begin():
            expected_key = f"{PROMO_CARD_BOOKING_REMINDER_JOB_TYPE}:{lead_id}"
            job = await session.scalar(select(MessageJob).where(MessageJob.dedupe_key == expected_key))
            refreshed = await session.get(PromoLead, lead_id)

    assert job is not None
    assert job.job_type == PROMO_CARD_BOOKING_REMINDER_JOB_TYPE
    assert job.payload == {"promo_lead_id": lead_id}
    assert job.company_id == _COMPANY

    meta = refreshed.meta or {}
    assert meta.get("booking_reminder_job_id") == job.id
    assert "booking_reminder_queued_at" in meta
    assert meta.get("booking_reminder_template") == _TEMPLATE


@pytest.mark.asyncio
async def test_apply_idempotent_no_duplicate_no_meta_corruption(session_maker):
    """Second apply call does not create a duplicate job and does not corrupt meta (Fix 6)."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id

    async with session_maker() as session:
        async with session.begin():
            leads = await _run_fetch(session)
            created_first = await _run_enqueue(session, leads[0])

    async with session_maker() as session:
        async with session.begin():
            all_leads = (
                (await session.execute(select(PromoLead).where(PromoLead.campaign_name == _CAMPAIGN))).scalars().all()
            )
            created_second = await _run_enqueue(session, all_leads[0])

    assert created_first is True
    assert created_second is False

    async with session_maker() as session:
        async with session.begin():
            jobs = (
                (
                    await session.execute(
                        select(MessageJob).where(MessageJob.job_type == PROMO_CARD_BOOKING_REMINDER_JOB_TYPE)
                    )
                )
                .scalars()
                .all()
            )
            lead_row = await session.get(PromoLead, lead_id)

    assert len(jobs) == 1
    meta = lead_row.meta or {}
    assert "booking_reminder_job_id" in meta
    assert "booking_reminder_sent_at" not in meta  # must NOT be set at enqueue time


# ===========================================================================
# Outbox handler — payload / eligibility tests
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_payload",
    [
        # missing key
        {},
        # explicit None
        {"promo_lead_id": None},
        # non-digit string
        {"promo_lead_id": "abc"},
        # bool — must NOT be coerced via int(True)==1
        {"promo_lead_id": True},
        {"promo_lead_id": False},
        # non-positive int
        {"promo_lead_id": -1},
        {"promo_lead_id": 0},
        # float — must NOT be coerced via int(1.5)==1
        {"promo_lead_id": 1.5},
        # string with decimal point
        {"promo_lead_id": "1.0"},
        # string with leading/trailing whitespace (int(' 1 ')==1 in plain Python)
        {"promo_lead_id": " 1 "},
        # signed string
        {"promo_lead_id": "+1"},
        {"promo_lead_id": "-1"},
        # zero digit string
        {"promo_lead_id": "0"},
        # unsupported container types
        {"promo_lead_id": []},
        {"promo_lead_id": {}},
    ],
)
async def test_handler_invalid_payload_does_not_crash(session_maker, bad_payload):
    """Invalid or missing promo_lead_id: job fails, provider never called (Fix 5)."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_sender(session)

    provider = _OkProvider()

    async with session_maker() as session:
        async with session.begin():
            job = MessageJob(
                company_id=_COMPANY,
                job_type=PROMO_CARD_BOOKING_REMINDER_JOB_TYPE,
                run_at=_NOW,
                dedupe_key=f"{PROMO_CARD_BOOKING_REMINDER_JOB_TYPE}:bad:{id(bad_payload)}",
                max_attempts=3,
                payload=bad_payload,
            )
            session.add(job)
            await session.flush()

            await _run_handler(session, job, provider)

    assert job.status == "failed"
    assert len(provider.calls) == 0


@pytest.mark.asyncio
async def test_handler_sends_template_with_correct_params(session_maker):
    """Handler sends template with params ['15', '25.06.2026', booking_link]."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(expires_at=datetime(2026, 6, 25, 12, 0, 0, tzinfo=_UTC))
            session.add(lead)
            await session.flush()
            lead_id = lead.id
            await _seed_sender(session)

    provider = _OkProvider()

    async with session_maker() as session:
        async with session.begin():
            job = _make_job(lead_id)
            session.add(job)
            await session.flush()

            with patch("altegio_bot.workers.outbox_worker._apply_rate_limit", new=AsyncMock(return_value=None)):
                await _run_handler(session, job, provider)

    assert job.status == "done"
    assert len(provider.calls) == 1
    call = provider.calls[0]
    assert call["template_name"] == _TEMPLATE
    assert call["language"] == "de"
    assert call["params"] == ["15", "25.06.2026", _BOOKING_LINK]


@pytest.mark.asyncio
async def test_handler_cancels_if_lead_booked(session_maker):
    """Handler cancels before send when lead.status='booked' (Fix 2)."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(status="booked")
            session.add(lead)
            await session.flush()
            lead_id = lead.id
            await _seed_sender(session)

    provider = _OkProvider()

    async with session_maker() as session:
        async with session.begin():
            job = _make_job(lead_id)
            session.add(job)
            await session.flush()
            await _run_handler(session, job, provider)

    assert job.status == "canceled"
    assert len(provider.calls) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_status", ["applied", "used", "cancelled"])
async def test_handler_cancels_if_lead_terminal(session_maker, terminal_status):
    """Handler cancels before send when lead is in a terminal status (Fix 2)."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(status=terminal_status)
            session.add(lead)
            await session.flush()
            lead_id = lead.id
            await _seed_sender(session)

    provider = _OkProvider()

    async with session_maker() as session:
        async with session.begin():
            job = _make_job(lead_id)
            session.add(job)
            await session.flush()
            await _run_handler(session, job, provider)

    assert job.status == "canceled"
    assert len(provider.calls) == 0


@pytest.mark.asyncio
async def test_handler_cancels_if_manual_review_required(session_maker):
    """Handler cancels when lead.meta.manual_review_required=True (Fix 2)."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(meta={**_ACTIVE_META, "manual_review_required": True})
            session.add(lead)
            await session.flush()
            lead_id = lead.id
            await _seed_sender(session)

    provider = _OkProvider()

    async with session_maker() as session:
        async with session.begin():
            job = _make_job(lead_id)
            session.add(job)
            await session.flush()

            with patch("altegio_bot.workers.outbox_worker._apply_rate_limit", new=AsyncMock(return_value=None)):
                await _run_handler(session, job, provider)

    assert job.status == "canceled"
    assert len(provider.calls) == 0


@pytest.mark.asyncio
async def test_handler_cancels_if_applied_at_set(session_maker):
    """Handler cancels when lead.applied_at is not None (Fix 2)."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(applied_at=_NOW - timedelta(hours=1))
            session.add(lead)
            await session.flush()
            lead_id = lead.id
            await _seed_sender(session)

    provider = _OkProvider()

    async with session_maker() as session:
        async with session.begin():
            job = _make_job(lead_id)
            session.add(job)
            await session.flush()

            with patch("altegio_bot.workers.outbox_worker._apply_rate_limit", new=AsyncMock(return_value=None)):
                await _run_handler(session, job, provider)

    assert job.status == "canceled"
    assert len(provider.calls) == 0


@pytest.mark.asyncio
async def test_handler_cancels_if_reminder_already_sent(session_maker):
    """Handler cancels when lead.meta.booking_reminder_sent_at exists (Fix 2)."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(meta={**_ACTIVE_META, "booking_reminder_sent_at": "2026-05-31T10:00:00+00:00"})
            session.add(lead)
            await session.flush()
            lead_id = lead.id
            await _seed_sender(session)

    provider = _OkProvider()

    async with session_maker() as session:
        async with session.begin():
            job = _make_job(lead_id)
            session.add(job)
            await session.flush()

            with patch("altegio_bot.workers.outbox_worker._apply_rate_limit", new=AsyncMock(return_value=None)):
                await _run_handler(session, job, provider)

    assert job.status == "canceled"
    assert len(provider.calls) == 0


@pytest.mark.asyncio
async def test_handler_cancels_if_active_card_missing(session_maker):
    """Handler cancels when loyalty_card_id is None (active card not issued) (Fix 2)."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(loyalty_card_id=None)
            session.add(lead)
            await session.flush()
            lead_id = lead.id
            await _seed_sender(session)

    provider = _OkProvider()

    async with session_maker() as session:
        async with session.begin():
            job = _make_job(lead_id)
            session.add(job)
            await session.flush()

            with patch("altegio_bot.workers.outbox_worker._apply_rate_limit", new=AsyncMock(return_value=None)):
                await _run_handler(session, job, provider)

    assert job.status == "canceled"
    assert job.last_error is not None
    assert "active loyalty card missing" in job.last_error
    assert len(provider.calls) == 0


@pytest.mark.asyncio
async def test_handler_cancels_opted_out_normalized_other_company(session_maker):
    """Handler cancels if opt-out exists for same phone in another company (Fix 4).

    Phone formats differ: lead has '+4917600000001', client row has '4917600000001'.
    """
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    id=902,
                    company_id=9999,  # different company
                    altegio_client_id=902,
                    display_name="OptedOutOtherCo",
                    phone_e164="4917600000001",  # no leading +
                    raw={},
                    wa_opted_out=True,
                )
            )
            lead = _make_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id
            await _seed_sender(session)

    provider = _OkProvider()

    async with session_maker() as session:
        async with session.begin():
            job = _make_job(lead_id)
            session.add(job)
            await session.flush()

            with patch("altegio_bot.workers.outbox_worker._apply_rate_limit", new=AsyncMock(return_value=None)):
                await _run_handler(session, job, provider)

    assert job.status == "canceled"
    assert len(provider.calls) == 0


@pytest.mark.asyncio
async def test_handler_131026_suppression(session_maker):
    """Handler cancels and creates audit OutboxMessage when 131026 threshold reached (Fix 3)."""
    from altegio_bot.workers import outbox_worker as ow

    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id
            await _seed_sender(session)

    provider = _OkProvider()

    async with session_maker() as session:
        async with session.begin():
            job = _make_job(lead_id)
            session.add(job)
            await session.flush()

            with (
                patch("altegio_bot.workers.outbox_worker._apply_rate_limit", new=AsyncMock(return_value=None)),
                patch.object(ow.settings, "wa_131026_suppression_enabled", True),
                patch.object(ow.settings, "wa_131026_suppression_threshold", 2),
                patch.object(ow.settings, "wa_131026_suppression_window_days", 14),
                patch("altegio_bot.workers.outbox_worker._count_131026_failures", new=AsyncMock(return_value=3)),
            ):
                await _run_handler(session, job, provider)

    assert job.status == "canceled"
    assert job.last_error is not None
    assert job.last_error.startswith("suppressed_131026")
    assert len(provider.calls) == 0

    async with session_maker() as session:
        async with session.begin():
            outbox = await session.scalar(select(OutboxMessage).where(OutboxMessage.job_id == job.id))

    assert outbox is not None
    assert outbox.status == "canceled"
    assert (outbox.error or "").startswith("suppressed_131026")
    assert outbox.meta.get("suppression_code") == "131026"


# ===========================================================================
# Outbox handler — success output tests
# ===========================================================================


@pytest.mark.asyncio
async def test_handler_success_outbox_body_nonempty(session_maker):
    """Success OutboxMessage.body is non-empty and contains key strings (Fix 7)."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id
            await _seed_sender(session)

    provider = _OkProvider()

    async with session_maker() as session:
        async with session.begin():
            job = _make_job(lead_id)
            session.add(job)
            await session.flush()

            with patch("altegio_bot.workers.outbox_worker._apply_rate_limit", new=AsyncMock(return_value=None)):
                await _run_handler(session, job, provider)

    async with session_maker() as session:
        async with session.begin():
            outbox = await session.scalar(select(OutboxMessage).where(OutboxMessage.job_id == job.id))

    assert outbox is not None
    assert outbox.status == "sent"
    assert outbox.body  # non-empty
    assert "Sommer-Aktion" in outbox.body
    assert "15" in outbox.body
    assert _BOOKING_LINK in outbox.body


@pytest.mark.asyncio
async def test_handler_success_lead_meta_complete(session_maker):
    """Success sets all required meta fields on PromoLead (Fix 8)."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id
            await _seed_sender(session)

    provider = _OkProvider()

    async with session_maker() as session:
        async with session.begin():
            job = _make_job(lead_id)
            session.add(job)
            await session.flush()

            with patch("altegio_bot.workers.outbox_worker._apply_rate_limit", new=AsyncMock(return_value=None)):
                await _run_handler(session, job, provider)

    assert job.status == "done"

    async with session_maker() as session:
        async with session.begin():
            lead_row = await session.get(PromoLead, lead_id)
            outbox = await session.scalar(select(OutboxMessage).where(OutboxMessage.job_id == job.id))

    meta = lead_row.meta or {}
    assert "booking_reminder_sent_at" in meta
    assert "booking_reminder_outbox_id" in meta
    assert "booking_reminder_provider_message_id" in meta
    assert meta.get("booking_reminder_template") == _TEMPLATE
    assert meta["booking_reminder_outbox_id"] == outbox.id
    assert meta["booking_reminder_provider_message_id"] == "wamid.PROMO_REMINDER_OK"


@pytest.mark.asyncio
async def test_handler_expiry_display_calendar_month_boundary(session_maker):
    """expires_at on day=1 midnight UTC shows last day of prev month (Fix 9)."""
    # expires_at = 2026-07-01 00:00:00 UTC → display = 30.06.2026 (last valid day)
    calendar_boundary = datetime(2026, 7, 1, 0, 0, 0, tzinfo=_UTC)

    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(expires_at=calendar_boundary)
            session.add(lead)
            await session.flush()
            lead_id = lead.id
            await _seed_sender(session)

    provider = _OkProvider()

    async with session_maker() as session:
        async with session.begin():
            job = _make_job(lead_id)
            session.add(job)
            await session.flush()

            with patch("altegio_bot.workers.outbox_worker._apply_rate_limit", new=AsyncMock(return_value=None)):
                await _run_handler(session, job, provider)

    assert len(provider.calls) == 1
    # param[1] = expires_at_display — must be 30.06.2026, not 01.07.2026
    assert provider.calls[0]["params"][1] == "30.06.2026"


# ===========================================================================
# _parse_positive_int_id — unit tests (Fix 5 strict)
# ===========================================================================


@pytest.mark.parametrize(
    "value,expected_id,expect_error",
    [
        # ---- accepted ----
        (1, 1, False),
        (42, 42, False),
        ("1", 1, False),
        ("42", 42, False),
        ("001", 1, False),  # digit-only with leading zeros → valid
        # ---- rejected: bool (subclass of int, must be caught before int check) ----
        (True, None, True),
        (False, None, True),
        # ---- rejected: non-positive int ----
        (0, None, True),
        (-1, None, True),
        # ---- rejected: float ----
        (1.0, None, True),
        (1.5, None, True),
        # ---- rejected: strings that bare int() would accept ----
        ("1.0", None, True),
        (" 1 ", None, True),
        ("+1", None, True),
        ("-1", None, True),
        ("0", None, True),
        ("", None, True),
        ("abc", None, True),
        # ---- rejected: containers ----
        ([], None, True),
        ({}, None, True),
        # ---- absent (None) — not an error, just missing ----
        (None, None, False),
    ],
)
def test_parse_positive_int_id_unit(value, expected_id, expect_error):
    """_parse_positive_int_id accepts/rejects values per strict rules (Fix 5 strict)."""
    from altegio_bot.workers.outbox_worker import _parse_positive_int_id

    result_id, error = _parse_positive_int_id(value, "test_field")

    assert result_id == expected_id, f"value={value!r}: expected id {expected_id}, got {result_id}"
    if expect_error:
        assert error is not None, f"value={value!r}: expected an error string, got None"
    else:
        assert error is None, f"value={value!r}: expected no error, got {error!r}"


@pytest.mark.asyncio
async def test_handler_bool_and_float_rejected_not_treated_as_id(session_maker):
    """bool/float payload fails before DB lookup — they are NOT silently cast to int.

    Regression guard: bare ``int(True) == 1`` and ``int(1.5) == 1`` would
    load a PromoLead with id=1 if one exists.  The strict helper rejects
    them at the type-check level so PromoLead is never queried.
    """
    async with session_maker() as session:
        async with session.begin():
            # Seed a lead — its auto-assigned id is the first candidate a
            # buggy int(True)/int(1.5) resolution would wrongly load.
            lead = _make_lead()
            session.add(lead)
            await session.flush()
            await _seed_sender(session)

    for bad_value in (True, False, 1.5, 1.0):
        provider = _OkProvider()
        async with session_maker() as session:
            async with session.begin():
                job = MessageJob(
                    company_id=_COMPANY,
                    job_type=PROMO_CARD_BOOKING_REMINDER_JOB_TYPE,
                    run_at=_NOW,
                    dedupe_key=f"{PROMO_CARD_BOOKING_REMINDER_JOB_TYPE}:strict:{id(bad_value)}{bad_value}",
                    max_attempts=3,
                    payload={"promo_lead_id": bad_value},
                )
                session.add(job)
                await session.flush()
                await _run_handler(session, job, provider)

        assert job.status == "failed", f"Expected 'failed' for promo_lead_id={bad_value!r}, got {job.status!r}"
        assert len(provider.calls) == 0, f"Provider must not be called for promo_lead_id={bad_value!r}"


@pytest.mark.asyncio
async def test_handler_digit_string_id_resolves_correct_lead(session_maker):
    """A zero-padded digit-string id (e.g. '001') resolves to the matching PromoLead.

    This verifies that valid digit-only strings are accepted by the strict
    parser and that the handler proceeds to send the template normally.
    """
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(expires_at=datetime(2026, 6, 25, 12, 0, 0, tzinfo=_UTC))
            session.add(lead)
            await session.flush()
            lead_id = lead.id
            await _seed_sender(session)

    provider = _OkProvider()

    async with session_maker() as session:
        async with session.begin():
            # Zero-pad to at least 3 digits so we always test the leading-zero path.
            str_id = str(lead_id).zfill(3)  # e.g. lead_id=1 → '001', lead_id=42 → '042'
            job = MessageJob(
                company_id=_COMPANY,
                job_type=PROMO_CARD_BOOKING_REMINDER_JOB_TYPE,
                run_at=_NOW,
                dedupe_key=f"{PROMO_CARD_BOOKING_REMINDER_JOB_TYPE}:str_id:{lead_id}",
                max_attempts=3,
                payload={"promo_lead_id": str_id},
            )
            session.add(job)
            await session.flush()

            with patch("altegio_bot.workers.outbox_worker._apply_rate_limit", new=AsyncMock(return_value=None)):
                await _run_handler(session, job, provider)

    assert job.status == "done", f"Expected 'done' for digit-string id {str_id!r}, got {job.status!r}"
    assert len(provider.calls) == 1
