"""Tests: promo service block in replies and promo_apply_existing_booking job.

Covers:
A.  build_reply_issued_with_card() includes service block when setting is non-empty.
B.  Empty promo_allowed_services_display_text → no service block in any issued reply.
C.  build_reply_issued() and build_reply_already_issued() also include the block.
D.  handle_promo_command() creates promo_apply_existing_booking job for new issued lead.
E.  process_promo_eligibility_check_job() creates job after decision_status='issued'.
F.  process_promo_apply_existing_booking_job: existing booking predating promo,
    allowed service → discount applied (allow_existing_booking_before_promo flag).
G.  process_promo_apply_existing_booking_job: no future booking → done,
    meta.existing_booking_skip_reason='no_future_booking'.
H.  process_promo_apply_existing_booking_job: future booking but service not allowed
    → done, no Altegio API calls.
I.  process_promo_apply_existing_booking_job: ambiguous candidates (same starts_at)
    → fail-closed, meta.existing_booking_skip_reason='ambiguous_candidates'.
J.  _ensure_promo_apply_existing_booking_job is idempotent: two calls → one job.
K.  process_promo_apply_existing_booking_job skips non-issued leads (applied/booked/expired).
L.  handle_promo_command does not call Altegio record-search or PUT APIs — job
    is created without proactively searching for existing bookings.
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
    PromoLead,
    Record,
    RecordService,
    WhatsAppEvent,
    WhatsAppSender,
)
from altegio_bot.promo_discount_apply import process_promo_apply_existing_booking_job
from altegio_bot.settings import settings
from altegio_bot.workers import outbox_worker as ow
from altegio_bot.workers.promo_lead_handler import (
    PROMO_APPLY_EXISTING_BOOKING_JOB_TYPE,
    PROMO_ELIGIBILITY_CHECK_JOB_TYPE,
    _ensure_promo_apply_existing_booking_job,
    build_reply_already_issued,
    build_reply_issued,
    build_reply_issued_with_card,
)
from altegio_bot.workers.whatsapp_inbox_worker import handle_event

_UTC = timezone.utc
_NOW = datetime(2026, 5, 16, 12, 0, 0, tzinfo=_UTC)
_FUTURE = datetime(2099, 1, 1, tzinfo=_UTC)
_PHONE = "+4916099887711"
_COMPANY = 1
_LOCATION = 9001
_CARD_ID = "555"
_PROGRAM_ID = "dp_001"
_ALLOWED_SERVICE = 12345
_OTHER_SERVICE = 99999

PHONE_NUMBER_ID = "PNID_EXISTING_BOOKING"
FROM_PHONE = "4916099887711"
CAMPAIGN = "welcome_discount"


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _enable_promo_funnel():
    with (
        patch.object(settings, "promo_lead_funnel_enabled", True),
        patch.object(settings, "promo_check_new_client_in_altegio", False),
        patch.object(settings, "promo_async_eligibility_check_enabled", False),
        patch.object(settings, "promo_allowed_services_display_text", ""),
    ):
        yield


class _CaptureProvider:
    wamid = "wamid.EXISTING_BOOKING_TEST"

    def __init__(self) -> None:
        self.sent: list[tuple[int, str, str]] = []

    async def send(self, sender_id, phone_e164, text, contact_name=None):
        self.sent.append((sender_id, phone_e164, text))
        return self.wamid

    async def send_template(self, *args, **kwargs):
        pass


class _FakeCW:
    async def log_incoming_message(self, phone, text, contact_name=None):
        pass

    async def aclose(self):
        pass


def _inbound_payload(phone_number_id, from_phone, text):
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {"phone_number_id": phone_number_id},
                            "messages": [
                                {
                                    "from": from_phone,
                                    "id": "wamid.INBOUND",
                                    "timestamp": "1700000000",
                                    "type": "text",
                                    "text": {"body": text},
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


async def _setup_sender(session, *, sender_id: int = 401, company_id: int = _COMPANY) -> None:
    session.add(
        WhatsAppSender(
            id=sender_id,
            company_id=company_id,
            sender_code="default",
            phone_number_id=PHONE_NUMBER_ID,
            display_phone="+49",
            is_active=True,
        )
    )
    await session.flush()


async def _seed_client(session, *, client_id: int = 200, phone: str = _PHONE) -> Client:
    c = Client(
        id=client_id,
        company_id=_COMPANY,
        altegio_client_id=client_id,
        phone_e164=phone,
        display_name="Promo Test Kunde",
        raw={},
    )
    session.add(c)
    await session.flush()
    return c


async def _seed_record(
    session,
    *,
    record_id: int = 300,
    altegio_record_id: int = 8001,
    client_id: int = 200,
    starts_at: datetime | None = None,
    attendance: int | None = None,
    visit_attendance: int | None = None,
    company_id: int = _COMPANY,
) -> Record:
    r = Record(
        id=record_id,
        company_id=company_id,
        altegio_record_id=altegio_record_id,
        client_id=client_id,
        altegio_client_id=client_id,
        is_deleted=False,
        starts_at=starts_at,
        attendance=attendance,
        visit_attendance=visit_attendance,
        raw={},
    )
    session.add(r)
    await session.flush()
    return r


async def _seed_service(session, *, record_id: int = 300, service_id: int = _ALLOWED_SERVICE) -> None:
    session.add(RecordService(record_id=record_id, service_id=service_id, title="Haarschnitt", raw={}))
    await session.flush()


def _make_lead(
    *,
    phone: str = _PHONE,
    company_id: int = _COMPANY,
    status: str = "issued",
    issued_at: datetime = datetime(2026, 1, 1, tzinfo=_UTC),
    expires_at: datetime = _FUTURE,
    loyalty_card_id: str | None = _CARD_ID,
    location_id: int | None = _LOCATION,
    discount_program_id: str | None = _PROGRAM_ID,
    meta: dict | None = None,
) -> PromoLead:
    return PromoLead(
        company_id=company_id,
        phone_e164=phone,
        campaign_name=CAMPAIGN,
        secret_code="aktion",
        discount_amount=Decimal("15"),
        discount_type="fixed",
        status=status,
        issued_at=issued_at,
        expires_at=expires_at,
        loyalty_card_id=loyalty_card_id,
        location_id=location_id,
        discount_program_id=discount_program_id,
        meta=meta if meta is not None else {"loyalty_card_issued": True},
    )


def _base_settings_ctx(**overrides):
    import contextlib

    defaults = {
        "promo_apply_discount_enabled": True,
        "promo_apply_discount_api_verified": True,
        "promo_allowed_service_ids": str(_ALLOWED_SERVICE),
        "promo_apply_mode": "record_price_override",
    }
    defaults.update(overrides)
    patches = [patch.object(settings, k, v) for k, v in defaults.items()]

    @contextlib.contextmanager
    def _ctx():
        import contextlib as _cl

        with _cl.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            yield

    return _ctx()


def _make_existing_booking_job(lead_id: int, *, job_id: int = 500) -> MessageJob:
    return MessageJob(
        id=job_id,
        company_id=_COMPANY,
        record_id=None,
        client_id=None,
        job_type=PROMO_APPLY_EXISTING_BOOKING_JOB_TYPE,
        run_at=_NOW,
        dedupe_key=f"{PROMO_APPLY_EXISTING_BOOKING_JOB_TYPE}:{lead_id}",
        max_attempts=3,
        payload={"promo_lead_id": lead_id},
    )


# =============================================================================
# A. Service block in build_reply_issued_with_card
# =============================================================================


def test_service_block_in_issued_with_card_reply() -> None:
    """Test A: build_reply_issued_with_card includes service block when setting is set."""
    expires_at = datetime(2026, 6, 30, tzinfo=_UTC)
    with patch.object(settings, "promo_allowed_services_display_text", "Haarschnitt, Coloration"):
        reply = build_reply_issued_with_card(
            expires_at,
            "https://example.com/book",
            Decimal("15"),
            "fixed",
            "CARD42",
        )
    assert "Der Rabatt gilt für folgende Leistungen:" in reply
    assert "Haarschnitt, Coloration" in reply
    # Block must appear before the booking link.
    assert reply.index("Leistungen:") < reply.index("Termin buchen:")


# =============================================================================
# B. Empty setting → no service block
# =============================================================================


def test_no_service_block_when_setting_empty() -> None:
    """Test B: empty promo_allowed_services_display_text → no service block."""
    expires_at = datetime(2026, 6, 30, tzinfo=_UTC)
    for fn in (
        lambda: build_reply_issued(expires_at, "https://example.com", Decimal("15"), "fixed"),
        lambda: build_reply_already_issued(expires_at, "https://example.com"),
        lambda: build_reply_issued_with_card(expires_at, "https://example.com", Decimal("15"), "fixed", "C1"),
    ):
        with patch.object(settings, "promo_allowed_services_display_text", ""):
            reply = fn()
        assert "Leistungen" not in reply


def test_no_service_block_when_setting_whitespace_only() -> None:
    """Whitespace-only setting is treated as empty."""
    expires_at = datetime(2026, 6, 30, tzinfo=_UTC)
    with patch.object(settings, "promo_allowed_services_display_text", "   \t  "):
        reply = build_reply_issued(expires_at, "https://example.com", Decimal("15"), "fixed")
    assert "Leistungen" not in reply


# =============================================================================
# C. build_reply_issued and build_reply_already_issued also include block
# =============================================================================


def test_service_block_in_issued_and_already_issued_replies() -> None:
    """Test C: service block appears in build_reply_issued and build_reply_already_issued."""
    expires_at = datetime(2026, 6, 30, tzinfo=_UTC)
    services_text = "Haarschnitt, Keratin"
    with patch.object(settings, "promo_allowed_services_display_text", services_text):
        r_issued = build_reply_issued(expires_at, "https://example.com", Decimal("15"), "fixed")
        r_already = build_reply_already_issued(expires_at, "https://example.com")

    for reply in (r_issued, r_already):
        assert "Der Rabatt gilt für folgende Leistungen:" in reply
        assert services_text in reply
        assert reply.index("Leistungen:") < reply.index("Termin buchen:")


# =============================================================================
# D. handle_promo_command creates promo_apply_existing_booking job
# =============================================================================


@pytest.mark.asyncio
async def test_handle_promo_command_creates_existing_booking_job(session_maker) -> None:
    """Test D: handle_promo_command enqueues promo_apply_existing_booking for new issued lead."""
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            await _setup_sender(session)

            evt = WhatsAppEvent(
                dedupe_key="wa:existing-booking-D-1",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_inbound_payload(PHONE_NUMBER_ID, FROM_PHONE, "aktion"),
            )
            session.add(evt)
            await session.flush()

            with patch(
                "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                return_value=_FakeCW(),
            ):
                await handle_event(session, evt, provider)

    async with session_maker() as s:
        jobs = list(
            (await s.execute(select(MessageJob).where(MessageJob.job_type == PROMO_APPLY_EXISTING_BOOKING_JOB_TYPE)))
            .scalars()
            .all()
        )
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()

    assert lead is not None, "PromoLead must be created"
    assert lead.status == "issued"
    assert len(jobs) == 1, "exactly one promo_apply_existing_booking job must be created"
    job = jobs[0]
    assert job.payload.get("promo_lead_id") == lead.id
    assert job.dedupe_key == f"{PROMO_APPLY_EXISTING_BOOKING_JOB_TYPE}:{lead.id}"
    assert job.max_attempts == 3


# =============================================================================
# E. process_promo_eligibility_check_job creates job after decision_status='issued'
# =============================================================================


@pytest.mark.asyncio
async def test_eligibility_check_job_creates_existing_booking_job(session_maker) -> None:
    """Test E: process_promo_eligibility_check_job enqueues existing-booking job on 'issued'."""
    provider = _CaptureProvider()

    # Step 1: fire promo with async eligibility check enabled → creates pending_check lead + job.
    with patch.object(settings, "promo_async_eligibility_check_enabled", True):
        async with session_maker() as session:
            async with session.begin():
                await _setup_sender(session)

                evt = WhatsAppEvent(
                    dedupe_key="wa:existing-booking-E-1",
                    status="received",
                    error=None,
                    query={},
                    headers={},
                    payload=_inbound_payload(PHONE_NUMBER_ID, FROM_PHONE, "aktion"),
                )
                session.add(evt)
                await session.flush()

                with patch(
                    "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                    return_value=_FakeCW(),
                ):
                    await handle_event(session, evt, provider)

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        elig_job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == PROMO_ELIGIBILITY_CHECK_JOB_TYPE))
        ).scalar_one()

    assert lead.status == "pending_check"

    # Step 2: process the eligibility check job → should issue lead and create existing-booking job.
    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, elig_job.id, provider)

    async with session_maker() as s:
        refreshed_lead = await s.get(PromoLead, lead.id)
        eb_jobs = list(
            (await s.execute(select(MessageJob).where(MessageJob.job_type == PROMO_APPLY_EXISTING_BOOKING_JOB_TYPE)))
            .scalars()
            .all()
        )

    assert refreshed_lead is not None
    assert refreshed_lead.status == "issued"
    assert len(eb_jobs) == 1, "exactly one promo_apply_existing_booking job must be created"
    assert eb_jobs[0].payload.get("promo_lead_id") == lead.id


# =============================================================================
# F. Existing booking before promo → applied (allow_existing_booking_before_promo)
# =============================================================================


@pytest.mark.asyncio
async def test_promo_apply_existing_booking_applies_discount(session_maker) -> None:
    """Test F: booking starts_at predates promo issued_at, allowed service → applied."""
    # Set issued_at far in the future so starts_at (which is also in the future
    # but earlier) is before issued_at — exercising the promo timestamp guard bypass.
    _ISSUED_AT = datetime(2099, 6, 1, 0, 0, tzinfo=_UTC)
    _STARTS_AT = datetime(2099, 5, 1, 0, 0, tzinfo=_UTC)  # future but < issued_at

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session, starts_at=_STARTS_AT)
            await _seed_service(session)

            lead = _make_lead(issued_at=_ISSUED_AT, expires_at=datetime(2199, 1, 1, tzinfo=_UTC))
            session.add(lead)
            await session.flush()

            job = _make_existing_booking_job(lead.id)
            session.add(job)
            await session.flush()

            fake_altegio_record = {
                "attendance": 0,
                "visit_attendance": 0,
                "services": [{"id": _ALLOWED_SERVICE, "cost": 100.0, "manual_cost": None}],
                "comment": "",
            }
            fake_put_result = {"data": {"services": [{"id": _ALLOWED_SERVICE, "discount": 15.0}]}}

            with (
                _base_settings_ctx(),
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    AsyncMock(return_value=fake_altegio_record),
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    AsyncMock(return_value=fake_put_result),
                ),
            ):
                await process_promo_apply_existing_booking_job(session, job)

            lead_id = lead.id

    async with session_maker() as s:
        refreshed_lead = await s.get(PromoLead, lead_id)

    assert job.status == "done"
    assert refreshed_lead is not None
    assert refreshed_lead.status == "applied"
    meta = refreshed_lead.meta or {}
    assert meta.get("booking_created_before_promo_allowed") is True


# =============================================================================
# G. No future booking → done, no_future_booking
# =============================================================================


@pytest.mark.asyncio
async def test_promo_apply_existing_booking_no_future_record(session_maker) -> None:
    """Test G: no future record for phone → job done, meta.existing_booking_skip_reason set."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            job = _make_existing_booking_job(lead.id)
            session.add(job)
            await session.flush()

            with _base_settings_ctx():
                await process_promo_apply_existing_booking_job(session, job)

            lead_id = lead.id

    async with session_maker() as s:
        refreshed_lead = await s.get(PromoLead, lead_id)

    assert job.status == "done"
    assert job.last_error is None
    assert refreshed_lead is not None
    # Lead stays issued — no record found to apply against.
    assert refreshed_lead.status == "issued"
    meta = refreshed_lead.meta or {}
    assert meta.get("existing_booking_skip_reason") == "no_future_booking"


# =============================================================================
# H. Future booking, service not allowed → done, no external API
# =============================================================================


@pytest.mark.asyncio
async def test_promo_apply_existing_booking_service_not_allowed(session_maker) -> None:
    """Test H: future record exists but service not in allowlist → skip, no API call."""
    _STARTS_AT = _NOW + timedelta(days=7)

    fetch_mock = AsyncMock()

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session, starts_at=_STARTS_AT)
            await _seed_service(session, service_id=_OTHER_SERVICE)  # non-allowed service

            lead = _make_lead()
            session.add(lead)
            await session.flush()

            job = _make_existing_booking_job(lead.id)
            session.add(job)
            await session.flush()

            with (
                _base_settings_ctx(),
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    fetch_mock,
                ),
            ):
                await process_promo_apply_existing_booking_job(session, job)

            lead_id = lead.id

    async with session_maker() as s:
        refreshed_lead = await s.get(PromoLead, lead_id)

    fetch_mock.assert_not_called()
    assert job.status == "done"
    assert refreshed_lead is not None
    assert refreshed_lead.status == "issued"
    meta = refreshed_lead.meta or {}
    assert meta.get("existing_booking_skip_reason") == "service_not_allowed"


# =============================================================================
# I. Ambiguous candidates (same starts_at) → fail-closed
# =============================================================================


@pytest.mark.asyncio
async def test_promo_apply_existing_booking_ambiguous_candidates(session_maker) -> None:
    """Test I: two future records with identical starts_at → ambiguous → fail-closed."""
    _STARTS_AT = _NOW + timedelta(days=5)

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            # Two records with the same starts_at.
            await _seed_record(session, record_id=301, altegio_record_id=8001, starts_at=_STARTS_AT)
            await _seed_service(session, record_id=301, service_id=_ALLOWED_SERVICE)
            await _seed_record(session, record_id=302, altegio_record_id=8002, starts_at=_STARTS_AT)
            await _seed_service(session, record_id=302, service_id=_ALLOWED_SERVICE)

            lead = _make_lead()
            session.add(lead)
            await session.flush()

            job = _make_existing_booking_job(lead.id)
            session.add(job)
            await session.flush()

            with _base_settings_ctx():
                await process_promo_apply_existing_booking_job(session, job)

            lead_id = lead.id

    async with session_maker() as s:
        refreshed_lead = await s.get(PromoLead, lead_id)

    assert job.status == "done"
    assert refreshed_lead is not None
    assert refreshed_lead.status == "issued"
    meta = refreshed_lead.meta or {}
    assert meta.get("existing_booking_skip_reason") == "ambiguous_candidates"


# =============================================================================
# J. Idempotency: two enqueue calls → one job
# =============================================================================


@pytest.mark.asyncio
async def test_ensure_existing_booking_job_idempotent(session_maker) -> None:
    """Test J: calling _ensure_promo_apply_existing_booking_job twice creates only one job."""
    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            now = _NOW
            await _ensure_promo_apply_existing_booking_job(session, lead, now)
            await _ensure_promo_apply_existing_booking_job(session, lead, now)

    async with session_maker() as s:
        jobs = list(
            (await s.execute(select(MessageJob).where(MessageJob.job_type == PROMO_APPLY_EXISTING_BOOKING_JOB_TYPE)))
            .scalars()
            .all()
        )

    assert len(jobs) == 1, "idempotent: only one job must be created"
    assert jobs[0].max_attempts == 3


# =============================================================================
# K. Job skipped for non-issued leads
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,extra_fields",
    [
        ("applied", {"applied_at": _NOW}),
        ("booked", {}),
        ("expired", {}),
        ("rejected_not_new", {"reject_reason": "has_prior_visits"}),
    ],
)
async def test_promo_apply_existing_booking_skips_non_issued_leads(
    session_maker, status: str, extra_fields: dict
) -> None:
    """Test K: job is skipped (done) for leads not in 'issued' status."""
    fetch_mock = AsyncMock()

    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(status=status)
            for k, v in extra_fields.items():
                setattr(lead, k, v)
            session.add(lead)
            await session.flush()

            job = _make_existing_booking_job(lead.id)
            session.add(job)
            await session.flush()

            with patch(
                "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                fetch_mock,
            ):
                await process_promo_apply_existing_booking_job(session, job)

    fetch_mock.assert_not_called()
    assert job.status == "done"
    assert job.last_error is None


@pytest.mark.asyncio
async def test_promo_apply_existing_booking_skips_expired_lead(session_maker) -> None:
    """Test K (expired variant): expired lead → job done, no record search."""
    fetch_mock = AsyncMock()

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session, starts_at=_NOW + timedelta(days=7))

            # Lead is 'issued' but already past expires_at.
            lead = _make_lead(status="issued", expires_at=_NOW - timedelta(days=1))
            session.add(lead)
            await session.flush()

            job = _make_existing_booking_job(lead.id)
            session.add(job)
            await session.flush()

            with patch(
                "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                fetch_mock,
            ):
                await process_promo_apply_existing_booking_job(session, job)

    fetch_mock.assert_not_called()
    assert job.status == "done"


# =============================================================================
# L. handle_promo_command does not call Altegio record-search APIs
# =============================================================================


@pytest.mark.asyncio
async def test_handle_promo_command_no_altegio_record_api_for_existing_booking(
    session_maker,
) -> None:
    """Test L: handle_promo_command creates job without calling Altegio record/PUT APIs."""
    fetch_mock = AsyncMock()
    put_mock = AsyncMock()
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            await _setup_sender(session)

            evt = WhatsAppEvent(
                dedupe_key="wa:existing-booking-L-1",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_inbound_payload(PHONE_NUMBER_ID, FROM_PHONE, "aktion"),
            )
            session.add(evt)
            await session.flush()

            with (
                patch(
                    "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                    return_value=_FakeCW(),
                ),
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    fetch_mock,
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    put_mock,
                ),
            ):
                await handle_event(session, evt, provider)

    # handle_promo_command must NOT have called Altegio APIs — the search is deferred to the job.
    fetch_mock.assert_not_called()
    put_mock.assert_not_called()

    # The job must have been created.
    async with session_maker() as s:
        jobs = list(
            (await s.execute(select(MessageJob).where(MessageJob.job_type == PROMO_APPLY_EXISTING_BOOKING_JOB_TYPE)))
            .scalars()
            .all()
        )

    assert len(jobs) == 1, "promo_apply_existing_booking job must be created"
    assert jobs[0].max_attempts == 3
