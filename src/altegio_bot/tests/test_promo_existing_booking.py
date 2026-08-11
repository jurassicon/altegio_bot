"""Tests: promo service block in replies and promo_apply_existing_booking job.

Covers:
A.  build_reply_issued_with_card() includes service block (new German text).
B.  Empty promo_allowed_services_display_text → no service block in any issued reply.
C.  build_reply_issued() and build_reply_already_issued() also include the block.
D.  handle_promo_command():
      D1. Creates promo_apply_existing_booking job for new issued lead (gate=True, send OK).
      D2. Does NOT create job when promo_apply_existing_booking_enabled=False.
      D3. Does NOT create job when safe_send fails.
E.  process_promo_eligibility_check_job():
      E1. Creates job after decision_status='issued' (gate=True, send OK).
      E2. Does NOT create job when send fails.
F.  process_promo_apply_existing_booking_job:
      F1. Booking predating promo, raw timestamp used → applied,
          booking_created_before_promo_allowed=True in meta.
      F2. First record has non-allowed service, second has allowed → second chosen.
      F3. Missing raw timestamp → booking_created_at_missing=True in meta, still applied.
      F4. datetime_created field parsed by canonical parser; predates promo → applied.
      F5. Naive timestamp in create_date → canonical Belgrade tz (not UTC) applied.
G.  No future booking → done, meta.existing_booking_skip_reason='no_future_booking'.
H.  Future booking but service not allowed → done, no Altegio API calls.
I.  Ambiguous candidates:
      I1. Two eligible records with same starts_at → fail-closed,
          meta.existing_booking_skip_reason='ambiguous_candidates'.
      I2. Two records with same starts_at but only one is eligible (other has wrong
          service) → not ambiguous, eligible one chosen.
J.  _ensure_promo_apply_existing_booking_job is idempotent: two calls → one job;
    lead.meta receives job_id, queued_at, dedupe_key.
K.  process_promo_apply_existing_booking_job skips non-issued/expired leads.
    Defers (status='queued') when promo_apply_discount_enabled=False or
    promo_apply_discount_api_verified=False.
    Kill switch (promo_apply_existing_booking_enabled=False at execution) → done, not queued.
L.  handle_promo_command does not call Altegio record-search or PUT APIs — job
    is created without proactively searching for existing bookings.
M.  After apply, meta includes existing_booking_apply_altegio_record_id.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
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
    process_promo_eligibility_check_job,
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
        patch.object(settings, "promo_apply_existing_booking_enabled", True),
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


class _FailingProvider:
    """Provider that always raises on send — simulates delivery failure."""

    async def send(self, sender_id, phone_e164, text, contact_name=None):
        raise RuntimeError("simulated send failure")

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
    raw: dict | None = None,
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
        raw=raw if raw is not None else {},
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


_FAKE_ALTEGIO_RECORD = {
    "attendance": 0,
    "visit_attendance": 0,
    "services": [{"id": _ALLOWED_SERVICE, "cost": 100.0, "manual_cost": None}],
    "comment": "",
}
_FAKE_PUT_RESULT = {"data": {"services": [{"id": _ALLOWED_SERVICE, "discount": 15.0}]}}


# =============================================================================
# A. Service block — new German text
# =============================================================================


def test_service_block_in_issued_with_card_reply() -> None:
    """Test A: build_reply_issued_with_card includes new German service block text."""
    expires_at = datetime(2026, 6, 30, tzinfo=_UTC)
    with patch.object(settings, "promo_allowed_services_display_text", "Haarschnitt, Coloration"):
        reply = build_reply_issued_with_card(
            expires_at,
            "https://example.com/book",
            Decimal("15"),
            "fixed",
            "CARD42",
        )
    assert "Bitte buchen Sie für diese Aktion eine der folgenden Leistungen:" in reply
    assert "Haarschnitt, Coloration" in reply
    assert "Nur bei diesen Leistungen kann der Rabatt automatisch zugeordnet werden." in reply
    # Block must appear before the booking link.
    assert reply.index("Nur bei diesen Leistungen") < reply.index("Termin buchen:")


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
        assert "Bitte buchen Sie für diese Aktion" not in reply
        assert "Nur bei diesen Leistungen" not in reply


def test_no_service_block_when_setting_whitespace_only() -> None:
    """Whitespace-only setting is treated as empty."""
    expires_at = datetime(2026, 6, 30, tzinfo=_UTC)
    with patch.object(settings, "promo_allowed_services_display_text", "   \t  "):
        reply = build_reply_issued(expires_at, "https://example.com", Decimal("15"), "fixed")
    assert "Bitte buchen Sie für diese Aktion" not in reply


# =============================================================================
# C. build_reply_issued and build_reply_already_issued also include block
# =============================================================================


def test_service_block_in_issued_and_already_issued_replies() -> None:
    """Test C: service block (new German text) appears in both issued reply builders."""
    expires_at = datetime(2026, 6, 30, tzinfo=_UTC)
    services_text = "Haarschnitt, Keratin"
    with patch.object(settings, "promo_allowed_services_display_text", services_text):
        r_issued = build_reply_issued(expires_at, "https://example.com", Decimal("15"), "fixed")
        r_already = build_reply_already_issued(expires_at, "https://example.com")

    for reply in (r_issued, r_already):
        assert "Bitte buchen Sie für diese Aktion eine der folgenden Leistungen:" in reply
        assert services_text in reply
        assert "Nur bei diesen Leistungen kann der Rabatt automatisch zugeordnet werden." in reply
        assert reply.index("Nur bei diesen Leistungen") < reply.index("Termin buchen:")


# =============================================================================
# D. handle_promo_command: job creation, gate, send failure
# =============================================================================


@pytest.mark.asyncio
async def test_handle_promo_command_creates_existing_booking_job(session_maker) -> None:
    """Test D1: handle_promo_command enqueues promo_apply_existing_booking for new issued lead."""
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            await _setup_sender(session)

            evt = WhatsAppEvent(
                dedupe_key="wa:existing-booking-D1",
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
    # Richer meta written to lead
    meta = lead.meta or {}
    assert meta.get("existing_booking_job_queued") is True
    assert meta.get("existing_booking_job_id") == job.id
    assert "existing_booking_job_queued_at" in meta


@pytest.mark.asyncio
async def test_handle_promo_command_no_job_when_gate_disabled(session_maker) -> None:
    """Test D2: gate=False → promo_apply_existing_booking job NOT created."""
    provider = _CaptureProvider()

    with patch.object(settings, "promo_apply_existing_booking_enabled", False):
        async with session_maker() as session:
            async with session.begin():
                await _setup_sender(session, sender_id=402)

                evt = WhatsAppEvent(
                    dedupe_key="wa:existing-booking-D2",
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

    assert len(jobs) == 0, "gate disabled: no promo_apply_existing_booking job must be created"


@pytest.mark.asyncio
async def test_handle_promo_command_no_job_on_send_failure(session_maker) -> None:
    """Test D3: send fails → promo_apply_existing_booking job NOT created."""
    provider = _FailingProvider()

    async with session_maker() as session:
        async with session.begin():
            await _setup_sender(session, sender_id=403)

            evt = WhatsAppEvent(
                dedupe_key="wa:existing-booking-D3",
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

    assert len(jobs) == 0, "send failed: job must NOT be created before successful send"


# =============================================================================
# E. process_promo_eligibility_check_job: job creation, send failure
# =============================================================================


@pytest.mark.asyncio
async def test_eligibility_check_job_creates_existing_booking_job(session_maker) -> None:
    """Test E1: process_promo_eligibility_check_job enqueues existing-booking job on 'issued'."""
    provider = _CaptureProvider()

    # Step 1: fire promo with async eligibility check enabled → creates pending_check lead + job.
    with patch.object(settings, "promo_async_eligibility_check_enabled", True):
        async with session_maker() as session:
            async with session.begin():
                await _setup_sender(session, sender_id=404)

                evt = WhatsAppEvent(
                    dedupe_key="wa:existing-booking-E1",
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
    # Richer meta
    meta = refreshed_lead.meta or {}
    assert meta.get("existing_booking_job_queued") is True


@pytest.mark.asyncio
async def test_eligibility_check_job_no_existing_booking_job_on_send_failure(session_maker) -> None:
    """Test E2: eligibility check send fails → existing-booking job NOT created."""
    provider = _FailingProvider()

    with patch.object(settings, "promo_async_eligibility_check_enabled", True):
        async with session_maker() as session:
            async with session.begin():
                await _setup_sender(session, sender_id=405)

                evt = WhatsAppEvent(
                    dedupe_key="wa:existing-booking-E2",
                    status="received",
                    error=None,
                    query={},
                    headers={},
                    payload=_inbound_payload(PHONE_NUMBER_ID, FROM_PHONE, "aktion"),
                )
                session.add(evt)
                await session.flush()

                # Inbound send (checking status) uses _CaptureProvider so it succeeds.
                with patch(
                    "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                    return_value=_FakeCW(),
                ):
                    await handle_event(session, evt, _CaptureProvider())

    async with session_maker() as s:
        elig_job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == PROMO_ELIGIBILITY_CHECK_JOB_TYPE))
        ).scalar_one()

    # Process eligibility job with a failing provider → send fails → no existing-booking job.
    async with session_maker() as session:
        async with session.begin():
            await ow.process_job_in_session(session, elig_job.id, provider)

    async with session_maker() as s:
        eb_jobs = list(
            (await s.execute(select(MessageJob).where(MessageJob.job_type == PROMO_APPLY_EXISTING_BOOKING_JOB_TYPE)))
            .scalars()
            .all()
        )

    assert len(eb_jobs) == 0, "send failed: existing-booking job must NOT be created"


@pytest.mark.asyncio
async def test_eligibility_reply_passes_altegio_tenant_to_provider_scoped_chatwoot_mirror(
    session_maker,
) -> None:
    """An Altegio promo job keeps its provider/company pair through safe_send."""
    from altegio_bot.webhooks.common import (
        parse_chatwoot_inbox_company_map,
        resolve_chatwoot_tenant_inbox,
    )

    captured: dict[str, object] = {}

    async def fake_safe_send(**kwargs: object) -> tuple[str, None]:
        captured.update(kwargs)
        return "wamid.PROMO.ELIGIBILITY", None

    async with session_maker() as session:
        async with session.begin():
            await _setup_sender(session, sender_id=406)
            lead = _make_lead(status="pending_check", loyalty_card_id=None)
            session.add(lead)
            await session.flush()
            job = MessageJob(
                provider=PROVIDER_ALTEGIO,
                company_id=_COMPANY,
                job_type=PROMO_ELIGIBILITY_CHECK_JOB_TYPE,
                run_at=_NOW,
                status="processing",
                dedupe_key="promo-eligibility-provider-scope",
                payload={"promo_lead_id": lead.id},
            )
            session.add(job)
            await session.flush()

            with (
                patch.object(settings, "promo_issue_loyalty_card_enabled", False),
                patch(
                    "altegio_bot.workers.promo_lead_handler.safe_send",
                    side_effect=fake_safe_send,
                ),
            ):
                await process_promo_eligibility_check_job(session, job, _CaptureProvider())

        jobs = list((await session.execute(select(MessageJob))).scalars().all())

    assert captured["tenant_provider"] == PROVIDER_ALTEGIO
    assert captured["company_id"] == _COMPANY
    parsed = parse_chatwoot_inbox_company_map(
        '{"103":{"provider":"altegio","company_id":1},"101":{"provider":"easyweek","company_id":308697}}'
    )
    assert resolve_chatwoot_tenant_inbox(
        parsed,
        captured["tenant_provider"],
        captured["company_id"],
    ) == (103, None)
    assert all(queued.provider != PROVIDER_EASYWEEK for queued in jobs)


# =============================================================================
# F. process_promo_apply_existing_booking_job: timestamp and service selection
# =============================================================================

# Fixed timestamps for F tests — all in the far future for clarity.
_F_BOOKING_CREATED_AT = datetime(2099, 4, 1, 0, 0, tzinfo=_UTC)  # booking created
_F_ISSUED_AT = datetime(2099, 6, 1, 0, 0, tzinfo=_UTC)  # promo issued (after booking)
_F_STARTS_AT = datetime(2099, 7, 1, 0, 0, tzinfo=_UTC)  # appointment (future)
_F_EXPIRES_AT = datetime(2199, 1, 1, tzinfo=_UTC)


@pytest.mark.asyncio
async def test_promo_apply_existing_booking_raw_timestamp_predates_promo(session_maker) -> None:
    """Test F1: raw create_date used; predates promo issued_at; allow flag → applied."""
    raw_ts = {"create_date": _F_BOOKING_CREATED_AT.isoformat()}

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session, starts_at=_F_STARTS_AT, raw=raw_ts)
            await _seed_service(session)

            lead = _make_lead(issued_at=_F_ISSUED_AT, expires_at=_F_EXPIRES_AT)
            session.add(lead)
            await session.flush()

            job = _make_existing_booking_job(lead.id)
            session.add(job)
            await session.flush()

            with (
                _base_settings_ctx(),
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    AsyncMock(return_value=_FAKE_ALTEGIO_RECORD),
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    AsyncMock(return_value=_FAKE_PUT_RESULT),
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
    assert meta.get("existing_booking_apply_result") == "applied"
    assert meta.get("existing_booking_apply_record_id") == 300


@pytest.mark.asyncio
async def test_promo_apply_existing_booking_first_not_allowed_second_allowed(session_maker) -> None:
    """Test F2: first future record has wrong service; second has allowed → second chosen."""
    _STARTS_AT_1 = datetime(2099, 5, 1, tzinfo=_UTC)  # earlier, non-allowed service
    _STARTS_AT_2 = datetime(2099, 8, 1, tzinfo=_UTC)  # later, allowed service

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session, record_id=301, altegio_record_id=8001, starts_at=_STARTS_AT_1)
            await _seed_service(session, record_id=301, service_id=_OTHER_SERVICE)
            await _seed_record(session, record_id=302, altegio_record_id=8002, starts_at=_STARTS_AT_2)
            await _seed_service(session, record_id=302, service_id=_ALLOWED_SERVICE)

            lead = _make_lead(expires_at=_F_EXPIRES_AT)
            session.add(lead)
            await session.flush()

            job = _make_existing_booking_job(lead.id)
            session.add(job)
            await session.flush()

            with (
                _base_settings_ctx(),
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    AsyncMock(return_value=_FAKE_ALTEGIO_RECORD),
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    AsyncMock(return_value=_FAKE_PUT_RESULT),
                ),
            ):
                await process_promo_apply_existing_booking_job(session, job)

            lead_id = lead.id

    async with session_maker() as s:
        refreshed_lead = await s.get(PromoLead, lead_id)

    assert job.status == "done"
    assert refreshed_lead is not None
    meta = refreshed_lead.meta or {}
    # Second record (302) must have been selected, not the first (301).
    assert meta.get("existing_booking_apply_record_id") == 302
    assert meta.get("existing_booking_apply_result") == "applied"


@pytest.mark.asyncio
async def test_promo_apply_existing_booking_missing_raw_timestamp(session_maker) -> None:
    """Test F3: no timestamp in record.raw → booking_created_at_missing=True, still applied."""
    _STARTS_AT = datetime(2099, 5, 1, tzinfo=_UTC)

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            # raw={} → no timestamp fields → canonical parser returns None
            await _seed_record(session, starts_at=_STARTS_AT, raw={})
            await _seed_service(session)

            lead = _make_lead(expires_at=_F_EXPIRES_AT)
            session.add(lead)
            await session.flush()

            job = _make_existing_booking_job(lead.id)
            session.add(job)
            await session.flush()

            with (
                _base_settings_ctx(),
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    AsyncMock(return_value=_FAKE_ALTEGIO_RECORD),
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    AsyncMock(return_value=_FAKE_PUT_RESULT),
                ),
            ):
                await process_promo_apply_existing_booking_job(session, job)

            lead_id = lead.id

    async with session_maker() as s:
        refreshed_lead = await s.get(PromoLead, lead_id)

    assert job.status == "done"
    assert refreshed_lead is not None
    meta = refreshed_lead.meta or {}
    # try_apply_promo_discount must have set booking_created_at_missing=True.
    assert meta.get("booking_created_at_missing") is True
    assert meta.get("existing_booking_apply_result") == "applied"


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
    _STARTS_AT = datetime(2099, 5, 1, tzinfo=_UTC)

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
# I. Ambiguous candidates / only one eligible
# =============================================================================


@pytest.mark.asyncio
async def test_promo_apply_existing_booking_ambiguous_eligible_candidates(session_maker) -> None:
    """Test I1: two eligible records with identical starts_at → ambiguous → fail-closed."""
    _STARTS_AT = datetime(2099, 5, 1, tzinfo=_UTC)

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            # Both records have the allowed service and the same starts_at.
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
    assert meta.get("existing_booking_eligible_records") == 2


@pytest.mark.asyncio
async def test_promo_apply_existing_booking_one_eligible_same_starts_at(session_maker) -> None:
    """Test I2: two records with same starts_at; only one is eligible → not ambiguous."""
    _STARTS_AT = datetime(2099, 5, 1, tzinfo=_UTC)

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            # First record: non-allowed service (not eligible).
            await _seed_record(session, record_id=301, altegio_record_id=8001, starts_at=_STARTS_AT)
            await _seed_service(session, record_id=301, service_id=_OTHER_SERVICE)
            # Second record: allowed service (eligible), same starts_at.
            await _seed_record(session, record_id=302, altegio_record_id=8002, starts_at=_STARTS_AT)
            await _seed_service(session, record_id=302, service_id=_ALLOWED_SERVICE)

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
                    AsyncMock(return_value=_FAKE_ALTEGIO_RECORD),
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    AsyncMock(return_value=_FAKE_PUT_RESULT),
                ),
            ):
                await process_promo_apply_existing_booking_job(session, job)

            lead_id = lead.id

    async with session_maker() as s:
        refreshed_lead = await s.get(PromoLead, lead_id)

    assert job.status == "done"
    assert refreshed_lead is not None
    meta = refreshed_lead.meta or {}
    # Not ambiguous — only record 302 was eligible.
    assert meta.get("existing_booking_skip_reason") is None
    assert meta.get("existing_booking_apply_record_id") == 302
    assert meta.get("existing_booking_eligible_records") == 1


# =============================================================================
# J. Idempotency: two enqueue calls → one job; richer meta
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

            lead_id = lead.id

    async with session_maker() as s:
        jobs = list(
            (await s.execute(select(MessageJob).where(MessageJob.job_type == PROMO_APPLY_EXISTING_BOOKING_JOB_TYPE)))
            .scalars()
            .all()
        )
        refreshed_lead = await s.get(PromoLead, lead_id)

    assert len(jobs) == 1, "idempotent: only one job must be created"
    assert jobs[0].max_attempts == 3
    # Richer meta must be set on the lead.
    meta = refreshed_lead.meta or {}
    assert meta.get("existing_booking_job_queued") is True
    assert meta.get("existing_booking_job_id") == jobs[0].id
    assert "existing_booking_job_queued_at" in meta
    assert "existing_booking_job_dedupe_key" in meta


# =============================================================================
# K. Non-issued leads → done; expired → done; defer when gates disabled
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,extra_fields",
    [
        ("applied", {"applied_at": _NOW}),
        ("booked", {}),
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
    """Test K (expired variant): issued lead past expires_at → job done, no record search."""
    fetch_mock = AsyncMock()

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session, starts_at=datetime(2099, 1, 1, tzinfo=_UTC))

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


@pytest.mark.asyncio
async def test_promo_apply_existing_booking_defer_when_apply_disabled(session_maker) -> None:
    """Test K (defer): promo_apply_discount_enabled=False → job deferred, not done."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session, starts_at=datetime(2099, 1, 1, tzinfo=_UTC))
            await _seed_service(session)

            lead = _make_lead()
            session.add(lead)
            await session.flush()

            job = _make_existing_booking_job(lead.id)
            session.add(job)
            await session.flush()

            with patch.object(settings, "promo_apply_discount_enabled", False):
                await process_promo_apply_existing_booking_job(session, job)

    assert job.status == "queued", "must defer, not mark done"
    assert job.locked_at is None
    assert job.run_at is not None


@pytest.mark.asyncio
async def test_promo_apply_existing_booking_defer_when_api_not_verified(session_maker) -> None:
    """Test K (defer): promo_apply_discount_api_verified=False → job deferred, not done."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session, starts_at=datetime(2099, 1, 1, tzinfo=_UTC))
            await _seed_service(session)

            lead = _make_lead()
            session.add(lead)
            await session.flush()

            job = _make_existing_booking_job(lead.id)
            session.add(job)
            await session.flush()

            with (
                patch.object(settings, "promo_apply_discount_enabled", True),
                patch.object(settings, "promo_apply_discount_api_verified", False),
            ):
                await process_promo_apply_existing_booking_job(session, job)

    assert job.status == "queued", "must defer, not mark done"
    assert job.locked_at is None
    assert job.run_at is not None


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
            await _setup_sender(session, sender_id=406)

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


# =============================================================================
# K (kill switch). promo_apply_existing_booking_enabled=False at execution → done
# =============================================================================


@pytest.mark.asyncio
async def test_promo_apply_existing_booking_kill_switch_at_execution(session_maker) -> None:
    """Kill switch: flag=False at execution time → job done (not queued), no API calls."""
    fetch_mock = AsyncMock()

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session, starts_at=datetime(2099, 1, 1, tzinfo=_UTC))
            await _seed_service(session)

            lead = _make_lead()
            session.add(lead)
            await session.flush()

            job = _make_existing_booking_job(lead.id)
            session.add(job)
            await session.flush()

            with (
                patch.object(settings, "promo_apply_existing_booking_enabled", False),
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
    assert job.status == "done", "kill switch must mark job done, not queued"
    assert job.locked_at is None
    assert refreshed_lead is not None
    meta = refreshed_lead.meta or {}
    assert meta.get("existing_booking_apply_result") == "disabled_by_kill_switch"
    assert meta.get("existing_booking_apply_skip_reason") == "promo_apply_existing_booking_enabled=False"
    assert "existing_booking_apply_checked_at" in meta


# =============================================================================
# F4/F5. Canonical parser: datetime_created field; naive timestamp → Belgrade tz
# =============================================================================


@pytest.mark.asyncio
async def test_promo_apply_existing_booking_datetime_created_field(session_maker) -> None:
    """Test F4: datetime_created field parsed by canonical parser; predates promo → applied."""
    raw_ts = {"datetime_created": _F_BOOKING_CREATED_AT.isoformat()}

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session, starts_at=_F_STARTS_AT, raw=raw_ts)
            await _seed_service(session)

            lead = _make_lead(issued_at=_F_ISSUED_AT, expires_at=_F_EXPIRES_AT)
            session.add(lead)
            await session.flush()

            job = _make_existing_booking_job(lead.id)
            session.add(job)
            await session.flush()

            with (
                _base_settings_ctx(),
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    AsyncMock(return_value=_FAKE_ALTEGIO_RECORD),
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    AsyncMock(return_value=_FAKE_PUT_RESULT),
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
    assert meta.get("existing_booking_apply_result") == "applied"


@pytest.mark.asyncio
async def test_promo_apply_existing_booking_naive_timestamp_canonical_tz(session_maker) -> None:
    """Test F5: naive timestamp in create_date → canonical Belgrade tz applied (not UTC)."""
    from altegio_bot.altegio_records import extract_booking_created_at_from_record_details

    # Naive — no tzinfo.  Canonical localises to Europe/Belgrade (UTC+2 in April),
    # the old local parser incorrectly used UTC.  The difference is 2 hours.
    raw_ts = {"create_date": "2099-04-01T10:00:00"}

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session, starts_at=_F_STARTS_AT, raw=raw_ts)
            await _seed_service(session)

            lead = _make_lead(issued_at=_F_ISSUED_AT, expires_at=_F_EXPIRES_AT)
            session.add(lead)
            await session.flush()

            job = _make_existing_booking_job(lead.id)
            session.add(job)
            await session.flush()

            with (
                _base_settings_ctx(),
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    AsyncMock(return_value=_FAKE_ALTEGIO_RECORD),
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    AsyncMock(return_value=_FAKE_PUT_RESULT),
                ),
            ):
                await process_promo_apply_existing_booking_job(session, job)

            lead_id = lead.id

    async with session_maker() as s:
        refreshed_lead = await s.get(PromoLead, lead_id)

    assert refreshed_lead is not None
    meta = refreshed_lead.meta or {}
    # meta["booking_created_at"] is set by try_apply_promo_discount when the booking
    # predates the promo (allow_existing_booking_before_promo=True path).
    expected_dt = extract_booking_created_at_from_record_details(raw_ts)
    assert expected_dt is not None, "canonical parser must parse the naive timestamp"
    assert meta.get("booking_created_at") == expected_dt.isoformat()
    assert meta.get("booking_created_before_promo_allowed") is True


# =============================================================================
# M. existing_booking_apply_altegio_record_id in meta after apply
# =============================================================================


@pytest.mark.asyncio
async def test_promo_apply_existing_booking_altegio_record_id_in_meta(session_maker) -> None:
    """Test M: after apply, meta includes existing_booking_apply_altegio_record_id."""
    _STARTS_AT = datetime(2099, 5, 1, tzinfo=_UTC)
    _ALTEGIO_RECORD_ID = 8001

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session, altegio_record_id=_ALTEGIO_RECORD_ID, starts_at=_STARTS_AT)
            await _seed_service(session)

            lead = _make_lead(expires_at=_F_EXPIRES_AT)
            session.add(lead)
            await session.flush()

            job = _make_existing_booking_job(lead.id)
            session.add(job)
            await session.flush()

            with (
                _base_settings_ctx(),
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    AsyncMock(return_value=_FAKE_ALTEGIO_RECORD),
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    AsyncMock(return_value=_FAKE_PUT_RESULT),
                ),
            ):
                await process_promo_apply_existing_booking_job(session, job)

            lead_id = lead.id

    async with session_maker() as s:
        refreshed_lead = await s.get(PromoLead, lead_id)

    assert job.status == "done"
    assert refreshed_lead is not None
    meta = refreshed_lead.meta or {}
    assert meta.get("existing_booking_apply_altegio_record_id") == _ALTEGIO_RECORD_ID
    assert meta.get("existing_booking_apply_result") == "applied"
