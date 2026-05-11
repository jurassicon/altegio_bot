"""Tests: promo discount application to Altegio visits.

Covers:
1.  Feature disabled (promo_apply_discount_enabled=False) — API not called, status unchanged.
2.  API not verified (promo_apply_discount_api_verified=False) — API not called,
    meta.discount_apply_error set, status remains booked.
3.  No active PromoLead — no API call.
4.  Expired PromoLead — no API call (excluded by SQL query).
5.  Active issued lead + allowed service + verified API → applied, MessageJob queued
    for customer WhatsApp notification; lead.meta stores job_id and dedupe_key.
18. Direct wrapper: success=false response → PromoDiscountApplyError.
19. Direct wrapper: no success key in response → PromoDiscountApplyError.
20. try_apply with API returning success=false: lead.status=apply_failed, no MessageJob.
21. Idempotent notification: _ensure called twice for same lead → one MessageJob.
6.  Active lead without loyalty_card_id — not found by SQL, no API call.
7.  Active lead but service not in allowlist — no API call, meta.apply_skip_reason set.
8.  API failure — status='apply_failed', meta.discount_apply_error set.
9.  Already-applied lead — finder excludes it (status not in issued/booked), no API call.
10. Old client edge: prior attended visit (excluding current) → skip discount.
11. company_id isolation: same phone, two companies — only matching company lead used.
12. Update webhook: inbox_worker does not call try_apply_promo_discount.
13. Direct wrapper: api_verified=False → PromoDiscountApplyError, no HTTP call.
14. Direct wrapper: success response → PromoDiscountApplyResult(applied=True).
15. Direct wrapper: HTTP error → PromoDiscountApplyError.
16. Direct wrapper: invalid JSON → PromoDiscountApplyError.
17. Direct wrapper: unexpected response shape → PromoDiscountApplyError.
22. Missing booking_created_at (None) → skip, apply_skip_reason set.
23. booking_created_at before lead.issued_at → skip, predates promo lead.
24. booking_created_at after lead.issued_at → apply proceeds normally.
25. Booked lead bound to different record → finder returns None, no API call.
26. Booked lead with same record → retry allowed, apply proceeds.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from sqlalchemy import select

from altegio_bot.models.models import Client, MessageJob, PromoLead, Record, RecordService
from altegio_bot.promo_discount_apply import (
    PromoDiscountApplyError,
    PromoDiscountApplyResult,
    apply_promo_discount_to_visit,
    try_apply_promo_discount,
)
from altegio_bot.settings import settings

_UTC = timezone.utc
_NOW = datetime(2026, 5, 8, 12, 0, 0, tzinfo=_UTC)
_FUTURE = datetime(2099, 1, 1, tzinfo=_UTC)
_PHONE = "+4916099887766"
_COMPANY = 1
_LOCATION = 9001
_CARD_ID = "555"
_PROGRAM_ID = "dp_001"
_ALLOWED_SERVICE = 12345
_OTHER_SERVICE = 99999


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


async def _seed_client(session, *, client_id: int = 100, phone: str = _PHONE) -> Client:
    c = Client(
        id=client_id,
        company_id=_COMPANY,
        altegio_client_id=client_id,
        phone_e164=phone,
        display_name="Test",
        raw={},
    )
    session.add(c)
    await session.flush()
    return c


async def _seed_record(
    session,
    *,
    record_id: int = 200,
    altegio_record_id: int = 999,
    client_id: int = 100,
    is_deleted: bool = False,
    attendance: int | None = None,
    visit_attendance: int | None = None,
) -> Record:
    r = Record(
        id=record_id,
        company_id=_COMPANY,
        altegio_record_id=altegio_record_id,
        client_id=client_id,
        altegio_client_id=client_id,
        is_deleted=is_deleted,
        attendance=attendance,
        visit_attendance=visit_attendance,
        raw={},
    )
    session.add(r)
    await session.flush()
    return r


async def _seed_service(session, *, record_id: int = 200, service_id: int = _ALLOWED_SERVICE) -> None:
    session.add(
        RecordService(
            record_id=record_id,
            service_id=service_id,
            title="Test Service",
            raw={},
        )
    )
    await session.flush()


def _make_lead(
    *,
    phone: str = _PHONE,
    company_id: int = _COMPANY,
    status: str = "issued",
    expires_at: datetime = _FUTURE,
    loyalty_card_id: str | None = _CARD_ID,
    location_id: int | None = _LOCATION,
    discount_program_id: str | None = _PROGRAM_ID,
    meta: dict | None = None,
) -> PromoLead:
    return PromoLead(
        company_id=company_id,
        phone_e164=phone,
        campaign_name="welcome_discount",
        secret_code="aktion",
        discount_amount=Decimal("15"),
        discount_type="fixed",
        status=status,
        issued_at=datetime(2026, 1, 1, tzinfo=_UTC),
        expires_at=expires_at,
        loyalty_card_id=loyalty_card_id,
        location_id=location_id,
        discount_program_id=discount_program_id,
        meta=meta if meta is not None else {"loyalty_card_issued": True},
    )


def _base_settings_ctx(**overrides):
    """Context manager that patches all required settings for discount apply."""
    import contextlib

    defaults = {
        "promo_apply_discount_enabled": True,
        "promo_apply_discount_api_verified": True,
        "promo_allowed_service_ids": str(_ALLOWED_SERVICE),
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


# ---------------------------------------------------------------------------
# 1. Feature disabled → nothing happens
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_feature_disabled_no_api_call(session_maker) -> None:
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session)
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with patch.object(settings, "promo_apply_discount_enabled", False):
                    await try_apply_promo_discount(session, record, _COMPANY)

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()
    assert lead is not None
    assert lead.status == "issued"


# ---------------------------------------------------------------------------
# 2. API not verified → fail closed, meta error set
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_api_not_verified_blocks_call(session_maker) -> None:
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session)
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx(promo_apply_discount_api_verified=False):
                    await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()

    assert lead is not None
    # Status transitions to booked (booking happened) even when API not verified
    assert lead.status == "booked"
    meta = lead.meta or {}
    assert "discount_apply_error" in meta
    assert "promo_apply_discount_api_verified=False" in meta["discount_apply_error"]


# ---------------------------------------------------------------------------
# 3. No active PromoLead → no API call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_active_lead_no_api_call(session_maker) -> None:
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session)
            await _seed_service(session)
            # No PromoLead in DB

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY)

    mock_api.assert_not_called()


# ---------------------------------------------------------------------------
# 4. Expired PromoLead → excluded by SQL, no API call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_expired_lead_excluded(session_maker) -> None:
    past = datetime(2020, 1, 1, tzinfo=_UTC)
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session)
            await _seed_service(session)
            lead = _make_lead(expires_at=past)
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY)

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()
    assert lead is not None
    assert lead.status == "issued"


# ---------------------------------------------------------------------------
# 4b. Ineligible PromoLead statuses → excluded by SQL, no API call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["rejected_not_new", "pending_check"])
async def test_ineligible_lead_status_excluded(session_maker, status: str) -> None:
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session)
            await _seed_service(session)
            lead = _make_lead(status=status)
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()

    assert lead is not None
    assert lead.status == status


# ---------------------------------------------------------------------------
# 5. Happy path: applied, MessageJob queued for customer notification
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_happy_path_applies_discount(session_maker) -> None:
    mock_api = AsyncMock(return_value=PromoDiscountApplyResult(applied=True, raw={"success": True}))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session, altegio_record_id=777)
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_api.assert_called_once_with(
        location_id=_LOCATION,
        card_id=int(_CARD_ID),
        program_id=_PROGRAM_ID,
        record_id=777,
    )

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()

    assert lead is not None
    assert lead.status == "applied"
    assert lead.applied_at is not None
    meta = lead.meta or {}
    assert meta.get("discount_applied_at") is not None
    assert meta.get("discount_apply_altegio_record_id") == 777
    assert meta.get("discount_apply_card_id") == int(_CARD_ID)
    assert meta.get("discount_apply_program_id") == _PROGRAM_ID
    assert meta.get("customer_notification") == "queued"
    assert meta.get("customer_notification_job_id") is not None
    assert meta.get("customer_notification_created_at") is not None
    assert meta.get("customer_notification_dedupe_key", "").startswith("promo_discount_applied:")

    async with session_maker() as s:
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert job is not None
    assert job.client_id == 100
    assert job.dedupe_key.startswith("promo_discount_applied:")
    assert job.id == meta.get("customer_notification_job_id")


# ---------------------------------------------------------------------------
# 6. Lead without loyalty_card_id → excluded by SQL, no API call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lead_without_card_id_excluded(session_maker) -> None:
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session)
            await _seed_service(session)
            lead = _make_lead(loyalty_card_id=None)
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY)

    mock_api.assert_not_called()


# ---------------------------------------------------------------------------
# 7. Service not in allowlist → no API call, skip reason recorded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_service_not_allowed_skips_discount(session_maker) -> None:
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session)
            await _seed_service(session, service_id=_OTHER_SERVICE)  # not in allowlist
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()

    assert lead is not None
    assert lead.status == "issued"
    meta = lead.meta or {}
    assert meta.get("apply_skip_reason") is not None
    assert "no allowed service" in meta["apply_skip_reason"]


# ---------------------------------------------------------------------------
# 8. API failure → status='apply_failed'
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_api_failure_sets_apply_failed(session_maker) -> None:
    mock_api = AsyncMock(side_effect=PromoDiscountApplyError("Altegio 503 server error"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session, altegio_record_id=888)
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_api.assert_called_once()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()

    assert lead is not None
    assert lead.status == "apply_failed"
    meta = lead.meta or {}
    assert "Altegio 503" in (meta.get("discount_apply_error") or "")
    assert meta.get("discount_apply_attempted_at") is not None


# ---------------------------------------------------------------------------
# 9. Already-applied lead → finder excludes it, no API call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_already_applied_lead_skipped(session_maker) -> None:
    """An already-applied lead is excluded by the SQL query (status not in issued/booked)."""
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session, altegio_record_id=500)
            await _seed_service(session)
            lead = _make_lead(
                status="applied",
                meta={
                    "loyalty_card_issued": True,
                    "discount_applied_at": "2026-05-08T10:00:00+00:00",
                    "discount_apply_altegio_record_id": 500,
                },
            )
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY)

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()
    assert lead is not None
    assert lead.status == "applied"  # unchanged


# ---------------------------------------------------------------------------
# 10. Old client edge: prior attended visit (excl. current) → skip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prior_attended_visit_skips_discount(session_maker) -> None:
    """Client has a prior attended visit (different record) → discount not applied."""
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))

    other_phone = "+10000000001"  # client seeded in conftest with id=1

    async with session_maker() as session:
        async with session.begin():
            # Add attended prior visit for client id=1 (phone +10000000001)
            await _seed_record(
                session,
                record_id=300,
                altegio_record_id=301,
                client_id=1,
                attendance=1,
            )
            # Current booking (different record id)
            current_record = await _seed_record(
                session,
                record_id=400,
                altegio_record_id=401,
                client_id=1,
            )
            await _seed_service(session, record_id=400)
            lead = _make_lead(phone=other_phone)
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, current_record, _COMPANY, booking_created_at=_NOW)

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == other_phone))).scalar_one_or_none()

    assert lead is not None
    meta = lead.meta or {}
    assert "prior attended visits" in (meta.get("apply_skip_reason") or meta.get("apply_error") or "")


# ---------------------------------------------------------------------------
# 11. company_id isolation: lead for company 1 is not matched for company 2
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_company_id_isolation_wrong_company_not_matched(session_maker) -> None:
    """A PromoLead for company 1 must not be picked up by a booking for company 2."""
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session, altegio_record_id=600)
            await _seed_service(session)
            lead = _make_lead()  # company_id=_COMPANY=1
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    # Passing company_id=2 — must NOT match the company_id=1 lead
                    await try_apply_promo_discount(session, record, 2)

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()
    assert lead is not None
    assert lead.status == "issued"  # untouched


# ---------------------------------------------------------------------------
# 12. Update webhook: inbox_worker does not call try_apply_promo_discount
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_webhook_skips_promo_apply() -> None:
    """inbox_worker.handle_event with event_status='update' must not call try_apply_promo_discount."""
    from altegio_bot.workers.inbox_worker import handle_event

    mock_try_apply = AsyncMock()
    session = AsyncMock()

    event = MagicMock()
    event.company_id = _COMPANY
    event.resource = "record"
    event.event_status = "update"
    event.resource_id = None
    event.payload = {
        "data": {
            "id": 42,
            "client": {"id": 100, "display_name": "Test", "phone": _PHONE},
            "services": [{"id": _ALLOWED_SERVICE, "title": "Test", "cost_to_pay": 50}],
            "date": "2026-05-08 12:00:00",
            "staff_id": 5,
        }
    }

    mock_record = MagicMock()
    mock_record.id = 200
    mock_record.company_id = _COMPANY
    mock_record.is_deleted = False

    mock_resolver = AsyncMock(side_effect=RuntimeError("must not be called"))

    with (
        patch("altegio_bot.workers.inbox_worker.upsert_client", new=AsyncMock(return_value=100)),
        patch("altegio_bot.workers.inbox_worker.upsert_record", new=AsyncMock(return_value=200)),
        patch("altegio_bot.workers.inbox_worker.replace_record_services", new=AsyncMock()),
        patch("altegio_bot.workers.inbox_worker.record_has_allowed_service", new=AsyncMock(return_value=True)),
        patch("altegio_bot.workers.inbox_worker.plan_jobs_for_record_event", new=AsyncMock()),
        patch("altegio_bot.workers.inbox_worker.try_apply_promo_discount", mock_try_apply),
        patch("altegio_bot.workers.inbox_worker.resolve_booking_created_at_for_record_create", mock_resolver),
        patch.object(settings, "promo_apply_discount_enabled", True),
    ):
        session.get = AsyncMock(return_value=mock_record)
        await handle_event(session, event)

    mock_try_apply.assert_not_called()
    mock_resolver.assert_not_called()


# ---------------------------------------------------------------------------
# 12b. Record create timestamp lookup failures fail closed
# ---------------------------------------------------------------------------


def _make_record_create_event(*, create_date: str | None = None) -> MagicMock:
    event = MagicMock()
    event.id = 1
    event.company_id = _COMPANY
    event.resource = "record"
    event.event_status = "create"
    event.resource_id = None
    event.received_at = datetime(2026, 5, 8, 20, 0, 0, tzinfo=_UTC)
    data = {
        "id": 424242,
        "client": {"id": 100, "display_name": "Test", "phone": _PHONE},
        "services": [{"id": _ALLOWED_SERVICE, "title": "Test", "cost_to_pay": 50}],
        "date": "2026-05-20 12:00:00",
        "staff_id": 5,
    }
    if create_date is not None:
        data["create_date"] = create_date
    event.payload = {"data": data}
    return event


@pytest.mark.asyncio
async def test_record_create_get_record_http_error_skips_apply_without_notification(session_maker) -> None:
    from altegio_bot.altegio_records import AltegioRecordResearchError
    from altegio_bot.workers.inbox_worker import handle_event

    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))
    fetch_mock = AsyncMock(side_effect=AltegioRecordResearchError("HTTP 500: location_id=9001 record_id=424242"))

    async with session_maker() as session:
        async with session.begin():
            session.add(_make_lead())
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api),
                patch("altegio_bot.workers.inbox_worker.fetch_record_details_for_booking_created_at", fetch_mock),
                patch("altegio_bot.workers.inbox_worker.record_has_allowed_service", new=AsyncMock(return_value=True)),
                patch("altegio_bot.workers.inbox_worker.plan_jobs_for_record_event", new=AsyncMock()),
                patch.object(settings, "promo_location_id_by_company", f'{{"{_COMPANY}": {_LOCATION}}}'),
                _base_settings_ctx(),
            ):
                await handle_event(session, _make_record_create_event())

    fetch_mock.assert_awaited_once_with(location_id=_LOCATION, record_id=424242)
    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead.status == "issued"
    assert "missing booking created timestamp" in (lead.meta or {}).get("apply_skip_reason", "")
    assert job is None


@pytest.mark.asyncio
async def test_record_create_get_record_create_date_after_promo_applies(session_maker) -> None:
    from altegio_bot.workers.inbox_worker import handle_event

    mock_api = AsyncMock(return_value=PromoDiscountApplyResult(applied=True, raw={"success": True}))
    fetch_mock = AsyncMock(return_value={"id": 424242, "create_date": "2026-05-08 14:05:00"})

    issued_at = datetime(2026, 5, 8, 12, 0, 0, tzinfo=_UTC)

    async with session_maker() as session:
        async with session.begin():
            session.add(_make_lead(status="issued"))
            await session.flush()
            lead = (await session.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
            lead.issued_at = issued_at

            with (
                patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api),
                patch("altegio_bot.workers.inbox_worker.fetch_record_details_for_booking_created_at", fetch_mock),
                patch("altegio_bot.workers.inbox_worker.record_has_allowed_service", new=AsyncMock(return_value=True)),
                patch("altegio_bot.workers.inbox_worker.plan_jobs_for_record_event", new=AsyncMock()),
                patch.object(settings, "promo_location_id_by_company", f'{{"{_COMPANY}": {_LOCATION}}}'),
                _base_settings_ctx(),
            ):
                await handle_event(session, _make_record_create_event())

    mock_api.assert_called_once_with(
        location_id=_LOCATION,
        card_id=int(_CARD_ID),
        program_id=_PROGRAM_ID,
        record_id=424242,
    )

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        job = (await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))).scalar_one()

    assert lead.status == "applied"
    assert job.payload["phone_e164"] == _PHONE


# ---------------------------------------------------------------------------
# 13–17. Direct wrapper unit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_wrapper_api_not_verified_raises() -> None:
    """apply_promo_discount_to_visit raises PromoDiscountApplyError when api_verified=False."""
    with patch("httpx.AsyncClient") as mock_client_cls:
        with patch.object(settings, "promo_apply_discount_api_verified", False):
            with pytest.raises(PromoDiscountApplyError, match="api_verified"):
                await apply_promo_discount_to_visit(
                    location_id=9001,
                    card_id=555,
                    program_id=1,
                    record_id=777,
                )

    mock_client_cls.assert_not_called()


@pytest.mark.asyncio
async def test_wrapper_success_response() -> None:
    """Successful HTTP 200 response → PromoDiscountApplyResult(applied=True)."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {"success": True, "id": 42}

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        with patch.object(settings, "promo_apply_discount_api_verified", True):
            result = await apply_promo_discount_to_visit(
                location_id=9001,
                card_id=555,
                program_id=1,
                record_id=777,
            )

    assert result.applied is True
    assert result.raw == {"success": True, "id": 42}


@pytest.mark.asyncio
async def test_wrapper_http_error_raises() -> None:
    """HTTP error → PromoDiscountApplyError."""
    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=httpx.ConnectError("connection refused"))
        mock_client_cls.return_value = mock_client

        with patch.object(settings, "promo_apply_discount_api_verified", True):
            with pytest.raises(PromoDiscountApplyError, match="HTTP error"):
                await apply_promo_discount_to_visit(
                    location_id=9001,
                    card_id=555,
                    program_id=1,
                    record_id=777,
                )


@pytest.mark.asyncio
async def test_wrapper_invalid_json_raises() -> None:
    """Invalid JSON response → PromoDiscountApplyError."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.side_effect = ValueError("invalid json")

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        with patch.object(settings, "promo_apply_discount_api_verified", True):
            with pytest.raises(PromoDiscountApplyError, match="invalid JSON"):
                await apply_promo_discount_to_visit(
                    location_id=9001,
                    card_id=555,
                    program_id=1,
                    record_id=777,
                )


@pytest.mark.asyncio
async def test_wrapper_unexpected_shape_raises() -> None:
    """Non-dict JSON response → PromoDiscountApplyError."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = ["unexpected", "list"]

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        with patch.object(settings, "promo_apply_discount_api_verified", True):
            with pytest.raises(PromoDiscountApplyError, match="unexpected response shape"):
                await apply_promo_discount_to_visit(
                    location_id=9001,
                    card_id=555,
                    program_id=1,
                    record_id=777,
                )


# ---------------------------------------------------------------------------
# 18. Wrapper: success=false → PromoDiscountApplyError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_wrapper_success_false_raises_error() -> None:
    """Altegio response with success=false → PromoDiscountApplyError (fail-closed)."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {"success": False, "error": "card already used"}

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        with patch.object(settings, "promo_apply_discount_api_verified", True):
            with pytest.raises(PromoDiscountApplyError, match="unsuccessful response"):
                await apply_promo_discount_to_visit(
                    location_id=9001,
                    card_id=555,
                    program_id=1,
                    record_id=777,
                )


# ---------------------------------------------------------------------------
# 19. Wrapper: no success key → PromoDiscountApplyError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_wrapper_no_success_key_raises_error() -> None:
    """Altegio response without 'success' key → PromoDiscountApplyError (fail-closed)."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {"data": {"id": 42}}

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        with patch.object(settings, "promo_apply_discount_api_verified", True):
            with pytest.raises(PromoDiscountApplyError, match="unsuccessful response"):
                await apply_promo_discount_to_visit(
                    location_id=9001,
                    card_id=555,
                    program_id=1,
                    record_id=777,
                )


# ---------------------------------------------------------------------------
# 20. try_apply with API returning success=false → apply_failed, no MessageJob
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_failed_when_api_returns_success_false(session_maker) -> None:
    """When Altegio returns success=false, lead→apply_failed and no MessageJob created."""
    mock_api = AsyncMock(side_effect=PromoDiscountApplyError("unsuccessful response: success=false"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session, altegio_record_id=888)
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead.status == "apply_failed"
    meta = lead.meta or {}
    assert "discount_apply_error" in meta
    assert "unsuccessful response" in meta["discount_apply_error"]
    assert job is None


# ---------------------------------------------------------------------------
# 21. Idempotent notification: _ensure called twice → one MessageJob
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_notification_job_idempotent(session_maker) -> None:
    """_ensure_promo_discount_notification_job is idempotent: calling twice creates one job."""
    from datetime import timezone

    from altegio_bot.promo_discount_apply import _ensure_promo_discount_notification_job

    _now = datetime(2026, 5, 9, 12, 0, 0, tzinfo=timezone.utc)

    async with session_maker() as session:
        async with session.begin():
            client = await _seed_client(session)
            record = await _seed_record(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            await _ensure_promo_discount_notification_job(session, lead, client, record, _PHONE, _now)
            first_job_id = lead.meta.get("customer_notification_job_id")

            await _ensure_promo_discount_notification_job(session, lead, client, record, _PHONE, _now)
            second_job_id = lead.meta.get("customer_notification_job_id")

    assert first_job_id is not None
    assert second_job_id == first_job_id

    async with session_maker() as s:
        jobs = list(
            (await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))).scalars().all()
        )
    assert len(jobs) == 1


# ---------------------------------------------------------------------------
# 22. Missing booking_created_at → fail-closed, apply_skip_reason set
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_booking_timestamp_skips_apply(session_maker) -> None:
    """None booking_created_at → skip (fail-closed), no API call, apply_skip_reason set."""
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session)
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=None)

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead is not None
    assert lead.status == "issued"
    assert "missing booking created timestamp" in (lead.meta or {}).get("apply_skip_reason", "")
    assert job is None


# ---------------------------------------------------------------------------
# 23. booking_created_at before lead.issued_at → skip, predates promo
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_booking_predates_promo_skips_apply(session_maker) -> None:
    """booking_created_at before lead.issued_at → skip, meta records both timestamps."""
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))

    promo_issued_at = datetime(2026, 5, 8, 12, 0, 0, tzinfo=_UTC)
    booking_ts = datetime(2026, 5, 8, 11, 0, 0, tzinfo=_UTC)  # 1 hour before promo issued

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session)
            await _seed_service(session)
            lead = PromoLead(
                company_id=_COMPANY,
                phone_e164=_PHONE,
                campaign_name="welcome_discount",
                secret_code="aktion",
                discount_amount=Decimal("15"),
                discount_type="fixed",
                status="issued",
                issued_at=promo_issued_at,
                expires_at=_FUTURE,
                loyalty_card_id=_CARD_ID,
                location_id=_LOCATION,
                discount_program_id=_PROGRAM_ID,
                meta={"loyalty_card_issued": True},
            )
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=booking_ts)

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead is not None
    assert lead.status == "issued"
    meta = lead.meta or {}
    assert "predates promo lead" in meta.get("apply_skip_reason", "")
    assert meta.get("booking_created_at") is not None
    assert meta.get("promo_issued_at") is not None
    assert job is None


# ---------------------------------------------------------------------------
# 24. booking_created_at after lead.issued_at → apply proceeds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_booking_after_promo_proceeds(session_maker) -> None:
    """booking_created_at after lead.issued_at → timestamp guard passes, apply succeeds."""
    mock_api = AsyncMock(return_value=PromoDiscountApplyResult(applied=True, raw={"success": True}))

    promo_issued_at = datetime(2026, 5, 8, 12, 0, 0, tzinfo=_UTC)
    booking_ts = datetime(2026, 5, 8, 12, 1, 0, tzinfo=_UTC)  # 1 minute after promo issued

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session, altegio_record_id=555)
            await _seed_service(session)
            lead = PromoLead(
                company_id=_COMPANY,
                phone_e164=_PHONE,
                campaign_name="welcome_discount",
                secret_code="aktion",
                discount_amount=Decimal("15"),
                discount_type="fixed",
                status="issued",
                issued_at=promo_issued_at,
                expires_at=_FUTURE,
                loyalty_card_id=_CARD_ID,
                location_id=_LOCATION,
                discount_program_id=_PROGRAM_ID,
                meta={"loyalty_card_issued": True},
            )
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=booking_ts)

    mock_api.assert_called_once()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()

    assert lead is not None
    assert lead.status == "applied"


# ---------------------------------------------------------------------------
# 25. Booked lead bound to different record → skip, no API call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_booked_lead_different_record_skips(session_maker) -> None:
    """A booked lead already bound to old_record must not be rebound to current_record."""
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            old_record = await _seed_record(session, record_id=300, altegio_record_id=3001)
            current_record = await _seed_record(session, record_id=400, altegio_record_id=4001)
            await _seed_service(session, record_id=400)
            lead = _make_lead(status="booked")
            lead.record_id = old_record.id
            lead.altegio_record_id = old_record.altegio_record_id
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, current_record, _COMPANY, booking_created_at=_NOW)

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead is not None
    assert lead.status == "booked"
    assert lead.record_id == 300  # unchanged
    assert job is None


# ---------------------------------------------------------------------------
# 26. Booked lead same record → retry allowed, apply proceeds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_booked_lead_same_record_retry_allowed(session_maker) -> None:
    """A booked lead with same record_id is eligible for retry → apply proceeds."""
    mock_api = AsyncMock(return_value=PromoDiscountApplyResult(applied=True, raw={"success": True}))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session, altegio_record_id=777)
            await _seed_service(session)
            lead = _make_lead(status="booked")
            lead.record_id = record.id
            lead.altegio_record_id = record.altegio_record_id
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_api.assert_called_once()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()

    assert lead is not None
    assert lead.status == "applied"
