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
from sqlalchemy import select, update

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
    starts_at: datetime | None = None,
    comment: str | None = None,
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
        starts_at=starts_at,
        comment=comment,
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
    """Context manager that patches all required settings for discount apply.

    Defaults to promo_apply_mode="loyalty_program" so the 39 existing tests
    continue to exercise the legacy path. New record_price_override tests pass
    promo_apply_mode="record_price_override" explicitly via **overrides.
    """
    import contextlib

    defaults = {
        "promo_apply_discount_enabled": True,
        "promo_apply_discount_api_verified": True,
        "promo_allowed_service_ids": str(_ALLOWED_SERVICE),
        "promo_apply_mode": "loyalty_program",
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
    resolver = AsyncMock(side_effect=AssertionError("booking_created_at_resolver must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session)
            await _seed_service(session)
            # No PromoLead in DB

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(
                        session,
                        record,
                        _COMPANY,
                        booking_created_at_resolver=resolver,
                    )

    mock_api.assert_not_called()
    resolver.assert_not_called()


# ---------------------------------------------------------------------------
# 4. Expired PromoLead → excluded by SQL, no API call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_expired_lead_excluded(session_maker) -> None:
    past = datetime(2020, 1, 1, tzinfo=_UTC)
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))
    resolver = AsyncMock(side_effect=AssertionError("booking_created_at_resolver must not be called"))

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
                    await try_apply_promo_discount(
                        session,
                        record,
                        _COMPANY,
                        booking_created_at_resolver=resolver,
                    )

    mock_api.assert_not_called()
    resolver.assert_not_called()

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
    resolver = AsyncMock(side_effect=AssertionError("booking_created_at_resolver must not be called"))

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
                    await try_apply_promo_discount(
                        session,
                        record,
                        _COMPANY,
                        booking_created_at_resolver=resolver,
                    )

    mock_api.assert_not_called()
    resolver.assert_not_called()

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
    resolver = AsyncMock(side_effect=AssertionError("booking_created_at_resolver must not be called"))

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
                    await try_apply_promo_discount(
                        session,
                        record,
                        _COMPANY,
                        booking_created_at_resolver=resolver,
                    )

    mock_api.assert_not_called()
    resolver.assert_not_called()


# ---------------------------------------------------------------------------
# 7. Service not in allowlist → no API call, skip reason recorded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_service_not_allowed_skips_discount(session_maker) -> None:
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))
    resolver = AsyncMock(side_effect=AssertionError("booking_created_at_resolver must not be called"))

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
                    await try_apply_promo_discount(
                        session,
                        record,
                        _COMPANY,
                        booking_created_at_resolver=resolver,
                    )

    mock_api.assert_not_called()
    resolver.assert_not_called()

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
    resolver = AsyncMock(side_effect=AssertionError("booking_created_at_resolver must not be called"))

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
                    await try_apply_promo_discount(
                        session,
                        current_record,
                        _COMPANY,
                        booking_created_at_resolver=resolver,
                    )

    mock_api.assert_not_called()
    resolver.assert_not_called()

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
    mock_record.comment = None  # no promo marker — suppression must not fire

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


@pytest.mark.asyncio
async def test_lazy_resolver_returns_none_skips_apply(session_maker) -> None:
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))
    resolver = AsyncMock(return_value=None)

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
                    await try_apply_promo_discount(
                        session,
                        record,
                        _COMPANY,
                        booking_created_at_resolver=resolver,
                    )

    resolver.assert_awaited_once()
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


@pytest.mark.asyncio
async def test_lazy_resolver_after_local_checks_applies_discount(session_maker) -> None:
    mock_api = AsyncMock(return_value=PromoDiscountApplyResult(applied=True, raw={"success": True}))
    resolver = AsyncMock(return_value=datetime(2026, 5, 8, 12, 1, 0, tzinfo=_UTC))

    promo_issued_at = datetime(2026, 5, 8, 12, 0, 0, tzinfo=_UTC)

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session, altegio_record_id=555)
            await _seed_service(session)
            lead = _make_lead()
            lead.issued_at = promo_issued_at
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(
                        session,
                        record,
                        _COMPANY,
                        booking_created_at_resolver=resolver,
                    )

    resolver.assert_awaited_once()
    mock_api.assert_called_once_with(
        location_id=_LOCATION,
        card_id=int(_CARD_ID),
        program_id=_PROGRAM_ID,
        record_id=555,
    )

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        job = (await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))).scalar_one()

    assert lead.status == "applied"
    assert job.payload["phone_e164"] == _PHONE


@pytest.mark.asyncio
async def test_race_lead_becomes_applied_during_lazy_resolver_skips_apply(session_maker) -> None:
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))
    booking_ts = datetime(2026, 5, 8, 12, 1, 0, tzinfo=_UTC)

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session, altegio_record_id=555)
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            async def resolver() -> datetime:
                await session.execute(
                    update(PromoLead)
                    .where(PromoLead.id == lead_id)
                    .values(status="applied")
                    .execution_options(synchronize_session=False)
                )
                await session.flush()
                return booking_ts

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(
                        session,
                        record,
                        _COMPANY,
                        booking_created_at_resolver=resolver,
                    )

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead.status == "applied"
    assert job is None


@pytest.mark.asyncio
async def test_race_lead_becomes_booked_for_other_record_during_lazy_resolver_skips_apply(session_maker) -> None:
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))
    booking_ts = datetime(2026, 5, 8, 12, 1, 0, tzinfo=_UTC)

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            current_record = await _seed_record(session, record_id=200, altegio_record_id=555)
            other_record = await _seed_record(session, record_id=300, altegio_record_id=777)
            await _seed_service(session, record_id=current_record.id)
            lead = _make_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            async def resolver() -> datetime:
                await session.execute(
                    update(PromoLead)
                    .where(PromoLead.id == lead_id)
                    .values(
                        status="booked",
                        record_id=other_record.id,
                        altegio_record_id=other_record.altegio_record_id,
                    )
                    .execution_options(synchronize_session=False)
                )
                await session.flush()
                return booking_ts

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(
                        session,
                        current_record,
                        _COMPANY,
                        booking_created_at_resolver=resolver,
                    )

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead.status == "booked"
    assert lead.record_id == 300
    assert lead.altegio_record_id == 777
    assert job is None


@pytest.mark.asyncio
async def test_revalidation_does_not_switch_to_new_lead_after_lazy_resolver(session_maker) -> None:
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))
    booking_ts = datetime(2026, 5, 8, 12, 1, 0, tzinfo=_UTC)
    replacement_ids: list[int] = []

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session, altegio_record_id=555)
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()
            initial_lead_id = lead.id

            async def resolver() -> datetime:
                await session.execute(
                    update(PromoLead)
                    .where(PromoLead.id == initial_lead_id)
                    .values(status="cancelled", campaign_name="welcome_discount_superseded")
                    .execution_options(synchronize_session=False)
                )
                replacement = _make_lead()
                session.add(replacement)
                await session.flush()
                replacement_ids.append(replacement.id)
                return booking_ts

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(
                        session,
                        record,
                        _COMPANY,
                        booking_created_at_resolver=resolver,
                    )

    mock_api.assert_not_called()

    async with session_maker() as s:
        initial_lead = await s.get(PromoLead, initial_lead_id)
        replacement_lead = await s.get(PromoLead, replacement_ids[0])
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert initial_lead is not None
    assert initial_lead.status == "cancelled"
    assert initial_lead.campaign_name == "welcome_discount_superseded"
    assert replacement_lead is not None
    assert replacement_lead.status == "issued"
    assert job is None


@pytest.mark.asyncio
async def test_prior_attended_visit_appears_during_lazy_resolver_skips_apply(session_maker) -> None:
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))
    booking_ts = datetime(2026, 5, 8, 12, 1, 0, tzinfo=_UTC)

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session, record_id=200, altegio_record_id=555)
            await _seed_service(session, record_id=record.id)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            async def resolver() -> datetime:
                await _seed_record(
                    session,
                    record_id=300,
                    altegio_record_id=777,
                    client_id=100,
                    attendance=1,
                )
                return booking_ts

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(
                        session,
                        record,
                        _COMPANY,
                        booking_created_at_resolver=resolver,
                    )

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead.status == "issued"
    assert "prior attended visits" in (lead.meta or {}).get("apply_skip_reason", "")
    assert job is None


@pytest.mark.asyncio
async def test_service_becomes_disallowed_during_lazy_resolver_skips_apply(session_maker) -> None:
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))
    booking_ts = datetime(2026, 5, 8, 12, 1, 0, tzinfo=_UTC)

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session, altegio_record_id=555)
            await _seed_service(session, record_id=record.id, service_id=_ALLOWED_SERVICE)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            async def resolver() -> datetime:
                await session.execute(
                    update(RecordService)
                    .where(RecordService.record_id == record.id)
                    .values(service_id=_OTHER_SERVICE)
                    .execution_options(synchronize_session=False)
                )
                await session.flush()
                return booking_ts

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(
                        session,
                        record,
                        _COMPANY,
                        booking_created_at_resolver=resolver,
                    )

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead.status == "issued"
    assert "no allowed service" in (lead.meta or {}).get("apply_skip_reason", "")
    assert job is None


@pytest.mark.asyncio
async def test_explicit_booking_timestamp_does_not_call_lazy_resolver(session_maker) -> None:
    mock_api = AsyncMock(return_value=PromoDiscountApplyResult(applied=True, raw={"success": True}))
    resolver = AsyncMock(side_effect=AssertionError("booking_created_at_resolver must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session, altegio_record_id=555)
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(
                        session,
                        record,
                        _COMPANY,
                        booking_created_at=_NOW,
                        booking_created_at_resolver=resolver,
                    )

    resolver.assert_not_called()
    mock_api.assert_called_once()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()

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


@pytest.mark.asyncio
async def test_booked_lead_same_record_retry_allowed_after_lazy_resolver(session_maker) -> None:
    mock_api = AsyncMock(return_value=PromoDiscountApplyResult(applied=True, raw={"success": True}))
    resolver = AsyncMock(return_value=_NOW)

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
                    await try_apply_promo_discount(
                        session,
                        record,
                        _COMPANY,
                        booking_created_at_resolver=resolver,
                    )

    resolver.assert_awaited_once()
    mock_api.assert_called_once()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()

    assert lead is not None
    assert lead.status == "applied"


# =============================================================================
# MVP-3: record_price_override mode tests (promo_apply_mode="record_price_override")
# =============================================================================

_STARTS_AT = datetime(2026, 5, 8, 10, 0, 0, tzinfo=_UTC)
_ALTEGIO_RECORD_ID_PO = 9999  # "PO" = price-override group
_COST_ORIGINAL = 140.0
_DISCOUNT_AMOUNT = 15.0
_COST_DISCOUNTED = _COST_ORIGINAL - _DISCOUNT_AMOUNT  # 125.0


def _make_altegio_get_data(
    *,
    service_id: int = _ALLOWED_SERVICE,
    cost: float = _COST_ORIGINAL,
    comment: str | None = None,
    attendance: int = 0,
    extra_services: list[dict] | None = None,
) -> dict:
    """Fake GET /record response data dict (return value of fetch_altegio_record_for_update)."""
    services = [
        {
            "id": service_id,
            "title": "Wimpern",
            "cost": cost,
            "manual_cost": cost,
            "first_cost": cost,
            "discount": 0,
            "amount": 1,
        }
    ]
    if extra_services:
        services.extend(extra_services)
    return {
        "id": _ALTEGIO_RECORD_ID_PO,
        "comment": comment,
        "attendance": attendance,
        "visit_attendance": 0,
        "staff_id": 10,
        "client": {"id": 50},
        "save_if_busy": 1,
        "datetime": "2026-05-08T10:00:00+02:00",
        "seance_length": 3600,
        "sms_remain_hours": 24,
        "email_remain_hours": 24,
        "api_id": None,
        "custom_color": None,
        "record_labels": [],
        "services": services,
    }


def _make_put_response(
    *,
    service_id: int = _ALLOWED_SERVICE,
    original: float = _COST_ORIGINAL,
    discounted: float = _COST_DISCOUNTED,
) -> dict:
    """Fake PUT /record response including Altegio-computed discount percentage."""
    pct = round((original - discounted) / original * 100, 4) if original else 0.0
    return {
        "success": True,
        "data": {
            "services": [
                {
                    "id": service_id,
                    "cost": discounted,
                    "first_cost": original,
                    "discount": pct,
                }
            ]
        },
    }


# ---------------------------------------------------------------------------
# A. Simple case: 1 record same day, 1 allowed service → price override applied
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_price_override_simple_applies_and_queues_notification(session_maker) -> None:
    """
    record_price_override simple case:
    1 record same day + 1 allowed service → price override, lead→applied, notification queued.
    """
    mock_get = AsyncMock(return_value=_make_altegio_get_data())
    mock_put = AsyncMock(return_value=_make_put_response())

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
            )
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(
                    promo_apply_mode="record_price_override",
                    promo_discount_amount=_DISCOUNT_AMOUNT,
                ),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_get.assert_awaited_once_with(location_id=_LOCATION, record_id=_ALTEGIO_RECORD_ID_PO)
    mock_put.assert_awaited_once()

    # Verify the PUT new_services had the correct prices
    put_kwargs = mock_put.call_args.kwargs
    new_svcs = put_kwargs["new_services"]
    assert len(new_svcs) == 1
    assert new_svcs[0]["cost"] == _COST_DISCOUNTED
    assert new_svcs[0]["first_cost"] == _COST_ORIGINAL

    # Comment contains simple marker, not manual
    new_comment = put_kwargs["new_comment"]
    assert "[PromoLead:" in new_comment
    assert ":manual]" not in new_comment

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead.status == "applied"
    assert lead.applied_at is not None
    meta = lead.meta or {}
    assert meta.get("discount_apply_method") == "record_price_override"
    assert meta.get("original_cost") == _COST_ORIGINAL
    assert meta.get("discounted_cost") == _COST_DISCOUNTED
    assert meta.get("discount_amount") == _DISCOUNT_AMOUNT
    assert meta.get("altegio_record_update_status") == "success"
    assert meta.get("customer_notification") == "queued"
    assert meta.get("altegio_returned_discount") is not None  # percentage from Altegio
    assert job is not None
    assert job.payload["phone_e164"] == _PHONE


# ---------------------------------------------------------------------------
# B. Complex: multiple records same day → manual review comment, no price change
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_price_override_complex_multiple_records_same_day(session_maker) -> None:
    """
    record_price_override complex case:
    2 records on the same local day → manual-review PUT, lead→booked, no notification.
    """
    mock_get = AsyncMock(return_value=_make_altegio_get_data())
    mock_put = AsyncMock(return_value={"success": True, "data": {}})

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
            )
            # Second record for the same client on the same day
            await _seed_record(
                session,
                record_id=201,
                altegio_record_id=8888,
                client_id=100,
                starts_at=_STARTS_AT,
            )
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(promo_apply_mode="record_price_override"),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_put.assert_awaited_once()

    # In the complex case the services in the PUT should be unchanged (original prices)
    put_kwargs = mock_put.call_args.kwargs
    new_svcs = put_kwargs["new_services"]
    assert len(new_svcs) == 1
    assert new_svcs[0]["cost"] == _COST_ORIGINAL  # no price change

    # Comment must contain the manual marker
    new_comment = put_kwargs["new_comment"]
    assert ":manual]" in new_comment

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead.status == "booked"
    meta = lead.meta or {}
    assert meta.get("manual_review_required") is True
    assert meta.get("discount_apply_skip_reason") == "multiple_records_same_day"
    assert job is None


# ---------------------------------------------------------------------------
# C. Idempotency: promo marker already in local comment → skip GET+PUT
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_price_override_idempotent_marker_in_comment_skips_put(session_maker) -> None:
    """
    Promo marker in local record.comment → early return without GET or PUT.
    Prevents duplicate price override on webhook retry.
    """
    mock_get = AsyncMock(side_effect=AssertionError("GET must not be called"))
    mock_put = AsyncMock(side_effect=AssertionError("PUT must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
                comment="Previous note\n[PromoLead:42]",  # marker already present
            )
            await _seed_service(session)
            lead = _make_lead(status="booked")
            lead.record_id = record.id
            lead.altegio_record_id = record.altegio_record_id
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(promo_apply_mode="record_price_override"),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_get.assert_not_called()
    mock_put.assert_not_called()


# ---------------------------------------------------------------------------
# G. Disallowed service in record_price_override mode → same early-exit as legacy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_price_override_disallowed_service_skips(session_maker) -> None:
    """
    Service not in allowlist → _passes_mutable_local_guards returns False before
    mode routing. Outcome is identical to the legacy path.
    """
    mock_get = AsyncMock(side_effect=AssertionError("GET must not be called"))
    mock_put = AsyncMock(side_effect=AssertionError("PUT must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
            )
            await _seed_service(session, service_id=_OTHER_SERVICE)  # NOT in allowlist
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(promo_apply_mode="record_price_override"),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_get.assert_not_called()
    mock_put.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()

    assert lead.status == "issued"
    meta = lead.meta or {}
    assert "no allowed service" in (meta.get("apply_skip_reason") or meta.get("apply_error") or "")


# ---------------------------------------------------------------------------
# H. Multiple allowed services in record → complex case
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_price_override_multiple_allowed_services_complex_case(session_maker) -> None:
    """
    2 allowed services in the record → len(matching) == 2 → complex case (manual review).
    """
    _SERVICE_B = 67891

    mock_get = AsyncMock(
        return_value=_make_altegio_get_data(
            extra_services=[
                {
                    "id": _SERVICE_B,
                    "title": "Wimpern B",
                    "cost": 90.0,
                    "manual_cost": 90.0,
                    "first_cost": 90.0,
                    "discount": 0,
                    "amount": 1,
                }
            ]
        )
    )
    mock_put = AsyncMock(return_value={"success": True, "data": {}})

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
            )
            await _seed_service(session, record_id=200, service_id=_ALLOWED_SERVICE)
            session.add(RecordService(record_id=200, service_id=_SERVICE_B, title="Wimpern B", raw={}))
            await session.flush()
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                # Both services are allowed
                _base_settings_ctx(
                    promo_apply_mode="record_price_override",
                    promo_allowed_service_ids=f"{_ALLOWED_SERVICE},{_SERVICE_B}",
                ),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_put.assert_awaited_once()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead.status == "booked"
    meta = lead.meta or {}
    assert meta.get("manual_review_required") is True
    assert meta.get("discount_apply_skip_reason") == "multiple_allowed_services_in_record"
    assert job is None


# ---------------------------------------------------------------------------
# I. PUT error → lead→apply_failed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_price_override_put_error_sets_apply_failed(session_maker) -> None:
    """PUT /record raises AltegioRecordUpdateError → lead→apply_failed."""
    from altegio_bot.altegio_record_update import AltegioRecordUpdateError

    mock_get = AsyncMock(return_value=_make_altegio_get_data())
    mock_put = AsyncMock(side_effect=AltegioRecordUpdateError("PUT /record HTTP 500"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
            )
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(promo_apply_mode="record_price_override"),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead.status == "apply_failed"
    meta = lead.meta or {}
    assert "PUT /record HTTP 500" in (meta.get("discount_apply_error") or "")
    assert meta.get("discount_apply_attempted_at") is not None
    assert job is None


# ---------------------------------------------------------------------------
# K. Attendance guard: attended record → skip before GET
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_price_override_attendance_guard_skips(session_maker) -> None:
    """
    Record with attendance=1 → attendance guard fires before GET/PUT.
    Lead is transitioned issued→booked (booking acknowledged) but no price override.
    """
    mock_get = AsyncMock(side_effect=AssertionError("GET must not be called"))
    mock_put = AsyncMock(side_effect=AssertionError("PUT must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
                attendance=1,  # already attended
            )
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(promo_apply_mode="record_price_override"),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_get.assert_not_called()
    mock_put.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()

    # Step 7 transitions issued→booked; attendance guard fires inside _apply_via_*
    assert lead.status == "booked"
    meta = lead.meta or {}
    assert "already attended" in (meta.get("apply_skip_reason") or "")


# ---------------------------------------------------------------------------
# L. Discount calculation: 140 → 125, meta stores amounts correctly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_price_override_discount_calculation_stores_correct_meta(session_maker) -> None:
    """
    original_cost=140, discount=15 → new_cost=125.
    meta stores: original_cost, discounted_cost, discount_amount, altegio_returned_discount (%).
    """
    mock_get = AsyncMock(return_value=_make_altegio_get_data(cost=140.0))
    mock_put = AsyncMock(return_value=_make_put_response(original=140.0, discounted=125.0))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
            )
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(
                    promo_apply_mode="record_price_override",
                    promo_discount_amount=15.0,
                ),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()

    assert lead.status == "applied"
    meta = lead.meta or {}
    assert meta.get("original_cost") == 140.0
    assert meta.get("discounted_cost") == 125.0
    assert meta.get("discount_amount") == 15.0
    # Altegio returns a percentage (not the € amount); ~10.71 for 15€ off 140€
    returned = meta.get("altegio_returned_discount")
    assert returned is not None
    assert abs(returned - round(15.0 / 140.0 * 100, 4)) < 0.01


# =============================================================================
# P2.2: Settings validator — promo_apply_mode
# =============================================================================


class TestPromoApplyModeValidator:
    """@field_validator('promo_apply_mode') accepts the two valid values and
    rejects typos at startup."""

    def test_record_price_override_valid(self) -> None:
        from pydantic import ValidationError

        from altegio_bot.settings import Settings

        try:
            s = Settings(
                database_url="postgresql://localhost/test",
                altegio_webhook_secret="x",
                promo_apply_mode="record_price_override",
            )
            assert s.promo_apply_mode == "record_price_override"
        except ValidationError:
            pytest.fail("record_price_override should be a valid promo_apply_mode")

    def test_loyalty_program_valid(self) -> None:
        from pydantic import ValidationError

        from altegio_bot.settings import Settings

        try:
            s = Settings(
                database_url="postgresql://localhost/test",
                altegio_webhook_secret="x",
                promo_apply_mode="loyalty_program",
            )
            assert s.promo_apply_mode == "loyalty_program"
        except ValidationError:
            pytest.fail("loyalty_program should be a valid promo_apply_mode")

    def test_typo_raises_validation_error(self) -> None:
        from pydantic import ValidationError

        from altegio_bot.settings import Settings

        with pytest.raises(ValidationError, match="promo_apply_mode"):
            Settings(
                database_url="postgresql://localhost/test",
                altegio_webhook_secret="x",
                promo_apply_mode="record_price_overide",  # typo — one 'r' missing
            )


# =============================================================================
# P2.1: get_service_cost_for_discount — cost=0 must NOT fall through to manual_cost
# =============================================================================


class TestGetServiceCostForDiscount:
    """Verifies that cost=0 is treated as a valid price (not a fallback sentinel)."""

    def test_cost_zero_does_not_fall_through(self) -> None:
        """cost=0, manual_cost=100 → original_cost 0 (not 100), new_cost 0."""
        from altegio_bot.promo_discount_apply import get_service_cost_for_discount

        result = get_service_cost_for_discount({"cost": 0, "manual_cost": 100.0})
        assert result == 0.0

    def test_cost_none_uses_manual_cost(self) -> None:
        from altegio_bot.promo_discount_apply import get_service_cost_for_discount

        result = get_service_cost_for_discount({"cost": None, "manual_cost": 80.0})
        assert result == 80.0

    def test_both_absent_returns_zero(self) -> None:
        from altegio_bot.promo_discount_apply import get_service_cost_for_discount

        assert get_service_cost_for_discount({}) == 0.0

    def test_positive_cost_returned_directly(self) -> None:
        from altegio_bot.promo_discount_apply import get_service_cost_for_discount

        result = get_service_cost_for_discount({"cost": 140.0, "manual_cost": 999.0})
        assert result == 140.0


@pytest.mark.asyncio
async def test_price_override_zero_cost_service_new_cost_is_zero(session_maker) -> None:
    """cost=0 on a service → new_cost=max(0, 0-15)=0 (not negative, not using manual_cost).

    This verifies get_service_cost_for_discount is wired correctly in the apply path.
    """
    mock_get = AsyncMock(
        return_value=_make_altegio_get_data(cost=0.0)  # cost=0 — free service
    )
    mock_put = AsyncMock(return_value={"success": True, "data": {}})

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
            )
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(
                    promo_apply_mode="record_price_override",
                    promo_discount_amount=_DISCOUNT_AMOUNT,
                ),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    put_kwargs = mock_put.call_args.kwargs
    new_svcs = put_kwargs["new_services"]
    assert new_svcs[0]["cost"] == 0.0  # max(0, 0-15) = 0, not negative


# =============================================================================
# P1.1: Minimal PUT payload — service fields and client fields
# =============================================================================


class TestBuildMinimalServiceForPut:
    """build_minimal_service_for_put strips all non-essential fields."""

    def test_strips_extra_fields(self) -> None:
        from altegio_bot.altegio_record_update import build_minimal_service_for_put

        svc = {
            "id": 12345,
            "title": "Wimpern",
            "cost": 140.0,
            "first_cost": 140.0,
            "discount": 0,
            "manual_cost": 140.0,
            "cost_to_pay": 140.0,
            "cost_per_unit": 140.0,
            "assistants": [],
            "amount": 1,
        }
        result = build_minimal_service_for_put(svc)
        assert set(result.keys()) == {"id", "first_cost", "discount", "cost"}
        assert result["id"] == 12345
        assert result["cost"] == 140.0
        assert result["first_cost"] == 140.0
        assert result["discount"] == 0

    def test_override_all_price_fields(self) -> None:
        from altegio_bot.altegio_record_update import build_minimal_service_for_put

        svc = {"id": 12345, "cost": 140.0, "first_cost": 140.0, "discount": 0}
        result = build_minimal_service_for_put(
            svc, override_cost=125.0, override_first_cost=140.0, override_discount=15.0
        )
        assert result == {"id": 12345, "cost": 125.0, "first_cost": 140.0, "discount": 15.0}


class TestNormalizeRecordClientForPut:
    """normalize_record_client_for_put keeps only phone, name, email."""

    def test_strips_extra_client_fields(self) -> None:
        from altegio_bot.altegio_record_update import normalize_record_client_for_put

        record_data = {
            "client": {
                "id": 50,
                "display_name": "Test User",
                "phone": "+49160123456",
                "name": "Test",
                "email": "test@example.com",
                "some_extra": "should not appear",
            }
        }
        result = normalize_record_client_for_put(record_data)
        assert set(result.keys()) == {"phone", "name", "email"}
        assert result["phone"] == "+49160123456"
        assert result["email"] == "test@example.com"

    def test_missing_client_returns_empty_dict(self) -> None:
        from altegio_bot.altegio_record_update import normalize_record_client_for_put

        assert normalize_record_client_for_put({}) == {}

    def test_none_values_skipped(self) -> None:
        from altegio_bot.altegio_record_update import normalize_record_client_for_put

        record_data = {"client": {"phone": "+49160123456", "name": None, "email": None}}
        result = normalize_record_client_for_put(record_data)
        assert result == {"phone": "+49160123456"}


@pytest.mark.asyncio
async def test_price_override_simple_put_payload_minimal_fields(session_maker) -> None:
    """Simple apply: PUT new_services must contain only id/first_cost/discount/cost.

    Extra fields (title, manual_cost, cost_to_pay, amount, assistants, …) must
    be stripped before the PUT request is built.
    """
    rich_altegio_data = _make_altegio_get_data()
    # Inject extra fields that should be stripped
    rich_altegio_data["services"][0].update(
        {
            "title": "Wimpern",
            "manual_cost": _COST_ORIGINAL,
            "cost_to_pay": _COST_ORIGINAL,
            "cost_per_unit": _COST_ORIGINAL,
            "assistants": [],
            "amount": 1,
        }
    )
    mock_get = AsyncMock(return_value=rich_altegio_data)
    mock_put = AsyncMock(return_value=_make_put_response())

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
            )
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(
                    promo_apply_mode="record_price_override",
                    promo_discount_amount=_DISCOUNT_AMOUNT,
                ),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_put.assert_awaited_once()
    put_kwargs = mock_put.call_args.kwargs
    new_svcs = put_kwargs["new_services"]
    assert len(new_svcs) == 1
    target_svc = new_svcs[0]
    # Only the four minimal fields should be present
    assert set(target_svc.keys()) == {"id", "first_cost", "discount", "cost"}
    # Correct overridden values
    assert target_svc["cost"] == _COST_DISCOUNTED
    assert target_svc["first_cost"] == _COST_ORIGINAL
    assert target_svc["discount"] == _DISCOUNT_AMOUNT


# =============================================================================
# P2.4: SQL-level day filter in _count_same_day_records_for_client
# =============================================================================


@pytest.mark.asyncio
async def test_same_day_count_excludes_yesterday_and_tomorrow(session_maker) -> None:
    """Records on adjacent days must NOT be counted as same-day."""
    from datetime import timedelta

    from altegio_bot.promo_discount_apply import _count_same_day_records_for_client

    today = _STARTS_AT  # 2026-05-08 10:00 UTC
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            # Today
            await _seed_record(session, record_id=200, altegio_record_id=2001, starts_at=today)
            # Yesterday — must NOT be counted
            await _seed_record(session, record_id=201, altegio_record_id=2002, starts_at=yesterday)
            # Tomorrow — must NOT be counted
            await _seed_record(session, record_id=202, altegio_record_id=2003, starts_at=tomorrow)

            count = await _count_same_day_records_for_client(
                session,
                client_id=100,
                company_id=_COMPANY,
                reference_starts_at=today,
            )
    assert count == 1


@pytest.mark.asyncio
async def test_same_day_count_two_records_on_same_day(session_maker) -> None:
    """Two records on the same local calendar day → count == 2."""
    from altegio_bot.promo_discount_apply import _count_same_day_records_for_client

    morning = _STARTS_AT  # 2026-05-08 10:00 UTC
    afternoon = datetime(2026, 5, 8, 14, 0, 0, tzinfo=_UTC)

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session, record_id=200, altegio_record_id=2001, starts_at=morning)
            await _seed_record(session, record_id=201, altegio_record_id=2002, starts_at=afternoon)

            count = await _count_same_day_records_for_client(
                session,
                client_id=100,
                company_id=_COMPANY,
                reference_starts_at=morning,
            )
    assert count == 2


# =============================================================================
# P2.5: String service id in fresh Altegio data → still matches int allowlist
# =============================================================================


@pytest.mark.asyncio
async def test_price_override_string_service_id_in_altegio_matches_int_allowlist(session_maker) -> None:
    """Altegio GET returns service id as a string — must still match int allowlist entry."""
    altegio_data = _make_altegio_get_data()
    # Override service id to be a string (Altegio quirk)
    altegio_data["services"][0]["id"] = str(_ALLOWED_SERVICE)

    mock_get = AsyncMock(return_value=altegio_data)
    mock_put = AsyncMock(return_value=_make_put_response())

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
            )
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(
                    promo_apply_mode="record_price_override",
                    promo_discount_amount=_DISCOUNT_AMOUNT,
                ),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    # String id must not prevent the simple apply from succeeding
    mock_put.assert_awaited_once()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
    assert lead.status == "applied"


# =============================================================================
# P2.6: Missing error-path tests for record_price_override mode
# =============================================================================


@pytest.mark.asyncio
async def test_price_override_get_record_fails_sets_apply_failed(session_maker) -> None:
    """GET /record raises AltegioRecordUpdateError → lead→apply_failed, no PUT, no notification."""
    from altegio_bot.altegio_record_update import AltegioRecordUpdateError

    mock_get = AsyncMock(side_effect=AltegioRecordUpdateError("GET /record HTTP 404"))
    mock_put = AsyncMock(side_effect=AssertionError("PUT must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
            )
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(promo_apply_mode="record_price_override"),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_put.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead.status == "apply_failed"
    meta = lead.meta or {}
    assert "GET /record HTTP 404" in (meta.get("discount_apply_error") or "")
    assert meta.get("discount_apply_attempted_at") is not None
    assert job is None


@pytest.mark.asyncio
async def test_price_override_fresh_altegio_attendance_skips_override(session_maker) -> None:
    """Fresh Altegio data has attendance=1 (but local record has attendance=0) → skip PUT."""
    mock_get = AsyncMock(return_value=_make_altegio_get_data(attendance=1))  # fresh attended
    mock_put = AsyncMock(side_effect=AssertionError("PUT must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
                attendance=0,  # local record: not yet attended
            )
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(promo_apply_mode="record_price_override"),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_put.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()

    assert lead.status == "booked"  # issued→booked before the GET; skip sets apply_skip_reason
    meta = lead.meta or {}
    assert "already attended" in (meta.get("apply_skip_reason") or "")


@pytest.mark.asyncio
async def test_price_override_target_service_missing_in_altegio_sets_apply_failed(session_maker) -> None:
    """Allowed service not present in fresh Altegio services → lead→apply_failed."""
    # Altegio returns a *different* service id than the one in the allowlist
    mock_get = AsyncMock(return_value=_make_altegio_get_data(service_id=_OTHER_SERVICE))
    mock_put = AsyncMock(side_effect=AssertionError("PUT must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
            )
            await _seed_service(session)  # local record has _ALLOWED_SERVICE
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(promo_apply_mode="record_price_override"),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_put.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead.status == "apply_failed"
    meta = lead.meta or {}
    # P1.1: the fresh_allowed_service_ids re-run now catches the mismatch before
    # the individual target_svc lookup, so the error message refers to the
    # Altegio record services check rather than the per-service search.
    assert "fresh Altegio record" in (meta.get("discount_apply_error") or "")
    assert job is None


# =============================================================================
# P1.2: should_suppress_promo_origin_record_update — suppression window tests
# =============================================================================

_SUPPRESS_ALTEGIO_RECORD_ID = 7777


def _make_suppress_event(*, received_at: datetime) -> MagicMock:
    """Build a fake AltegioEvent with an explicit received_at for suppression tests."""
    event = MagicMock()
    event.received_at = received_at
    return event


@pytest.mark.asyncio
async def test_suppress_immediate_marker_in_window(session_maker) -> None:
    """Marker in comment + promo_record_put_at within 5 min → suppressed (fast path)."""
    from altegio_bot.promo_discount_apply import should_suppress_promo_origin_record_update

    put_at = datetime(2026, 5, 8, 20, 0, 0, tzinfo=_UTC)
    received_at = datetime(2026, 5, 8, 20, 2, 0, tzinfo=_UTC)  # 2 min after PUT

    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(status="applied")
            lead.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            lead.meta = {
                "loyalty_card_issued": True,
                "promo_record_put_at": put_at.isoformat(),
                "promo_record_put_altegio_record_id": _SUPPRESS_ALTEGIO_RECORD_ID,
                "promo_record_put_kind": "simple",
            }
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            # Record has the simple marker in its comment
            record = MagicMock()
            record.id = 200
            record.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            record.comment = f"Promo note\n[PromoLead:{lead_id}]"

            event = _make_suppress_event(received_at=received_at)

            result = await should_suppress_promo_origin_record_update(session, record, event)

    assert result is True


@pytest.mark.asyncio
async def test_suppress_scan_path_no_comment_in_window(session_maker) -> None:
    """No comment marker but PromoLead with matching altegio_record_id + fresh put_at → suppressed (slow path)."""
    from altegio_bot.promo_discount_apply import should_suppress_promo_origin_record_update

    put_at = datetime(2026, 5, 8, 20, 0, 0, tzinfo=_UTC)
    received_at = datetime(2026, 5, 8, 20, 3, 0, tzinfo=_UTC)  # 3 min after PUT

    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(status="applied")
            lead.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            lead.meta = {
                "loyalty_card_issued": True,
                "promo_record_put_at": put_at.isoformat(),
                "promo_record_put_altegio_record_id": _SUPPRESS_ALTEGIO_RECORD_ID,
                "promo_record_put_kind": "simple",
            }
            session.add(lead)
            await session.flush()

            # Record has NO comment (slow path)
            record = MagicMock()
            record.id = 200
            record.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            record.comment = None

            event = _make_suppress_event(received_at=received_at)

            result = await should_suppress_promo_origin_record_update(session, record, event)

    assert result is True


@pytest.mark.asyncio
async def test_suppress_outside_window_not_suppressed(session_maker) -> None:
    """Marker in comment but received_at more than 5 min after put_at → NOT suppressed."""
    from altegio_bot.promo_discount_apply import should_suppress_promo_origin_record_update

    put_at = datetime(2026, 5, 8, 20, 0, 0, tzinfo=_UTC)
    received_at = datetime(2026, 5, 8, 20, 10, 0, tzinfo=_UTC)  # 10 min after → outside window

    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(status="applied")
            lead.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            lead.meta = {
                "loyalty_card_issued": True,
                "promo_record_put_at": put_at.isoformat(),
                "promo_record_put_altegio_record_id": _SUPPRESS_ALTEGIO_RECORD_ID,
            }
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            record = MagicMock()
            record.id = 200
            record.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            record.comment = f"Note\n[PromoLead:{lead_id}]"

            event = _make_suppress_event(received_at=received_at)

            result = await should_suppress_promo_origin_record_update(session, record, event)

    assert result is False


@pytest.mark.asyncio
async def test_suppress_no_marker_no_meta_not_suppressed(session_maker) -> None:
    """No marker in comment and no PromoLead with promo_record_put_at → NOT suppressed."""
    from altegio_bot.promo_discount_apply import should_suppress_promo_origin_record_update

    received_at = datetime(2026, 5, 8, 20, 0, 0, tzinfo=_UTC)

    async with session_maker() as session:
        async with session.begin():
            # A lead exists but has no promo_record_put_at in meta
            lead = _make_lead(status="applied")
            lead.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            lead.meta = {"loyalty_card_issued": True}  # no promo_record_put_at
            session.add(lead)
            await session.flush()

            record = MagicMock()
            record.id = 200
            record.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            record.comment = "Normal edit by salon staff"  # no promo marker

            event = _make_suppress_event(received_at=received_at)

            result = await should_suppress_promo_origin_record_update(session, record, event)

    assert result is False


# =============================================================================
# P1.2: meta stored after successful PUT — promo_record_put_* fields
# =============================================================================


@pytest.mark.asyncio
async def test_price_override_simple_stores_promo_record_put_meta(session_maker) -> None:
    """Simple apply stores promo_record_put_at, _marker, _record_id, _altegio_record_id, _kind in meta."""
    mock_get = AsyncMock(return_value=_make_altegio_get_data())
    mock_put = AsyncMock(return_value=_make_put_response())

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
            )
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(promo_apply_mode="record_price_override"),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()

    assert lead.status == "applied"
    meta = lead.meta or {}
    assert meta.get("promo_record_put_at") is not None
    assert meta.get("promo_record_put_kind") == "simple"
    assert meta.get("promo_record_put_altegio_record_id") == _ALTEGIO_RECORD_ID_PO
    assert meta.get("promo_record_put_record_id") == 200
    lead_id = lead.id
    assert meta.get("promo_record_put_marker") == f"[PromoLead:{lead_id}]"


@pytest.mark.asyncio
async def test_price_override_complex_stores_promo_record_put_meta(session_maker) -> None:
    """Complex (manual) case stores promo_record_put_kind='manual' and manual marker in meta."""
    mock_get = AsyncMock(return_value=_make_altegio_get_data())
    mock_put = AsyncMock(return_value={"success": True, "data": {}})

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
            )
            # Second record same day → complex case
            await _seed_record(
                session,
                record_id=201,
                altegio_record_id=8888,
                client_id=100,
                starts_at=_STARTS_AT,
            )
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(promo_apply_mode="record_price_override"),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()

    assert lead.status == "booked"
    meta = lead.meta or {}
    assert meta.get("promo_record_put_at") is not None
    assert meta.get("promo_record_put_kind") == "manual"
    lead_id = lead.id
    assert meta.get("promo_record_put_marker") == f"[PromoLead:{lead_id}:manual]"


# =============================================================================
# P1.1: Re-run mutable guards after GET — 3 new tests
# =============================================================================


@pytest.mark.asyncio
async def test_price_override_p11_concurrent_same_day_record_forces_complex(session_maker) -> None:
    """A second same-day record appears AFTER the initial is_simple=True decision
    (simulated by seeding it before the GET mock so the DB re-read sees it) →
    post-GET re-run forces complex/manual case.
    """
    mock_get = AsyncMock(return_value=_make_altegio_get_data())
    mock_put = AsyncMock(return_value={"success": True, "data": {}})

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
            )
            # Second record on the same day seeded NOW (before try_apply is called).
            # The pre-GET is_simple check only runs once; the post-GET re-run sees this.
            await _seed_record(
                session,
                record_id=201,
                altegio_record_id=8881,
                client_id=100,
                starts_at=_STARTS_AT,
            )
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(promo_apply_mode="record_price_override"),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_put.assert_awaited_once()
    put_kwargs = mock_put.call_args.kwargs
    # Complex case: comment must have manual marker, price unchanged
    assert ":manual]" in put_kwargs["new_comment"]
    new_svcs = put_kwargs["new_services"]
    assert new_svcs[0]["cost"] == _COST_ORIGINAL

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()

    assert lead.status == "booked"
    meta = lead.meta or {}
    assert meta.get("discount_apply_skip_reason") == "multiple_records_same_day"


@pytest.mark.asyncio
async def test_price_override_p11_fresh_altegio_service_not_in_allowlist_fails_closed(session_maker) -> None:
    """Altegio GET returns a service NOT in the allowlist → fresh_allowed_service_ids
    is empty → fail-closed (apply_failed), no PUT called.
    """
    # Altegio returns _OTHER_SERVICE only; local record has _ALLOWED_SERVICE
    mock_get = AsyncMock(return_value=_make_altegio_get_data(service_id=_OTHER_SERVICE))
    mock_put = AsyncMock(side_effect=AssertionError("PUT must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
            )
            await _seed_service(session)  # local: _ALLOWED_SERVICE
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(promo_apply_mode="record_price_override"),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_put.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()

    assert lead.status == "apply_failed"
    meta = lead.meta or {}
    assert "fresh Altegio record" in (meta.get("discount_apply_error") or "")


@pytest.mark.asyncio
async def test_price_override_p11_two_allowed_services_in_altegio_forces_complex(session_maker) -> None:
    """Altegio returns two allowed services (local record has one) → fresh_allowed
    disagrees with local matching → complex case (is_simple=False).
    """
    _SERVICE_B = 67892

    # Altegio record has both the primary allowed service and a second allowed one
    altegio_data = _make_altegio_get_data(
        extra_services=[
            {
                "id": _SERVICE_B,
                "title": "Extra",
                "cost": 50.0,
                "manual_cost": 50.0,
                "first_cost": 50.0,
                "discount": 0,
                "amount": 1,
            }
        ]
    )
    mock_get = AsyncMock(return_value=altegio_data)
    mock_put = AsyncMock(return_value={"success": True, "data": {}})

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
            )
            await _seed_service(session)  # local: _ALLOWED_SERVICE only
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                # Both _ALLOWED_SERVICE and _SERVICE_B are in the allowlist
                _base_settings_ctx(
                    promo_apply_mode="record_price_override",
                    promo_allowed_service_ids=f"{_ALLOWED_SERVICE},{_SERVICE_B}",
                ),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_put.assert_awaited_once()
    put_kwargs = mock_put.call_args.kwargs
    assert ":manual]" in put_kwargs["new_comment"]

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()

    assert lead.status == "booked"
    meta = lead.meta or {}
    # Two allowed services in Altegio → len(fresh_allowed_service_ids) > 1 → complex
    assert meta.get("manual_review_required") is True


# =============================================================================
# P1.2: parse_promo_marker + recovery tests — 3 new tests
# =============================================================================


@pytest.mark.asyncio
async def test_price_override_p12_local_marker_from_this_lead_recovers_applied(session_maker) -> None:
    """Local record.comment already has THIS lead's simple marker → recover to applied
    without GET or PUT, and ensure notification job is queued.
    """
    mock_get = AsyncMock(side_effect=AssertionError("GET must not be called"))
    mock_put = AsyncMock(side_effect=AssertionError("PUT must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            lead = _make_lead(status="booked")
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            # Seed record AFTER flushing lead so lead.id is known
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
                comment=f"Note\n[PromoLead:{lead_id}]",  # THIS lead's simple marker
            )
            lead.record_id = record.id
            lead.altegio_record_id = record.altegio_record_id
            await _seed_service(session)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(promo_apply_mode="record_price_override"),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_get.assert_not_called()
    mock_put.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead.status == "applied"
    assert job is not None  # notification queued on recovery


@pytest.mark.asyncio
async def test_price_override_p12_altegio_marker_from_this_lead_recovers_applied(session_maker) -> None:
    """Fresh Altegio comment (after GET) already has THIS lead's simple marker →
    recover to applied without PUT, notification queued.
    """
    mock_put = AsyncMock(side_effect=AssertionError("PUT must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            lead = _make_lead(status="booked")
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            # GET returns data with THIS lead's marker in the Altegio comment
            altegio_data = _make_altegio_get_data(
                comment=f"Note\n[PromoLead:{lead_id}]",
            )
            mock_get = AsyncMock(return_value=altegio_data)

            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
                comment=None,  # local comment has no marker — must do GET
            )
            lead.record_id = record.id
            lead.altegio_record_id = record.altegio_record_id
            await _seed_service(session)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(promo_apply_mode="record_price_override"),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_get.assert_awaited_once()
    mock_put.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead.status == "applied"
    assert job is not None


@pytest.mark.asyncio
async def test_price_override_p12_different_lead_marker_fails_closed(session_maker) -> None:
    """Local record.comment has a marker from a DIFFERENT lead → fail-closed
    (apply_failed), no GET or PUT called.
    """
    mock_get = AsyncMock(side_effect=AssertionError("GET must not be called"))
    mock_put = AsyncMock(side_effect=AssertionError("PUT must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(
                session,
                record_id=200,
                altegio_record_id=_ALTEGIO_RECORD_ID_PO,
                starts_at=_STARTS_AT,
                comment="Note\n[PromoLead:999999]",  # marker from a non-existent different lead
            )
            await _seed_service(session)
            lead = _make_lead(status="booked")
            lead.record_id = record.id
            lead.altegio_record_id = record.altegio_record_id
            session.add(lead)
            await session.flush()

            with (
                patch("altegio_bot.promo_discount_apply.fetch_altegio_record_for_update", mock_get),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _base_settings_ctx(promo_apply_mode="record_price_override"),
            ):
                await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    mock_get.assert_not_called()
    mock_put.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one()

    assert lead.status == "apply_failed"
    meta = lead.meta or {}
    assert "different lead" in (meta.get("discount_apply_error") or "")


# =============================================================================
# P2.2: _value_or_zero in build_minimal_service_for_put — zero values preserved
# =============================================================================


class TestValueOrZero:
    """_value_or_zero uses explicit None-check not truthiness, so 0 is preserved."""

    def test_zero_cost_preserved(self) -> None:
        from altegio_bot.altegio_record_update import build_minimal_service_for_put

        svc = {"id": 1, "cost": 0, "first_cost": 0, "discount": 0}
        result = build_minimal_service_for_put(svc)
        assert result["cost"] == 0.0
        assert result["first_cost"] == 0.0
        assert result["discount"] == 0.0

    def test_zero_fallback_when_none(self) -> None:
        from altegio_bot.altegio_record_update import build_minimal_service_for_put

        svc = {"id": 1, "cost": None, "first_cost": None, "discount": None}
        result = build_minimal_service_for_put(svc)
        assert result["cost"] == 0.0
        assert result["first_cost"] == 0.0
        assert result["discount"] == 0.0


# =============================================================================
# P2.3: Suppression slow path — stale/wrong metadata must NOT suppress
# =============================================================================


@pytest.mark.asyncio
async def test_suppress_slow_path_wrong_altegio_id_in_meta_not_suppressed(session_maker) -> None:
    """Slow path: candidate has promo_record_put_altegio_record_id that does NOT match
    the record's altegio_record_id → candidate is skipped → NOT suppressed.
    """
    from altegio_bot.promo_discount_apply import should_suppress_promo_origin_record_update

    put_at = datetime(2026, 5, 8, 20, 0, 0, tzinfo=_UTC)
    received_at = datetime(2026, 5, 8, 20, 2, 0, tzinfo=_UTC)

    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(status="applied")
            lead.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            lead.meta = {
                "loyalty_card_issued": True,
                "promo_record_put_at": put_at.isoformat(),
                # stored id is DIFFERENT from record.altegio_record_id
                "promo_record_put_altegio_record_id": _SUPPRESS_ALTEGIO_RECORD_ID + 1,
                "promo_record_put_kind": "simple",
            }
            session.add(lead)
            await session.flush()

            record = MagicMock()
            record.id = 200
            record.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            record.comment = None  # force slow path

            event = _make_suppress_event(received_at=received_at)
            result = await should_suppress_promo_origin_record_update(session, record, event)

    assert result is False


@pytest.mark.asyncio
async def test_suppress_slow_path_missing_put_kind_not_suppressed(session_maker) -> None:
    """Slow path: candidate meta has promo_record_put_at but no promo_record_put_kind
    (old schema version) → candidate is skipped → NOT suppressed.
    """
    from altegio_bot.promo_discount_apply import should_suppress_promo_origin_record_update

    put_at = datetime(2026, 5, 8, 20, 0, 0, tzinfo=_UTC)
    received_at = datetime(2026, 5, 8, 20, 2, 0, tzinfo=_UTC)

    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(status="applied")
            lead.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            lead.meta = {
                "loyalty_card_issued": True,
                "promo_record_put_at": put_at.isoformat(),
                "promo_record_put_altegio_record_id": _SUPPRESS_ALTEGIO_RECORD_ID,
                # promo_record_put_kind intentionally absent (old schema)
            }
            session.add(lead)
            await session.flush()

            record = MagicMock()
            record.id = 200
            record.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            record.comment = None  # force slow path

            event = _make_suppress_event(received_at=received_at)
            result = await should_suppress_promo_origin_record_update(session, record, event)

    assert result is False


# =============================================================================
# P2.5: Suppression window boundary tests — delta=300 True, 301 False, <0 False
# =============================================================================


@pytest.mark.asyncio
async def test_suppress_exactly_at_window_boundary_suppressed(session_maker) -> None:
    """delta == _SUPPRESS_WINDOW_SEC (300s) is still within window → suppressed."""
    from altegio_bot.promo_discount_apply import should_suppress_promo_origin_record_update

    put_at = datetime(2026, 5, 8, 20, 0, 0, tzinfo=_UTC)
    received_at = put_at + __import__("datetime").timedelta(seconds=300)

    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(status="applied")
            lead.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            lead.meta = {
                "loyalty_card_issued": True,
                "promo_record_put_at": put_at.isoformat(),
                "promo_record_put_altegio_record_id": _SUPPRESS_ALTEGIO_RECORD_ID,
                "promo_record_put_kind": "simple",
            }
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            record = MagicMock()
            record.id = 200
            record.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            record.comment = f"[PromoLead:{lead_id}]"
            event = _make_suppress_event(received_at=received_at)
            result = await should_suppress_promo_origin_record_update(session, record, event)

    assert result is True


@pytest.mark.asyncio
async def test_suppress_one_second_past_boundary_not_suppressed(session_maker) -> None:
    """delta == 301s (one second past window) → NOT suppressed."""
    from altegio_bot.promo_discount_apply import should_suppress_promo_origin_record_update

    put_at = datetime(2026, 5, 8, 20, 0, 0, tzinfo=_UTC)
    received_at = put_at + __import__("datetime").timedelta(seconds=301)

    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(status="applied")
            lead.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            lead.meta = {
                "loyalty_card_issued": True,
                "promo_record_put_at": put_at.isoformat(),
                "promo_record_put_altegio_record_id": _SUPPRESS_ALTEGIO_RECORD_ID,
                "promo_record_put_kind": "simple",
            }
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            record = MagicMock()
            record.id = 200
            record.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            record.comment = f"[PromoLead:{lead_id}]"
            event = _make_suppress_event(received_at=received_at)
            result = await should_suppress_promo_origin_record_update(session, record, event)

    assert result is False


@pytest.mark.asyncio
async def test_suppress_event_before_put_not_suppressed(session_maker) -> None:
    """Event received BEFORE put_at (negative delta) → NOT suppressed."""
    from altegio_bot.promo_discount_apply import should_suppress_promo_origin_record_update

    put_at = datetime(2026, 5, 8, 20, 0, 0, tzinfo=_UTC)
    received_at = put_at - __import__("datetime").timedelta(seconds=10)

    async with session_maker() as session:
        async with session.begin():
            lead = _make_lead(status="applied")
            lead.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            lead.meta = {
                "loyalty_card_issued": True,
                "promo_record_put_at": put_at.isoformat(),
                "promo_record_put_altegio_record_id": _SUPPRESS_ALTEGIO_RECORD_ID,
                "promo_record_put_kind": "simple",
            }
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            record = MagicMock()
            record.id = 200
            record.altegio_record_id = _SUPPRESS_ALTEGIO_RECORD_ID
            record.comment = f"[PromoLead:{lead_id}]"
            event = _make_suppress_event(received_at=received_at)
            result = await should_suppress_promo_origin_record_update(session, record, event)

    assert result is False


# =============================================================================
# P2.7: Settings validator tests isolated from environment
# =============================================================================


class TestPromoApplyModeValidatorEnvIsolated:
    """Validator tests using patch.dict(os.environ) to ensure env isolation."""

    def test_valid_value_from_env(self) -> None:
        import os

        from pydantic import ValidationError

        from altegio_bot.settings import Settings

        with __import__("unittest.mock", fromlist=["patch"]).patch.dict(
            os.environ, {"PROMO_APPLY_MODE": "record_price_override"}, clear=False
        ):
            try:
                s = Settings(database_url="postgresql://localhost/test", altegio_webhook_secret="x")
                assert s.promo_apply_mode == "record_price_override"
            except ValidationError:
                pytest.fail("record_price_override should be valid")

    def test_invalid_value_from_env_raises(self) -> None:
        import os

        from pydantic import ValidationError

        from altegio_bot.settings import Settings

        with __import__("unittest.mock", fromlist=["patch"]).patch.dict(
            os.environ, {"PROMO_APPLY_MODE": "bad_value"}, clear=False
        ):
            with pytest.raises(ValidationError, match="promo_apply_mode"):
                Settings(database_url="postgresql://localhost/test", altegio_webhook_secret="x")


# =============================================================================
# P2.8: normalize_record_client_for_put — empty-string policy
# =============================================================================


class TestNormalizeRecordClientEmptyString:
    """Empty-string email/phone/name must be excluded (same as None)."""

    def test_empty_email_excluded(self) -> None:
        from altegio_bot.altegio_record_update import normalize_record_client_for_put

        record_data = {"client": {"phone": "+49160123456", "name": "Test", "email": ""}}
        result = normalize_record_client_for_put(record_data)
        assert "email" not in result
        assert result["phone"] == "+49160123456"

    def test_empty_phone_excluded(self) -> None:
        from altegio_bot.altegio_record_update import normalize_record_client_for_put

        record_data = {"client": {"phone": "", "name": "Test", "email": "x@y.com"}}
        result = normalize_record_client_for_put(record_data)
        assert "phone" not in result
        assert result["email"] == "x@y.com"

    def test_all_empty_returns_empty_dict(self) -> None:
        from altegio_bot.altegio_record_update import normalize_record_client_for_put

        record_data = {"client": {"phone": "", "name": "", "email": ""}}
        assert normalize_record_client_for_put(record_data) == {}


# =============================================================================
# P2.9: parse_service_id unit tests
# =============================================================================


class TestParseServiceId:
    """parse_service_id coerces int/str/None values correctly."""

    def test_int_passthrough(self) -> None:
        from altegio_bot.promo_discount_apply import parse_service_id

        assert parse_service_id(12345) == 12345

    def test_string_coerced_to_int(self) -> None:
        from altegio_bot.promo_discount_apply import parse_service_id

        assert parse_service_id("12345") == 12345

    def test_none_returns_none(self) -> None:
        from altegio_bot.promo_discount_apply import parse_service_id

        assert parse_service_id(None) is None

    def test_invalid_string_returns_none(self) -> None:
        from altegio_bot.promo_discount_apply import parse_service_id

        assert parse_service_id("not_a_number") is None


# =============================================================================
# P3: Remove unused _SUPPRESS_LEAD_ID_PLACEHOLDER (cleanup — constant removed above)
# =============================================================================
# _SUPPRESS_LEAD_ID_PLACEHOLDER was removed; tests reference lead.id directly.


# =============================================================================
# P2.1: parse_promo_marker unit tests
# =============================================================================


class TestParsePromoMarker:
    """parse_promo_marker correctly extracts lead_id and kind from comments."""

    def test_simple_marker(self) -> None:
        from altegio_bot.promo_discount_apply import parse_promo_marker

        result = parse_promo_marker("Some note\n[PromoLead:42]")
        assert result == {"lead_id": 42, "kind": "simple"}

    def test_manual_marker(self) -> None:
        from altegio_bot.promo_discount_apply import parse_promo_marker

        result = parse_promo_marker("Note [PromoLead:7:manual] text")
        assert result == {"lead_id": 7, "kind": "manual"}

    def test_none_comment_returns_none(self) -> None:
        from altegio_bot.promo_discount_apply import parse_promo_marker

        assert parse_promo_marker(None) is None

    def test_no_marker_returns_none(self) -> None:
        from altegio_bot.promo_discount_apply import parse_promo_marker

        assert parse_promo_marker("Regular salon comment") is None
