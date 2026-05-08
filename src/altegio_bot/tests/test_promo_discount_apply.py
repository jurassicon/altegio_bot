"""Tests: promo discount application to Altegio visits.

Covers:
1.  Feature disabled (promo_apply_discount_enabled=False) — API not called, status unchanged.
2.  API not verified (promo_apply_discount_api_verified=False) — API not called,
    meta.discount_apply_error set, status remains booked.
3.  No active PromoLead — no API call, no OutboxMessage.
4.  Expired PromoLead — no API call (excluded by SQL query).
5.  Active issued lead + allowed service + verified API → applied, OutboxMessage created.
6.  Active lead without loyalty_card_id — not found by SQL, no API call.
7.  Active lead but service not in allowlist — no API call, meta.apply_skip_reason set.
8.  API failure — status='apply_failed', meta.discount_apply_error set, no OutboxMessage.
9.  Duplicate webhook (already applied) — no second API call, no duplicate OutboxMessage.
10. Old client edge: prior attended visit (excluding current) → skip discount.
11. Direct wrapper: success response → PromoDiscountApplyResult(applied=True).
12. Direct wrapper: HTTP error → PromoDiscountApplyError.
13. Direct wrapper: invalid JSON → PromoDiscountApplyError.
14. Direct wrapper: unexpected response shape → PromoDiscountApplyError.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from sqlalchemy import select

from altegio_bot.models.models import Client, OutboxMessage, PromoLead, Record, RecordService
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
    status: str = "issued",
    expires_at: datetime = _FUTURE,
    loyalty_card_id: str | None = _CARD_ID,
    location_id: int | None = _LOCATION,
    discount_program_id: str | None = _PROGRAM_ID,
    meta: dict | None = None,
) -> PromoLead:
    return PromoLead(
        company_id=_COMPANY,
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
                    await try_apply_promo_discount(session, record, _COMPANY)

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
# 5. Happy path: applied + OutboxMessage created
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_happy_path_applies_discount_and_creates_outbox(session_maker) -> None:
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
                    await try_apply_promo_discount(session, record, _COMPANY)

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

    async with session_maker() as s:
        outbox = (
            await s.execute(select(OutboxMessage).where(OutboxMessage.template_code == "wa_promo_discount_applied"))
        ).scalar_one_or_none()

    assert outbox is not None
    assert outbox.phone_e164 == _PHONE
    assert outbox.status == "queued"
    assert outbox.message_source == "bot"
    assert "Neukundenrabatt" in outbox.body
    assert "Gute Nachricht" in outbox.body


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
                    await try_apply_promo_discount(session, record, _COMPANY)

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()

    assert lead is not None
    assert lead.status == "issued"
    meta = lead.meta or {}
    assert meta.get("apply_skip_reason") is not None
    assert "no allowed service" in meta["apply_skip_reason"]


# ---------------------------------------------------------------------------
# 8. API failure → status='apply_failed', no OutboxMessage
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
                    await try_apply_promo_discount(session, record, _COMPANY)

    mock_api.assert_called_once()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == _PHONE))).scalar_one_or_none()

    assert lead is not None
    assert lead.status == "apply_failed"
    meta = lead.meta or {}
    assert "Altegio 503" in (meta.get("discount_apply_error") or "")
    assert meta.get("discount_apply_attempted_at") is not None

    async with session_maker() as s:
        outbox = (
            await s.execute(select(OutboxMessage).where(OutboxMessage.template_code == "wa_promo_discount_applied"))
        ).scalar_one_or_none()

    assert outbox is None, "No OutboxMessage must be created when API fails"


# ---------------------------------------------------------------------------
# 9. Duplicate webhook → no second API call, no duplicate OutboxMessage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_webhook_idempotent(session_maker) -> None:
    mock_api = AsyncMock(return_value=PromoDiscountApplyResult(applied=True, raw={"success": True}))

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

            # First call (already applied)
            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY)

            # Second call (same record, same lead)
            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY)

    mock_api.assert_not_called()

    async with session_maker() as s:
        outboxes = (
            (await s.execute(select(OutboxMessage).where(OutboxMessage.template_code == "wa_promo_discount_applied")))
            .scalars()
            .all()
        )

    assert len(outboxes) == 0, "No OutboxMessage must be created for already-applied lead"


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
                    await try_apply_promo_discount(session, current_record, _COMPANY)

    mock_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == other_phone))).scalar_one_or_none()

    assert lead is not None
    meta = lead.meta or {}
    assert "prior attended visits" in (meta.get("apply_skip_reason") or meta.get("apply_error") or "")


# ---------------------------------------------------------------------------
# 11–14. Direct wrapper unit tests
# ---------------------------------------------------------------------------


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

        with pytest.raises(PromoDiscountApplyError, match="unexpected response shape"):
            await apply_promo_discount_to_visit(
                location_id=9001,
                card_id=555,
                program_id=1,
                record_id=777,
            )
