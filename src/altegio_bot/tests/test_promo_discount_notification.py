"""Tests: customer WhatsApp notification after promo discount application.

Covers:
1.  notification_job_created_on_apply: successful apply queues a MessageJob with correct
    dedupe_key, client_id, record_id and stores job_id in lead.meta.
2.  notification_meta_fields: lead.meta contains customer_notification='queued',
    customer_notification_job_id, customer_notification_dedupe_key after apply.
3.  notification_job_not_created_when_api_not_verified: API gate blocks → no MessageJob.
4.  notification_job_not_created_when_service_not_allowed: service filter → no MessageJob.
5.  notification_job_idempotent: existing job → no duplicate, lead.meta points to existing.
6.  notification_body_content: message text contains required German UX strings.
7.  outbox_worker_sends_promo_discount_applied: worker processes job, OutboxMessage 'sent',
    job.status='done'.
8.  outbox_worker_fails_missing_body: missing payload body → job.status='failed'.
9.  outbox_worker_fails_no_sender: no active sender → job.status='failed'.
10. outbox_worker_reconciles_lead_on_success: worker updates PromoLead.meta to 'sent'.
11. outbox_worker_reconciles_lead_on_final_failure: worker updates PromoLead.meta to 'failed'.
12. outbox_worker_retryable_failure_leaves_queued: retryable error keeps 'queued',
    records customer_notification_last_error.
13. outbox_worker_missing_promo_lead_id_does_not_break_send: no promo_lead_id in payload
    → send succeeds, job.status='done'.
14. ensure_notification_job_race_recovery: concurrent IntegrityError → recover, meta
    points to existing job.
15. outbox_worker_no_sender_reconciles_lead: no active sender → job='failed',
    PromoLead.meta.customer_notification='failed'.
16. outbox_worker_missing_body_reconciles_lead: missing body → job='failed',
    PromoLead.meta.customer_notification='failed'.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from altegio_bot.models.models import Client, MessageJob, PromoLead, Record, RecordService
from altegio_bot.promo_discount_apply import (
    PromoDiscountApplyResult,
    try_apply_promo_discount,
)
from altegio_bot.settings import settings
from altegio_bot.workers import outbox_worker as ow

_UTC = timezone.utc
_FUTURE = datetime(2099, 1, 1, tzinfo=_UTC)
_NOW = datetime(2026, 5, 9, 12, 0, 0, tzinfo=_UTC)
_PHONE = "+4916099887766"
_COMPANY = 1
_LOCATION = 9001
_CARD_ID = "555"
_PROGRAM_ID = "dp_001"
_ALLOWED_SERVICE = 12345
_OTHER_SERVICE = 99999

_BODY = (
    "Gute Nachricht 🎁\n\n"
    "Ihr Neukundenrabatt wurde Ihrer Buchung zugeordnet.\n\n"
    "Bitte beachten Sie: In der Online-Buchung und in der ersten Bestätigung "
    "können noch reguläre Preise angezeigt werden. Unser Team sieht den Rabatt "
    "in Ihrer Buchung.\n\n"
    "Wir freuen uns auf Ihren Besuch 💙"
)


# ---------------------------------------------------------------------------
# DB seed helpers
# ---------------------------------------------------------------------------


async def _seed_client(session, *, client_id: int = 100, phone: str = _PHONE) -> Client:
    c = Client(
        id=client_id,
        company_id=_COMPANY,
        altegio_client_id=client_id,
        phone_e164=phone,
        display_name="Test Kunde",
        raw={},
    )
    session.add(c)
    await session.flush()
    return c


async def _seed_record(session, *, record_id: int = 200, altegio_record_id: int = 999) -> Record:
    r = Record(
        id=record_id,
        company_id=_COMPANY,
        altegio_record_id=altegio_record_id,
        client_id=100,
        altegio_client_id=100,
        is_deleted=False,
        raw={},
    )
    session.add(r)
    await session.flush()
    return r


async def _seed_service(session, *, record_id: int = 200, service_id: int = _ALLOWED_SERVICE) -> None:
    session.add(RecordService(record_id=record_id, service_id=service_id, title="Haarschnitt", raw={}))
    await session.flush()


def _make_lead(**overrides) -> PromoLead:
    kwargs = dict(
        company_id=_COMPANY,
        phone_e164=_PHONE,
        campaign_name="welcome_discount",
        secret_code="aktion",
        discount_amount=Decimal("15"),
        discount_type="fixed",
        status="issued",
        issued_at=datetime(2026, 1, 1, tzinfo=_UTC),
        expires_at=_FUTURE,
        loyalty_card_id=_CARD_ID,
        location_id=_LOCATION,
        discount_program_id=_PROGRAM_ID,
        meta={"loyalty_card_issued": True},
    )
    kwargs.update(overrides)
    return PromoLead(**kwargs)


def _base_settings_ctx(**overrides):
    import contextlib

    defaults = {
        "promo_apply_discount_enabled": True,
        "promo_apply_discount_api_verified": True,
        "promo_allowed_service_ids": str(_ALLOWED_SERVICE),
        "promo_apply_mode": "loyalty_program",  # keep notification tests on legacy path
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
# Unit test infrastructure
# ---------------------------------------------------------------------------


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


@dataclass
class FakeJob:
    id: int
    company_id: int
    job_type: str
    status: str
    run_at: datetime
    record_id: int | None = None
    client_id: int | None = None
    last_error: str | None = None
    attempts: int = 0
    max_attempts: int = 5
    locked_at: Any = None
    payload: dict = field(default_factory=dict)


@dataclass
class FakeClient:
    id: int
    display_name: str = "Anna"
    phone_e164: str | None = "+491234567890"


@dataclass
class FakePromoLead:
    id: int
    company_id: int = _COMPANY
    meta: dict = field(default_factory=dict)


@dataclass
class FakeRecord:
    id: int
    altegio_record_id: int | None = None
    company_id: int = _COMPANY


class FakeSession:
    def __init__(self, promo_lead: FakePromoLead | None = None) -> None:
        self.added: list[Any] = []
        self._pk = 0
        self._promo_lead = promo_lead

    def add(self, obj: Any) -> None:
        if not hasattr(obj, "id"):
            self._pk += 1
            setattr(obj, "id", self._pk)
        self.added.append(obj)

    async def get(self, model: Any, pk: Any) -> Any:
        if self._promo_lead is not None and getattr(self._promo_lead, "id", None) == pk:
            return self._promo_lead
        return None

    async def flush(self) -> None:
        pass


def _patch_common(monkeypatch: Any, *, job: FakeJob, client: FakeClient | None = None) -> None:
    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_find_success(session: Any, job_id: int) -> Any:
        return None

    async def fake_find_existing(session: Any, job_id: int) -> Any:
        return None

    async def fake_count_131026(session: Any, phone: str, window_days: int) -> int:
        return 0

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return None

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return client

    async def fake_apply_rl(session: Any, phone: str) -> Any:
        return None

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    monkeypatch.setattr(ow, "_find_success_outbox", fake_find_success)
    monkeypatch.setattr(ow, "_find_existing_outbox", fake_find_existing)
    monkeypatch.setattr(ow, "_count_131026_failures", fake_count_131026)
    monkeypatch.setattr(ow, "_load_record", fake_load_record)
    monkeypatch.setattr(ow, "_load_client", fake_load_client)
    monkeypatch.setattr(ow, "_apply_rate_limit", fake_apply_rl)


# ---------------------------------------------------------------------------
# 1. Notification job created on successful apply
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_notification_job_created_on_apply(session_maker) -> None:
    mock_api = AsyncMock(return_value=PromoDiscountApplyResult(applied=True, raw={"success": True}))

    lead_id: int | None = None

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session, altegio_record_id=777)
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    async with session_maker() as s:
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert job is not None
    assert job.company_id == _COMPANY
    assert job.client_id == 100
    assert job.record_id == 200
    assert job.status == "queued"
    assert job.dedupe_key == f"promo_discount_applied:{lead_id}"
    assert "body" in job.payload
    assert "phone_e164" in job.payload
    assert job.payload["phone_e164"] == _PHONE
    assert job.payload["promo_lead_id"] == lead_id


# ---------------------------------------------------------------------------
# 2. lead.meta contains all expected notification fields
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_notification_meta_fields(session_maker) -> None:
    mock_api = AsyncMock(return_value=PromoDiscountApplyResult(applied=True, raw={"success": True}))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session)
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.id == lead_id))).scalar_one()

    meta = lead.meta or {}
    assert lead.status == "applied"
    assert meta.get("customer_notification") == "queued"
    assert isinstance(meta.get("customer_notification_job_id"), int)
    assert meta.get("customer_notification_created_at") is not None
    assert meta.get("customer_notification_dedupe_key") == f"promo_discount_applied:{lead_id}"
    assert "discount_applied_at" in meta


# ---------------------------------------------------------------------------
# 3. Notification job NOT created when API not verified
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_notification_job_not_created_when_api_not_verified(session_maker) -> None:
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session)
            await _seed_service(session)
            session.add(_make_lead())
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx(promo_apply_discount_api_verified=False):
                    await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    async with session_maker() as s:
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert job is None


# ---------------------------------------------------------------------------
# 4. Notification job NOT created when service not allowed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_notification_job_not_created_when_service_not_allowed(session_maker) -> None:
    mock_api = AsyncMock(side_effect=RuntimeError("must not be called"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session)
            await _seed_service(session, service_id=_OTHER_SERVICE)
            session.add(_make_lead())
            await session.flush()

            with patch("altegio_bot.promo_discount_apply.apply_promo_discount_to_visit", mock_api):
                with _base_settings_ctx():
                    await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    async with session_maker() as s:
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert job is None


# ---------------------------------------------------------------------------
# 5. Idempotent: calling _ensure twice for the same lead → one job
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_notification_job_idempotent(session_maker) -> None:
    """_ensure_promo_discount_notification_job returns existing job on second call."""
    from altegio_bot.promo_discount_apply import _ensure_promo_discount_notification_job

    async with session_maker() as session:
        async with session.begin():
            client = await _seed_client(session)
            record = await _seed_record(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

            await _ensure_promo_discount_notification_job(session, lead, client, record, _PHONE, _NOW)
            first_job_id = lead.meta.get("customer_notification_job_id")

            await _ensure_promo_discount_notification_job(session, lead, client, record, _PHONE, _NOW)
            second_job_id = lead.meta.get("customer_notification_job_id")

    assert first_job_id is not None
    assert second_job_id == first_job_id

    async with session_maker() as s:
        jobs = list(
            (await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))).scalars().all()
        )
    assert len(jobs) == 1
    assert jobs[0].id == first_job_id


# ---------------------------------------------------------------------------
# 6. Message body content
# ---------------------------------------------------------------------------


def test_notification_body_content() -> None:
    from altegio_bot.promo_discount_apply import _build_notification_body

    body = _build_notification_body()
    assert "Neukundenrabatt" in body
    assert "Buchung" in body
    assert "reguläre Preise" in body
    assert "Rabatt" in body


# ---------------------------------------------------------------------------
# 7. outbox_worker sends promo_discount_applied job
# ---------------------------------------------------------------------------


def test_outbox_worker_sends_promo_discount_applied(monkeypatch: Any) -> None:
    job = FakeJob(
        id=1,
        company_id=_COMPANY,
        job_type="promo_discount_applied",
        status="queued",
        run_at=_NOW,
        client_id=100,
        payload={"body": _BODY, "phone_e164": _PHONE},
    )
    client = FakeClient(id=100, phone_e164=_PHONE)

    _patch_common(monkeypatch, job=job, client=client)

    async def fake_pick_sender_id(session: Any, company_id: int, sender_code: str) -> int:
        return 42

    async def fake_safe_send(**kwargs: Any) -> tuple[str, None]:
        return ("msg-001", None)

    monkeypatch.setattr(ow, "pick_sender_id", fake_pick_sender_id)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)

    session = FakeSession()
    _run(ow.process_job_in_session(session, 1, provider=object()))  # type: ignore

    assert job.status == "done"
    assert job.last_error is None
    assert len(session.added) == 1
    out = session.added[0]
    assert out.status == "sent"
    assert out.template_code == "promo_discount_applied"
    assert out.body == _BODY
    assert out.language == "de"
    assert out.phone_e164 == _PHONE
    assert out.sender_id == 42
    assert out.job_id == 1


# ---------------------------------------------------------------------------
# 8. outbox_worker fails when body missing from payload
# ---------------------------------------------------------------------------


def test_outbox_worker_fails_missing_body(monkeypatch: Any) -> None:
    job = FakeJob(
        id=2,
        company_id=_COMPANY,
        job_type="promo_discount_applied",
        status="queued",
        run_at=_NOW,
        client_id=100,
        payload={},
    )
    client = FakeClient(id=100, phone_e164=_PHONE)

    _patch_common(monkeypatch, job=job, client=client)

    async def fake_safe_send(**kwargs: Any) -> tuple[str, None]:
        raise AssertionError("safe_send must not be called")

    monkeypatch.setattr(ow, "safe_send", fake_safe_send)

    session = FakeSession()
    _run(ow.process_job_in_session(session, 2, provider=object()))  # type: ignore

    assert job.status == "failed"
    assert job.last_error is not None
    assert "missing body" in job.last_error
    assert session.added == []


# ---------------------------------------------------------------------------
# 9. outbox_worker fails when no active sender
# ---------------------------------------------------------------------------


def test_outbox_worker_fails_no_sender(monkeypatch: Any) -> None:
    job = FakeJob(
        id=3,
        company_id=_COMPANY,
        job_type="promo_discount_applied",
        status="queued",
        run_at=_NOW,
        client_id=100,
        payload={"body": _BODY, "phone_e164": _PHONE},
    )
    client = FakeClient(id=100, phone_e164=_PHONE)

    _patch_common(monkeypatch, job=job, client=client)

    async def fake_pick_sender_id(session: Any, company_id: int, sender_code: str) -> None:
        return None

    async def fake_safe_send(**kwargs: Any) -> tuple[str, None]:
        raise AssertionError("safe_send must not be called")

    monkeypatch.setattr(ow, "pick_sender_id", fake_pick_sender_id)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)

    session = FakeSession()
    _run(ow.process_job_in_session(session, 3, provider=object()))  # type: ignore

    assert job.status == "failed"
    assert job.last_error is not None
    assert "no active sender" in job.last_error
    assert session.added == []


# ---------------------------------------------------------------------------
# 10. outbox_worker reconciles PromoLead.meta → 'sent' on success
# ---------------------------------------------------------------------------


def test_outbox_worker_reconciles_lead_on_success(monkeypatch: Any) -> None:
    fake_lead = FakePromoLead(id=123, meta={"customer_notification": "queued"})
    job = FakeJob(
        id=10,
        company_id=_COMPANY,
        job_type="promo_discount_applied",
        status="queued",
        run_at=_NOW,
        client_id=100,
        payload={"body": _BODY, "phone_e164": _PHONE, "promo_lead_id": 123},
    )
    client = FakeClient(id=100, phone_e164=_PHONE)

    _patch_common(monkeypatch, job=job, client=client)

    async def fake_pick_sender_id(session: Any, company_id: int, sender_code: str) -> int:
        return 42

    async def fake_safe_send(**kwargs: Any) -> tuple[str, None]:
        return ("msg-xyz", None)

    monkeypatch.setattr(ow, "pick_sender_id", fake_pick_sender_id)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)

    session = FakeSession(promo_lead=fake_lead)
    _run(ow.process_job_in_session(session, 10, provider=object()))  # type: ignore

    assert job.status == "done"
    assert fake_lead.meta.get("customer_notification") == "sent"
    assert fake_lead.meta.get("customer_notification_sent_at") is not None
    assert fake_lead.meta.get("customer_notification_provider_message_id") == "msg-xyz"


# ---------------------------------------------------------------------------
# 11. outbox_worker reconciles PromoLead.meta → 'failed' on final failure
# ---------------------------------------------------------------------------


def test_outbox_worker_reconciles_lead_on_final_failure(monkeypatch: Any) -> None:
    fake_lead = FakePromoLead(id=124, meta={"customer_notification": "queued"})
    job = FakeJob(
        id=11,
        company_id=_COMPANY,
        job_type="promo_discount_applied",
        status="queued",
        run_at=_NOW,
        client_id=100,
        attempts=4,
        max_attempts=5,
        payload={"body": _BODY, "phone_e164": _PHONE, "promo_lead_id": 124},
    )
    client = FakeClient(id=100, phone_e164=_PHONE)

    _patch_common(monkeypatch, job=job, client=client)

    async def fake_pick_sender_id(session: Any, company_id: int, sender_code: str) -> int:
        return 42

    async def fake_safe_send(**kwargs: Any) -> tuple[None, str]:
        return (None, "provider error")

    monkeypatch.setattr(ow, "pick_sender_id", fake_pick_sender_id)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)

    session = FakeSession(promo_lead=fake_lead)
    _run(ow.process_job_in_session(session, 11, provider=object()))  # type: ignore

    assert job.status == "failed"
    assert fake_lead.meta.get("customer_notification") == "failed"
    assert fake_lead.meta.get("customer_notification_failed_at") is not None
    assert "provider error" in (fake_lead.meta.get("customer_notification_error") or "")


# ---------------------------------------------------------------------------
# 12. outbox_worker retryable failure: 'queued' preserved, last_error recorded
# ---------------------------------------------------------------------------


def test_outbox_worker_retryable_failure_leaves_queued(monkeypatch: Any) -> None:
    fake_lead = FakePromoLead(id=125, meta={"customer_notification": "queued"})
    job = FakeJob(
        id=12,
        company_id=_COMPANY,
        job_type="promo_discount_applied",
        status="queued",
        run_at=_NOW,
        client_id=100,
        attempts=0,
        max_attempts=5,
        payload={"body": _BODY, "phone_e164": _PHONE, "promo_lead_id": 125},
    )
    client = FakeClient(id=100, phone_e164=_PHONE)

    _patch_common(monkeypatch, job=job, client=client)

    async def fake_pick_sender_id(session: Any, company_id: int, sender_code: str) -> int:
        return 42

    async def fake_safe_send(**kwargs: Any) -> tuple[None, str]:
        return (None, "temporary error")

    monkeypatch.setattr(ow, "pick_sender_id", fake_pick_sender_id)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)

    session = FakeSession(promo_lead=fake_lead)
    _run(ow.process_job_in_session(session, 12, provider=object()))  # type: ignore

    assert job.status == "queued"
    assert fake_lead.meta.get("customer_notification") == "queued"
    assert "customer_notification_last_error" in fake_lead.meta
    assert "temporary error" in fake_lead.meta["customer_notification_last_error"]


# ---------------------------------------------------------------------------
# 13. Missing promo_lead_id in payload: send still succeeds
# ---------------------------------------------------------------------------


def test_outbox_worker_missing_promo_lead_id_does_not_break_send(monkeypatch: Any) -> None:
    job = FakeJob(
        id=13,
        company_id=_COMPANY,
        job_type="promo_discount_applied",
        status="queued",
        run_at=_NOW,
        client_id=100,
        payload={"body": _BODY, "phone_e164": _PHONE},
    )
    client = FakeClient(id=100, phone_e164=_PHONE)

    _patch_common(monkeypatch, job=job, client=client)

    async def fake_pick_sender_id(session: Any, company_id: int, sender_code: str) -> int:
        return 42

    async def fake_safe_send(**kwargs: Any) -> tuple[str, None]:
        return ("msg-abc", None)

    monkeypatch.setattr(ow, "pick_sender_id", fake_pick_sender_id)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)

    session = FakeSession()
    _run(ow.process_job_in_session(session, 13, provider=object()))  # type: ignore

    assert job.status == "done"
    assert len(session.added) == 1
    assert session.added[0].status == "sent"


# ---------------------------------------------------------------------------
# 14. _ensure race recovery: IntegrityError → re-read existing, meta updated
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_notification_job_race_recovery() -> None:
    """Concurrent IntegrityError on insert → helper recovers and points meta to existing job."""
    from sqlalchemy.exc import IntegrityError as SAIntegrityError

    from altegio_bot.promo_discount_apply import _ensure_promo_discount_notification_job

    EXISTING_JOB_ID = 77

    class _RaceSession:
        """First SELECT returns None; flush inside begin_nested raises IntegrityError;
        second SELECT (recovery) returns the existing job."""

        def __init__(self) -> None:
            self._executions = 0

        def add(self, obj: Any) -> None:
            pass

        async def execute(self, stmt: Any) -> Any:
            from unittest.mock import MagicMock

            self._executions += 1
            result = MagicMock()
            if self._executions == 1:
                result.scalar_one_or_none.return_value = None
            else:
                fake_job = MagicMock()
                fake_job.id = EXISTING_JOB_ID
                result.scalar_one_or_none.return_value = fake_job
            return result

        async def flush(self) -> None:
            raise SAIntegrityError(None, None, Exception("unique constraint violation"))

        class _Savepoint:
            async def __aenter__(self) -> "Any":
                return self

            async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
                return False  # never suppress; let IntegrityError propagate to except block

        def begin_nested(self) -> "_Savepoint":
            return self._Savepoint()

    lead = FakePromoLead(id=5, meta={"loyalty_card_issued": True})
    fake_client = FakeClient(id=100)
    fake_record = FakeRecord(id=200)

    await _ensure_promo_discount_notification_job(
        _RaceSession(),
        lead,
        fake_client,
        fake_record,
        _PHONE,
        _NOW,  # type: ignore[arg-type]
    )

    assert lead.meta.get("customer_notification") == "queued"
    assert lead.meta.get("customer_notification_job_id") == EXISTING_JOB_ID


# ---------------------------------------------------------------------------
# 15. no active sender reconciles PromoLead.meta → 'failed'
# ---------------------------------------------------------------------------


def test_outbox_worker_no_sender_reconciles_lead(monkeypatch: Any) -> None:
    fake_lead = FakePromoLead(id=200, meta={"customer_notification": "queued"})
    job = FakeJob(
        id=15,
        company_id=_COMPANY,
        job_type="promo_discount_applied",
        status="queued",
        run_at=_NOW,
        client_id=100,
        payload={"body": _BODY, "phone_e164": _PHONE, "promo_lead_id": 200},
    )
    client = FakeClient(id=100, phone_e164=_PHONE)

    _patch_common(monkeypatch, job=job, client=client)

    async def fake_pick_sender_id(session: Any, company_id: int, sender_code: str) -> None:
        return None

    async def fake_safe_send(**kwargs: Any) -> tuple:
        raise AssertionError("safe_send must not be called")

    monkeypatch.setattr(ow, "pick_sender_id", fake_pick_sender_id)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)

    session = FakeSession(promo_lead=fake_lead)
    _run(ow.process_job_in_session(session, 15, provider=object()))  # type: ignore

    assert job.status == "failed"
    assert "no active sender" in (job.last_error or "")
    assert fake_lead.meta.get("customer_notification") == "failed"
    assert fake_lead.meta.get("customer_notification_failed_at") is not None
    assert "no active sender" in (fake_lead.meta.get("customer_notification_error") or "")


# ---------------------------------------------------------------------------
# 16. missing body reconciles PromoLead.meta → 'failed'
# ---------------------------------------------------------------------------


def test_outbox_worker_missing_body_reconciles_lead(monkeypatch: Any) -> None:
    fake_lead = FakePromoLead(id=201, meta={"customer_notification": "queued"})
    job = FakeJob(
        id=16,
        company_id=_COMPANY,
        job_type="promo_discount_applied",
        status="queued",
        run_at=_NOW,
        client_id=100,
        payload={"phone_e164": _PHONE, "promo_lead_id": 201},  # body intentionally missing
    )
    client = FakeClient(id=100, phone_e164=_PHONE)

    _patch_common(monkeypatch, job=job, client=client)

    session = FakeSession(promo_lead=fake_lead)
    _run(ow.process_job_in_session(session, 16, provider=object()))  # type: ignore

    assert job.status == "failed"
    assert "missing body" in (job.last_error or "")
    assert fake_lead.meta.get("customer_notification") == "failed"
    assert fake_lead.meta.get("customer_notification_failed_at") is not None


# ===========================================================================
# Network-aware promo lead application tests (cross-company apply)
#
# Tests N1–N10 cover the network-aware behavior introduced for the Yasmine
# production incident: PromoLead issued in company 1271200, booking created
# in company 758285 (Karlsruhe).
# ===========================================================================

# --- Constants ---------------------------------------------------------------

_COMPANY_SRC = 1271200  # company where the PromoLead was originally issued
_COMPANY_DST = 758285  # company where the booking record was created
_LOCATION_DST = 9002  # Altegio location_id for _COMPANY_DST
_CROSS_PHONE = "+4915777903655"  # Yasmine's phone (different from _PHONE)
_CROSS_ALTEGIO_REC_ID = 8888
_NETWORK_IDS = f"{_COMPANY_SRC},{_COMPANY_DST}"
_NETWORK_LOC_MAP = f'{{"{_COMPANY_DST}": {_LOCATION_DST}}}'

# Fake Altegio GET /record response used by price-override path mocks.
_FAKE_ALTEGIO_RECORD: dict = {
    "id": _CROSS_ALTEGIO_REC_ID,
    "attendance": 0,
    "visit_attendance": 0,
    "comment": "",
    "services": [{"id": _ALLOWED_SERVICE, "cost": 100.0, "manual_cost": 100.0}],
}


# --- Helpers -----------------------------------------------------------------


def _network_settings_ctx(**overrides):
    """Settings context for cross-company tests (record_price_override mode)."""
    import contextlib

    defaults = {
        "promo_apply_discount_enabled": True,
        "promo_apply_discount_api_verified": True,
        "promo_allowed_service_ids": str(_ALLOWED_SERVICE),
        "promo_apply_mode": "record_price_override",
        "promo_network_apply_enabled": True,
        "promo_network_company_ids": _NETWORK_IDS,
        "promo_location_id_by_company": _NETWORK_LOC_MAP,
        "promo_altegio_client_api_verified": False,
        "promo_issue_loyalty_card_enabled": False,
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


async def _seed_cross_client(session, *, client_id: int = 300, phone: str = _CROSS_PHONE) -> Client:
    c = Client(
        id=client_id,
        company_id=_COMPANY_DST,
        altegio_client_id=client_id,
        phone_e164=phone,
        display_name="Yasmine",
        raw={},
    )
    session.add(c)
    await session.flush()
    return c


async def _seed_cross_record(
    session,
    *,
    record_id: int = 400,
    client_id: int = 300,
    altegio_record_id: int = _CROSS_ALTEGIO_REC_ID,
) -> Record:
    r = Record(
        id=record_id,
        company_id=_COMPANY_DST,
        altegio_record_id=altegio_record_id,
        client_id=client_id,
        altegio_client_id=client_id,
        is_deleted=False,
        starts_at=_NOW,
        raw={},
    )
    session.add(r)
    await session.flush()
    return r


def _make_cross_lead(**overrides) -> PromoLead:
    kwargs = dict(
        company_id=_COMPANY_SRC,
        phone_e164=_CROSS_PHONE,
        campaign_name="welcome_discount",
        secret_code="sommer",
        discount_amount=Decimal("15"),
        discount_type="fixed",
        status="issued",
        issued_at=datetime(2026, 1, 1, tzinfo=_UTC),
        expires_at=_FUTURE,
        loyalty_card_id="cross_card_001",
        loyalty_card_number="1234",
        location_id=_LOCATION,  # source-company location
        discount_program_id="dp_cross",
        meta={"loyalty_card_issued": True},
    )
    kwargs.update(overrides)
    return PromoLead(**kwargs)


# --- N1. Yasmine regression: cross-company lead applied ----------------------


@pytest.mark.asyncio
async def test_network_cross_company_lead_applied(session_maker) -> None:
    """N1 (Yasmine regression): lead in 1271200, record in 758285, both in network list
    → lead found cross-company, applied, meta.network_apply.cross_company=True, job created.
    """
    mock_fetch = AsyncMock(return_value=_FAKE_ALTEGIO_RECORD)
    mock_put = AsyncMock(return_value={"data": {"services": []}})

    lead_id: int | None = None

    async with session_maker() as session:
        async with session.begin():
            await _seed_cross_client(session)
            record = await _seed_cross_record(session)
            session.add(
                RecordService(
                    record_id=record.id,
                    service_id=_ALLOWED_SERVICE,
                    title="Lash",
                    raw={},
                )
            )
            lead = _make_cross_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            with (
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    mock_fetch,
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _network_settings_ctx(),
            ):
                await try_apply_promo_discount(
                    session,
                    record,
                    _COMPANY_DST,
                    booking_created_at=_NOW,
                )

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.id == lead_id))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    meta = lead.meta or {}
    network_apply = meta.get("network_apply", {})

    assert lead.status in ("applied", "booked"), f"expected applied or booked, got {lead.status!r}"
    assert lead.record_id == record.id
    assert lead.altegio_record_id == _CROSS_ALTEGIO_REC_ID
    assert network_apply.get("cross_company") is True
    assert network_apply.get("source_company_id") == _COMPANY_SRC
    assert network_apply.get("applied_company_id") == _COMPANY_DST
    # Notification job created (simple case: 1 same-day record, 1 service)
    assert job is not None, "promo_discount_applied job must be created"
    # job.company_id must match the record's company (758285), not the lead's (1271200)
    assert job.company_id == _COMPANY_DST

    async with session_maker() as s:
        leads = (await s.execute(select(PromoLead))).scalars().all()
    assert len(leads) == 1, f"expected 1 PromoLead, got {len(leads)}"


# --- N2. Cross-company disabled → no apply -----------------------------------


@pytest.mark.asyncio
async def test_network_disabled_cross_company_not_applied(session_maker) -> None:
    """N2: promo_network_apply_enabled=False → lead in 1271200 not found for
    record in 758285.  Status stays 'issued', no notification job.
    """
    mock_fetch = AsyncMock(side_effect=RuntimeError("must not call fetch"))
    mock_put = AsyncMock(side_effect=RuntimeError("must not call PUT"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_cross_client(session)
            record = await _seed_cross_record(session)
            session.add(
                RecordService(
                    record_id=record.id,
                    service_id=_ALLOWED_SERVICE,
                    title="Lash",
                    raw={},
                )
            )
            lead = _make_cross_lead()
            session.add(lead)
            await session.flush()

            with (
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    mock_fetch,
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _network_settings_ctx(promo_network_apply_enabled=False),
            ):
                await try_apply_promo_discount(
                    session,
                    record,
                    _COMPANY_DST,
                    booking_created_at=_NOW,
                )

    async with session_maker() as s:
        lead_db = (await s.execute(select(PromoLead))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead_db.status == "issued"
    assert job is None


# --- N3. Same-company behavior unchanged when network mode on ----------------


@pytest.mark.asyncio
async def test_network_same_company_behavior_unchanged(session_maker) -> None:
    """N3: same-company lead is still found and applied even when network mode enabled."""
    mock_api = AsyncMock(return_value=PromoDiscountApplyResult(applied=True, raw={"success": True}))

    lead_id: int | None = None

    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            record = await _seed_record(session, altegio_record_id=777)
            await _seed_service(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            with patch(
                "altegio_bot.promo_discount_apply.apply_promo_discount_to_visit",
                mock_api,
            ):
                with _base_settings_ctx(
                    promo_network_apply_enabled=True,
                    promo_network_company_ids=str(_COMPANY),
                ):
                    await try_apply_promo_discount(session, record, _COMPANY, booking_created_at=_NOW)

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.id == lead_id))).scalar_one()

    assert lead.status == "applied"


# --- N4. Record company not in allowed network list --------------------------


@pytest.mark.asyncio
async def test_network_record_company_not_in_allowed_list(session_maker) -> None:
    """N4: record.company_id absent from promo_network_company_ids → apply skipped."""
    mock_fetch = AsyncMock(side_effect=RuntimeError("must not call fetch"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_cross_client(session)
            record = await _seed_cross_record(session)
            session.add(
                RecordService(
                    record_id=record.id,
                    service_id=_ALLOWED_SERVICE,
                    title="Lash",
                    raw={},
                )
            )
            lead = _make_cross_lead()
            session.add(lead)
            await session.flush()

            # _COMPANY_DST (758285) intentionally absent
            with (
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    mock_fetch,
                ),
                _network_settings_ctx(promo_network_company_ids=str(_COMPANY_SRC)),
            ):
                await try_apply_promo_discount(
                    session,
                    record,
                    _COMPANY_DST,
                    booking_created_at=_NOW,
                )

    async with session_maker() as s:
        lead_db = (await s.execute(select(PromoLead))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead_db.status == "issued"
    assert job is None


# --- N5. Multiple active leads → fail-closed (unit test via mock session) ----


def test_network_multiple_candidates_fail_closed() -> None:
    """N5: multiple active leads in network scope → fail-closed, returns None."""
    from unittest.mock import MagicMock

    from altegio_bot.promo_discount_apply import find_applicable_promo_lead_for_record

    lead1 = MagicMock(spec=PromoLead)
    lead1.id = 10
    lead1.company_id = _COMPANY_SRC
    lead1.status = "issued"

    lead2 = MagicMock(spec=PromoLead)
    lead2.id = 11
    lead2.company_id = _COMPANY_DST
    lead2.status = "issued"

    _call_n: dict = {"n": 0}

    class _ScalarResult:
        def __init__(self, items: list) -> None:
            self._items = items

        def scalar_one_or_none(self) -> None:
            return None

        def scalars(self) -> "_ScalarResult":
            return self

        def all(self) -> list:
            return self._items

    class _FakeSession:
        async def execute(self, _stmt: object) -> _ScalarResult:
            _call_n["n"] += 1
            # First execute = same-company query → empty
            # Second execute = cross-company query → two candidates
            return _ScalarResult([] if _call_n["n"] == 1 else [lead1, lead2])

    record = MagicMock(spec=Record)
    record.id = 200
    record.altegio_record_id = _CROSS_ALTEGIO_REC_ID

    with (
        patch.object(settings, "promo_network_apply_enabled", True),
        patch.object(settings, "promo_network_company_ids", _NETWORK_IDS),
    ):
        result = _run(
            find_applicable_promo_lead_for_record(
                _FakeSession(),  # type: ignore[arg-type]
                company_id=_COMPANY_DST,
                phone_e164=_CROSS_PHONE,
                now=_NOW,
                record=record,
            )
        )

    assert result is None, "must fail-closed when multiple candidates found"


# --- N6. Booked lead already bound to a different record ---------------------


@pytest.mark.asyncio
async def test_network_booked_lead_different_record_skipped(
    session_maker,
) -> None:
    """N6: booked cross-company lead bound to record 8888, current record 8889 → skipped."""
    mock_fetch = AsyncMock(side_effect=RuntimeError("must not call fetch"))

    _other_altegio_id = 8889

    async with session_maker() as session:
        async with session.begin():
            await _seed_cross_client(session)
            record = await _seed_cross_record(
                session,
                record_id=401,
                altegio_record_id=_other_altegio_id,
            )
            session.add(
                RecordService(
                    record_id=record.id,
                    service_id=_ALLOWED_SERVICE,
                    title="Lash",
                    raw={},
                )
            )
            # Lead already booked to a DIFFERENT altegio_record_id (8888 ≠ 8889).
            # record_id=None so no FK constraint fires; the mismatch is on
            # altegio_record_id alone.
            lead = _make_cross_lead(
                status="booked",
                altegio_record_id=_CROSS_ALTEGIO_REC_ID,  # 8888 ≠ 8889
            )
            session.add(lead)
            await session.flush()

            with (
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    mock_fetch,
                ),
                _network_settings_ctx(),
            ):
                await try_apply_promo_discount(
                    session,
                    record,
                    _COMPANY_DST,
                    booking_created_at=_NOW,
                )

    async with session_maker() as s:
        lead_db = (await s.execute(select(PromoLead))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead_db.status == "booked"
    assert job is None


# --- N7. Service not allowed even in network mode ----------------------------


@pytest.mark.asyncio
async def test_network_service_not_allowed_skipped(session_maker) -> None:
    """N7: cross-company lead found, but record service not in allowlist → no apply."""
    mock_fetch = AsyncMock(side_effect=RuntimeError("must not call fetch"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_cross_client(session)
            record = await _seed_cross_record(session)
            session.add(
                RecordService(
                    record_id=record.id,
                    service_id=_OTHER_SERVICE,  # not in allowlist
                    title="Other",
                    raw={},
                )
            )
            lead = _make_cross_lead()
            session.add(lead)
            await session.flush()

            with (
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    mock_fetch,
                ),
                _network_settings_ctx(),
            ):
                await try_apply_promo_discount(
                    session,
                    record,
                    _COMPANY_DST,
                    booking_created_at=_NOW,
                )

    async with session_maker() as s:
        lead_db = (await s.execute(select(PromoLead))).scalar_one()

    assert lead_db.status == "issued"


# --- N8. Booking predates promo lead -----------------------------------------


@pytest.mark.asyncio
async def test_network_booking_predates_promo_skipped(session_maker) -> None:
    """N8: booking_created_at before lead.issued_at → apply skipped."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_cross_client(session)
            record = await _seed_cross_record(session)
            session.add(
                RecordService(
                    record_id=record.id,
                    service_id=_ALLOWED_SERVICE,
                    title="Lash",
                    raw={},
                )
            )
            # issued_at is AFTER _NOW → booking predates promo
            lead = _make_cross_lead(issued_at=datetime(2026, 6, 1, tzinfo=_UTC))
            session.add(lead)
            await session.flush()

            with _network_settings_ctx():
                await try_apply_promo_discount(
                    session,
                    record,
                    _COMPANY_DST,
                    booking_created_at=_NOW,
                )

    async with session_maker() as s:
        lead_db = (await s.execute(select(PromoLead))).scalar_one()

    assert lead_db.status == "issued"
    assert (lead_db.meta or {}).get("apply_skip_reason") == "booking predates promo lead"


# --- N9. Client provisioning: not verified → skipped; verified → called ------


@pytest.mark.asyncio
async def test_network_client_provisioning_not_called_when_not_verified(
    session_maker,
) -> None:
    """N9a: promo_altegio_client_api_verified=False → client API not called,
    binding records client_provisioning_skipped.
    """
    mock_fetch = AsyncMock(return_value=_FAKE_ALTEGIO_RECORD)
    mock_put = AsyncMock(return_value={"data": {"services": []}})
    mock_client_api = AsyncMock(side_effect=RuntimeError("must not call client API"))

    lead_id: int | None = None

    async with session_maker() as session:
        async with session.begin():
            await _seed_cross_client(session)
            record = await _seed_cross_record(session)
            session.add(
                RecordService(
                    record_id=record.id,
                    service_id=_ALLOWED_SERVICE,
                    title="Lash",
                    raw={},
                )
            )
            lead = _make_cross_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            with (
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    mock_fetch,
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                patch(
                    "altegio_bot.promo_loyalty.get_or_create_altegio_client",
                    mock_client_api,
                ),
                _network_settings_ctx(promo_altegio_client_api_verified=False),
            ):
                await try_apply_promo_discount(
                    session,
                    record,
                    _COMPANY_DST,
                    booking_created_at=_NOW,
                )

    mock_client_api.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.id == lead_id))).scalar_one()

    bindings = (lead.meta or {}).get("company_bindings", {})
    binding = bindings.get(str(_COMPANY_DST), {})
    assert binding.get("client_provisioning_skipped") == ("promo_altegio_client_api_verified=False")


@pytest.mark.asyncio
async def test_network_client_provisioning_called_when_verified(
    session_maker,
) -> None:
    """N9b: promo_altegio_client_api_verified=True → helper called for record company,
    altegio_client_id stored in company_bindings.
    """
    mock_fetch = AsyncMock(return_value=_FAKE_ALTEGIO_RECORD)
    mock_put = AsyncMock(return_value={"data": {"services": []}})
    mock_client_api = AsyncMock(return_value=182334954)

    lead_id: int | None = None

    async with session_maker() as session:
        async with session.begin():
            await _seed_cross_client(session)
            record = await _seed_cross_record(session)
            session.add(
                RecordService(
                    record_id=record.id,
                    service_id=_ALLOWED_SERVICE,
                    title="Lash",
                    raw={},
                )
            )
            lead = _make_cross_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            with (
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    mock_fetch,
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                patch(
                    "altegio_bot.promo_loyalty.get_or_create_altegio_client",
                    mock_client_api,
                ),
                _network_settings_ctx(promo_altegio_client_api_verified=True),
            ):
                await try_apply_promo_discount(
                    session,
                    record,
                    _COMPANY_DST,
                    booking_created_at=_NOW,
                )

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.id == lead_id))).scalar_one()

    bindings = (lead.meta or {}).get("company_bindings", {})
    binding = bindings.get(str(_COMPANY_DST), {})
    assert binding.get("altegio_client_id") == 182334954


# --- N10. Loyalty card provisioning ------------------------------------------


@pytest.mark.asyncio
async def test_network_loyalty_card_not_reissued_if_binding_exists(
    session_maker,
) -> None:
    """N10a: company_bindings already has an entry → loyalty card not re-issued."""
    mock_fetch = AsyncMock(return_value=_FAKE_ALTEGIO_RECORD)
    mock_put = AsyncMock(return_value={"data": {"services": []}})
    mock_card = AsyncMock(side_effect=RuntimeError("must not re-issue card"))

    existing_bindings = {
        str(_COMPANY_DST): {
            "altegio_client_id": 182334954,
            "loyalty_card_id": "existing_card",
            "loyalty_card_number": "1234",
            "location_id": _LOCATION_DST,
            "source": "network_apply",
        }
    }

    lead_id: int | None = None

    async with session_maker() as session:
        async with session.begin():
            await _seed_cross_client(session)
            record = await _seed_cross_record(session)
            session.add(
                RecordService(
                    record_id=record.id,
                    service_id=_ALLOWED_SERVICE,
                    title="Lash",
                    raw={},
                )
            )
            lead = _make_cross_lead(
                meta={
                    "loyalty_card_issued": True,
                    "company_bindings": existing_bindings,
                }
            )
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            with (
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    mock_fetch,
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                patch(
                    "altegio_bot.promo_loyalty.issue_promo_loyalty_card",
                    mock_card,
                ),
                _network_settings_ctx(
                    promo_issue_loyalty_card_enabled=True,
                    promo_loyalty_card_api_verified=True,
                    promo_loyalty_card_type_id="ctype_001",
                ),
            ):
                await try_apply_promo_discount(
                    session,
                    record,
                    _COMPANY_DST,
                    booking_created_at=_NOW,
                )

    mock_card.assert_not_called()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.id == lead_id))).scalar_one()

    # Binding must still have the original data (not overwritten)
    bindings = (lead.meta or {}).get("company_bindings", {})
    assert bindings.get(str(_COMPANY_DST), {}).get("loyalty_card_id") == ("existing_card")


@pytest.mark.asyncio
async def test_network_loyalty_card_provisioned_when_enabled(
    session_maker,
) -> None:
    """N10b: no existing binding + card issuance enabled → mock called, card stored."""
    from altegio_bot.promo_loyalty import LoyaltyCardResult

    mock_fetch = AsyncMock(return_value=_FAKE_ALTEGIO_RECORD)
    mock_put = AsyncMock(return_value={"data": {"services": []}})
    mock_card = AsyncMock(
        return_value=LoyaltyCardResult(
            loyalty_card_id="new_card_001",
            loyalty_card_number="9999",
            card_type_id="ctype_001",
        )
    )

    lead_id: int | None = None

    async with session_maker() as session:
        async with session.begin():
            await _seed_cross_client(session)
            record = await _seed_cross_record(session)
            session.add(
                RecordService(
                    record_id=record.id,
                    service_id=_ALLOWED_SERVICE,
                    title="Lash",
                    raw={},
                )
            )
            lead = _make_cross_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            with (
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    mock_fetch,
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                patch(
                    "altegio_bot.promo_loyalty.issue_promo_loyalty_card",
                    mock_card,
                ),
                _network_settings_ctx(
                    promo_altegio_client_api_verified=False,
                    promo_issue_loyalty_card_enabled=True,
                    promo_loyalty_card_api_verified=True,
                    promo_loyalty_card_type_id="ctype_001",
                ),
            ):
                await try_apply_promo_discount(
                    session,
                    record,
                    _COMPANY_DST,
                    booking_created_at=_NOW,
                )

    mock_card.assert_called_once()

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.id == lead_id))).scalar_one()

    bindings = (lead.meta or {}).get("company_bindings", {})
    binding = bindings.get(str(_COMPANY_DST), {})
    assert binding.get("loyalty_card_id") == "new_card_001"
    assert binding.get("loyalty_card_number") == "9999"


# --- A+B. Same-company incomplete lead must not apply via network fallback ----


@pytest.mark.asyncio
async def test_network_incomplete_same_company_lead_not_applied(
    session_maker,
) -> None:
    """A+B: same-company lead with incomplete eligibility must not be applied
    via network fallback (promo_network_apply_enabled=True).

    A: verifies end-to-end: lead stays 'issued', no notification job.
    B: verifies the network fallback excludes the record's own company_id
       so the same-company candidate cannot slip through even if same-company
       lookup misses it due to incomplete eligibility fields.
    """
    mock_fetch = AsyncMock(side_effect=RuntimeError("must not call fetch"))
    mock_put = AsyncMock(side_effect=RuntimeError("must not call PUT"))

    async with session_maker() as session:
        async with session.begin():
            await _seed_cross_client(session)
            record = await _seed_cross_record(session)
            session.add(
                RecordService(
                    record_id=record.id,
                    service_id=_ALLOWED_SERVICE,
                    title="Lash",
                    raw={},
                )
            )
            # Same company as record (_COMPANY_DST=758285) but missing loyalty_card_id.
            # Step 1 (same-company lookup) fails: loyalty_card_id IS NULL filter.
            # Step 2 (network fallback) must not find it: company_id != company_id
            # filter and eligibility filters both exclude it.
            lead = _make_cross_lead(
                company_id=_COMPANY_DST,
                loyalty_card_id=None,
            )
            session.add(lead)
            await session.flush()

            with (
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    mock_fetch,
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _network_settings_ctx(),
            ):
                await try_apply_promo_discount(
                    session,
                    record,
                    _COMPANY_DST,
                    booking_created_at=_NOW,
                )

    async with session_maker() as s:
        lead_db = (await s.execute(select(PromoLead))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead_db.status == "issued"
    assert job is None


# --- C. No external side effects when promo_apply_discount_api_verified=False -


@pytest.mark.asyncio
async def test_network_no_side_effects_when_api_not_verified(
    session_maker,
) -> None:
    """C: promo_apply_discount_api_verified=False → no external Altegio API calls
    and the cross-company lead does NOT transition to 'booked'.

    The cross-company API gate is checked BEFORE the issued→booked transition,
    so a failed gate leaves the lead in 'issued' status without any side effects.
    """
    mock_get_client = AsyncMock(side_effect=RuntimeError("must not call client API"))
    mock_issue_card = AsyncMock(side_effect=RuntimeError("must not call card API"))
    mock_fetch = AsyncMock(side_effect=RuntimeError("must not call fetch"))
    mock_put = AsyncMock(side_effect=RuntimeError("must not call PUT"))

    lead_id: int | None = None

    async with session_maker() as session:
        async with session.begin():
            await _seed_cross_client(session)
            record = await _seed_cross_record(session)
            session.add(
                RecordService(
                    record_id=record.id,
                    service_id=_ALLOWED_SERVICE,
                    title="Lash",
                    raw={},
                )
            )
            lead = _make_cross_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            with (
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    mock_fetch,
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                patch(
                    "altegio_bot.promo_loyalty.get_or_create_altegio_client",
                    mock_get_client,
                ),
                patch(
                    "altegio_bot.promo_loyalty.issue_promo_loyalty_card",
                    mock_issue_card,
                ),
                _network_settings_ctx(
                    promo_apply_discount_api_verified=False,
                    promo_altegio_client_api_verified=True,
                    promo_issue_loyalty_card_enabled=True,
                    promo_loyalty_card_api_verified=True,
                ),
            ):
                await try_apply_promo_discount(
                    session,
                    record,
                    _COMPANY_DST,
                    booking_created_at=_NOW,
                )

    mock_get_client.assert_not_called()
    mock_issue_card.assert_not_called()
    mock_fetch.assert_not_called()
    mock_put.assert_not_called()

    async with session_maker() as s:
        lead_db = (await s.execute(select(PromoLead).where(PromoLead.id == lead_id))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert lead_db.status == "issued", (
        f"cross-company lead must stay 'issued' when api_verified=False, got {lead_db.status!r}"
    )
    assert job is None


# --- D. Cross-company notification job uses record.company_id ----------------


@pytest.mark.asyncio
async def test_network_notification_job_uses_record_company_id(
    session_maker,
) -> None:
    """D: promo_discount_applied MessageJob.company_id equals record.company_id
    (758285), not lead.company_id (1271200), for cross-company apply.
    lead.meta.customer_notification_company_id is also 758285.
    """
    mock_fetch = AsyncMock(return_value=_FAKE_ALTEGIO_RECORD)
    mock_put = AsyncMock(return_value={"data": {"services": []}})

    lead_id: int | None = None

    async with session_maker() as session:
        async with session.begin():
            await _seed_cross_client(session)
            record = await _seed_cross_record(session)
            session.add(
                RecordService(
                    record_id=record.id,
                    service_id=_ALLOWED_SERVICE,
                    title="Lash",
                    raw={},
                )
            )
            lead = _make_cross_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id

            with (
                patch(
                    "altegio_bot.promo_discount_apply.fetch_altegio_record_for_update",
                    mock_fetch,
                ),
                patch(
                    "altegio_bot.promo_discount_apply.update_altegio_record_price_and_comment",
                    mock_put,
                ),
                _network_settings_ctx(),
            ):
                await try_apply_promo_discount(
                    session,
                    record,
                    _COMPANY_DST,
                    booking_created_at=_NOW,
                )

    async with session_maker() as s:
        lead_db = (await s.execute(select(PromoLead).where(PromoLead.id == lead_id))).scalar_one()
        job = (
            await s.execute(select(MessageJob).where(MessageJob.job_type == "promo_discount_applied"))
        ).scalar_one_or_none()

    assert job is not None, "promo_discount_applied job must be created"
    assert job.company_id == _COMPANY_DST, (
        f"job.company_id must be record's company ({_COMPANY_DST}), not lead's ({_COMPANY_SRC}), got {job.company_id}"
    )
    meta = lead_db.meta or {}
    assert meta.get("customer_notification_company_id") == _COMPANY_DST
