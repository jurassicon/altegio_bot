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
                    await try_apply_promo_discount(session, record, _COMPANY, booking_event_received_at=_NOW)

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
                    await try_apply_promo_discount(session, record, _COMPANY, booking_event_received_at=_NOW)

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
                    await try_apply_promo_discount(session, record, _COMPANY, booking_event_received_at=_NOW)

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
                    await try_apply_promo_discount(session, record, _COMPANY, booking_event_received_at=_NOW)

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
