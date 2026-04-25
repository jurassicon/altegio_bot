from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import select

from altegio_bot.altegio_records import AmbiguousRecordError
from altegio_bot.models.models import Client, MessageJob, OutboxMessage, Record
from altegio_bot.providers.dummy import DummyProvider
from altegio_bot.workers import outbox_worker as worker_mod

# ---------------------------------------------------------------------------
# Shared helpers for integration (real DB) tests
# ---------------------------------------------------------------------------

FIXED_NOW = datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc)


async def _make_repeat_job(session) -> MessageJob:
    client = Client(
        company_id=1,
        altegio_client_id=9001,
        display_name="Mala Coronado",
        phone_e164="+491234567890",
        wa_opted_out=False,
        raw={},
    )
    session.add(client)
    await session.flush()

    record = Record(
        company_id=1,
        altegio_record_id=111,
        client_id=client.id,
        altegio_client_id=9001,
        starts_at=worker_mod.utcnow() - timedelta(days=10),
        attendance=1,
        visit_attendance=1,
        is_deleted=False,
        raw={},
    )
    session.add(record)
    await session.flush()

    job = MessageJob(
        company_id=1,
        record_id=record.id,
        client_id=client.id,
        job_type="repeat_10d",
        run_at=worker_mod.utcnow() - timedelta(minutes=1),
        status="queued",
        attempts=0,
        max_attempts=5,
        dedupe_key=f"repeat_10d:1:{record.id}:2026-04-07T12:00:00+00:00",
        payload={},
    )
    session.add(job)
    await session.flush()

    return job


# ---------------------------------------------------------------------------
# Integration tests (real DB + monkeypatched Altegio API)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_repeat_10d_is_canceled_when_client_has_future_appointment(
    session_maker,
    monkeypatch,
) -> None:
    """Guard fires → job canceled, no outbox row, no send attempted."""
    api_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(worker_mod, "client_has_future_appointments", api_mock)

    send_mock = AsyncMock()
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)
    monkeypatch.setattr(worker_mod, "safe_send_template", send_mock)

    async with session_maker() as session:
        async with session.begin():
            job = await _make_repeat_job(session)

        async with session.begin():
            await worker_mod.process_job_in_session(
                session=session,
                job_id=job.id,
                provider=DummyProvider(),
            )

        refreshed_job = await session.get(MessageJob, job.id)
        outbox_rows = (
            (await session.execute(select(OutboxMessage).where(OutboxMessage.job_id == job.id))).scalars().all()
        )

    api_mock.assert_awaited_once_with(
        company_id=1,
        altegio_client_id=9001,
    )
    send_mock.assert_not_called()

    assert refreshed_job is not None
    assert refreshed_job.status == "canceled"
    assert refreshed_job.locked_at is None
    assert refreshed_job.last_error == "Skipped: client already has a future appointment (Altegio API)"
    assert outbox_rows == []


@pytest.mark.asyncio
async def test_repeat_10d_is_requeued_when_altegio_api_fails(
    session_maker,
    monkeypatch,
) -> None:
    """Transient Altegio API error → job requeued with backoff, send not attempted.

    Guard failures use payload['_api_guard_attempts'], not job.attempts, so the
    send-attempt budget is unaffected.
    """
    api_mock = AsyncMock(side_effect=httpx.ReadTimeout("boom"))
    monkeypatch.setattr(worker_mod, "client_has_future_appointments", api_mock)

    send_mock = AsyncMock()
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)
    monkeypatch.setattr(worker_mod, "safe_send_template", send_mock)

    async with session_maker() as session:
        async with session.begin():
            job = await _make_repeat_job(session)
            original_run_at = job.run_at

        async with session.begin():
            await worker_mod.process_job_in_session(
                session=session,
                job_id=job.id,
                provider=DummyProvider(),
            )

        refreshed_job = await session.get(MessageJob, job.id)
        outbox_rows = (
            (await session.execute(select(OutboxMessage).where(OutboxMessage.job_id == job.id))).scalars().all()
        )

    api_mock.assert_awaited_once_with(
        company_id=1,
        altegio_client_id=9001,
    )
    send_mock.assert_not_called()

    assert refreshed_job is not None
    assert refreshed_job.status == "queued"
    # Send-attempt budget is untouched — only guard counter advances.
    assert refreshed_job.attempts == 0
    assert (refreshed_job.payload or {}).get("_api_guard_attempts") == 1
    assert refreshed_job.locked_at is None
    assert refreshed_job.run_at > original_run_at
    assert "Altegio API error" in (refreshed_job.last_error or "")
    assert outbox_rows == []


@pytest.mark.asyncio
async def test_repeat_10d_is_requeued_when_record_date_is_ambiguous(
    session_maker,
    monkeypatch,
) -> None:
    """AmbiguousRecordError → guard requeues (not send, not cancel).

    If an active record's date cannot be parsed, we treat that as an API guard
    error — fail-safe, requeue with backoff, same as any other Altegio failure.
    """
    api_mock = AsyncMock(side_effect=AmbiguousRecordError("record id=77: bad date"))
    monkeypatch.setattr(worker_mod, "client_has_future_appointments", api_mock)

    send_mock = AsyncMock()
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)
    monkeypatch.setattr(worker_mod, "safe_send_template", send_mock)

    async with session_maker() as session:
        async with session.begin():
            job = await _make_repeat_job(session)
            original_run_at = job.run_at

        async with session.begin():
            await worker_mod.process_job_in_session(
                session=session,
                job_id=job.id,
                provider=DummyProvider(),
            )

        refreshed_job = await session.get(MessageJob, job.id)
        outbox_rows = (
            (await session.execute(select(OutboxMessage).where(OutboxMessage.job_id == job.id))).scalars().all()
        )

    send_mock.assert_not_called()

    assert refreshed_job is not None
    assert refreshed_job.status == "queued"
    assert refreshed_job.attempts == 0
    assert (refreshed_job.payload or {}).get("_api_guard_attempts") == 1
    assert refreshed_job.run_at > original_run_at
    assert "Altegio API error" in (refreshed_job.last_error or "")
    assert outbox_rows == []


@pytest.mark.asyncio
async def test_repeat_10d_sends_and_marks_done_when_no_future_appointment(
    session_maker,
    monkeypatch,
) -> None:
    """Happy path: guard passes (no future appointment) → message sent, job done.

    Verifies the full pipeline:
    - guard is called and returns False (no future appointment);
    - send layer is reached and called;
    - outbox row is created with status 'sent';
    - job is marked 'done'.

    send_mode is forced to 'text' so the test is independent of the environment
    setting for whatsapp_send_mode.  DummyProvider.send() succeeds without
    touching any external service.
    """
    monkeypatch.setattr(worker_mod, "client_has_future_appointments", AsyncMock(return_value=False))
    # Force text path so the test is deterministic regardless of env settings.
    monkeypatch.setattr(worker_mod.settings, "whatsapp_send_mode", "text")
    # Mock safe_send explicitly so we can assert it was called.
    # Returns (msg_id, None) — the None error signals success to the worker.
    send_mock = AsyncMock(return_value=("msg-happy-path-001", None))
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)

    # Skip template/sender DB setup by mocking _render_message.
    # sender_id=None is valid (OutboxMessage.sender_id is nullable).
    async def _fake_render(session, *, company_id, template_code, record, client):
        return (
            "Hallo {client_name}, wir vermissen dich!",
            None,
            "de",
            {"client_name": client.display_name if client else ""},
        )

    monkeypatch.setattr(worker_mod, "_render_message", _fake_render)

    async with session_maker() as session:
        async with session.begin():
            job = await _make_repeat_job(session)

        async with session.begin():
            await worker_mod.process_job_in_session(
                session=session,
                job_id=job.id,
                provider=DummyProvider(),
            )

        refreshed_job = await session.get(MessageJob, job.id)
        outbox_rows = (
            (await session.execute(select(OutboxMessage).where(OutboxMessage.job_id == job.id))).scalars().all()
        )

    # send_mock must have been called exactly once on the text path.
    send_mock.assert_awaited_once()
    call_kw = send_mock.call_args
    assert call_kw.kwargs["phone"] == "+491234567890"

    assert refreshed_job is not None
    assert refreshed_job.status == "done"
    assert refreshed_job.locked_at is None
    assert refreshed_job.last_error is None

    assert len(outbox_rows) == 1
    out = outbox_rows[0]
    assert out.status == "sent"
    assert out.template_code == "repeat_10d"
    assert out.phone_e164 == "+491234567890"
    assert "Mala Coronado" in out.body


# ---------------------------------------------------------------------------
# New guard: skip if client already returned after source record
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_repeat_10d_canceled_when_client_returned_after_source(
    session_maker,
    monkeypatch,
) -> None:
    """Guard fires: non-deleted record after source record → job canceled."""
    monkeypatch.setattr(
        worker_mod,
        "client_has_future_appointments",
        AsyncMock(return_value=False),
    )
    send_mock = AsyncMock()
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)
    monkeypatch.setattr(worker_mod, "safe_send_template", send_mock)

    async with session_maker() as session:
        async with session.begin():
            job = await _make_repeat_job(session)
            source_record = await session.get(Record, job.record_id)
            later_record = Record(
                company_id=1,
                altegio_record_id=222,
                client_id=source_record.client_id,
                altegio_client_id=9001,
                starts_at=worker_mod.utcnow() - timedelta(days=5),
                is_deleted=False,
                raw={},
            )
            session.add(later_record)

        async with session.begin():
            await worker_mod.process_job_in_session(
                session=session,
                job_id=job.id,
                provider=DummyProvider(),
            )

        refreshed_job = await session.get(MessageJob, job.id)
        outbox_rows = (
            (await session.execute(select(OutboxMessage).where(OutboxMessage.job_id == job.id))).scalars().all()
        )

    send_mock.assert_not_called()
    assert refreshed_job is not None
    assert refreshed_job.status == "canceled"
    assert refreshed_job.locked_at is None
    assert refreshed_job.last_error == ("Skipped: client already returned within repeat_10d window")
    assert outbox_rows == []


@pytest.mark.asyncio
async def test_repeat_10d_not_canceled_when_no_appointment_after_source(
    session_maker,
    monkeypatch,
) -> None:
    """Guard does not fire when there are no records after source record."""
    monkeypatch.setattr(
        worker_mod,
        "client_has_future_appointments",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(worker_mod.settings, "whatsapp_send_mode", "text")
    send_mock = AsyncMock(return_value=("msg-no-later-001", None))
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)

    async def _fake_render(session, *, company_id, template_code, record, client):
        return (
            "Hallo {client_name}!",
            None,
            "de",
            {"client_name": client.display_name if client else ""},
        )

    monkeypatch.setattr(worker_mod, "_render_message", _fake_render)

    async with session_maker() as session:
        async with session.begin():
            job = await _make_repeat_job(session)

        async with session.begin():
            await worker_mod.process_job_in_session(
                session=session,
                job_id=job.id,
                provider=DummyProvider(),
            )

        refreshed_job = await session.get(MessageJob, job.id)

    send_mock.assert_awaited_once()
    assert refreshed_job is not None
    assert refreshed_job.status == "done"
    assert refreshed_job.last_error is None


@pytest.mark.asyncio
async def test_repeat_10d_not_canceled_when_only_deleted_appointment_after_source(
    session_maker,
    monkeypatch,
) -> None:
    """Deleted record after source record is not a valid return — guard skips."""
    monkeypatch.setattr(
        worker_mod,
        "client_has_future_appointments",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(worker_mod.settings, "whatsapp_send_mode", "text")
    send_mock = AsyncMock(return_value=("msg-deleted-001", None))
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)

    async def _fake_render(session, *, company_id, template_code, record, client):
        return (
            "Hallo {client_name}!",
            None,
            "de",
            {"client_name": client.display_name if client else ""},
        )

    monkeypatch.setattr(worker_mod, "_render_message", _fake_render)

    async with session_maker() as session:
        async with session.begin():
            job = await _make_repeat_job(session)
            source_record = await session.get(Record, job.record_id)
            deleted_later = Record(
                company_id=1,
                altegio_record_id=333,
                client_id=source_record.client_id,
                altegio_client_id=9001,
                starts_at=worker_mod.utcnow() - timedelta(days=3),
                is_deleted=True,
                raw={},
            )
            session.add(deleted_later)

        async with session.begin():
            await worker_mod.process_job_in_session(
                session=session,
                job_id=job.id,
                provider=DummyProvider(),
            )

        refreshed_job = await session.get(MessageJob, job.id)

    send_mock.assert_awaited_once()
    assert refreshed_job is not None
    assert refreshed_job.status == "done"
    assert refreshed_job.last_error is None


# ---------------------------------------------------------------------------
# Unit tests (fake session — no real DB needed)
# ---------------------------------------------------------------------------


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


@dataclass
class _FakeRecord:
    id: int
    company_id: int
    client_id: int | None = 1
    attendance: int = 1
    visit_attendance: int = 0
    is_deleted: bool = False
    starts_at: datetime | None = None
    staff_name: str = "Tanja"
    short_link: str = ""


@dataclass
class _FakeClient:
    id: int
    phone_e164: str | None = "+491234567890"
    wa_opted_out: bool = False
    altegio_client_id: int | None = 9001
    display_name: str = "Test Client"


@dataclass
class _FakeJob:
    id: int
    company_id: int
    job_type: str
    status: str
    run_at: datetime
    record_id: int | None = 1
    client_id: int | None = 1
    last_error: str | None = None
    attempts: int = 0
    max_attempts: int = 5
    locked_at: datetime | None = None
    payload: dict = field(default_factory=dict)


class _FakeSession:
    def __init__(self) -> None:
        self.added: list[Any] = []

    def add(self, obj: Any) -> None:
        self.added.append(obj)


def _make_fake_repeat_job(**overrides: Any) -> _FakeJob:
    defaults: dict[str, Any] = {
        "id": 1,
        "company_id": 1,
        "job_type": "repeat_10d",
        "status": "queued",
        "run_at": FIXED_NOW,
        "record_id": 10,
        "client_id": 1,
    }
    defaults.update(overrides)
    return _FakeJob(**defaults)


def _patch_unit_common(monkeypatch: Any, job: _FakeJob, client: _FakeClient | None = None) -> None:
    """Patch mandatory stubs shared by all unit tests in this module."""
    if client is None:
        client = _FakeClient(id=1)

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_find_success(session: Any, job_id: int) -> Any:
        return None

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return _FakeRecord(id=10, company_id=1)

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return client

    async def fake_safe_send(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("send must not be called in guard tests")

    monkeypatch.setattr(worker_mod, "_load_job", fake_load_job)
    monkeypatch.setattr(worker_mod, "_find_success_outbox", fake_find_success)
    monkeypatch.setattr(worker_mod, "_load_record", fake_load_record)
    monkeypatch.setattr(worker_mod, "_load_client", fake_load_client)
    monkeypatch.setattr(worker_mod, "safe_send", fake_safe_send)
    monkeypatch.setattr(worker_mod, "safe_send_template", fake_safe_send)
    monkeypatch.setattr(worker_mod, "utcnow", lambda: FIXED_NOW)
    monkeypatch.setattr(worker_mod, "_count_131026_failures", AsyncMock(return_value=0))


def test_repeat_10d_guard_passes_when_no_future_appointment(monkeypatch: Any) -> None:
    """Guard returns False → job is NOT canceled by the guard.

    It fails at the render stage (no real infra), but the important thing
    is that the guard itself did not cancel the job.
    """
    job = _make_fake_repeat_job()
    _patch_unit_common(monkeypatch, job)

    monkeypatch.setattr(
        worker_mod,
        "client_has_future_appointments",
        AsyncMock(return_value=False),
    )

    async def fake_apply_rl(session: Any, phone: str) -> Any:
        return None

    async def fake_render(*args: Any, **kwargs: Any) -> Any:
        raise ValueError("render not set up")

    monkeypatch.setattr(worker_mod, "_apply_rate_limit", fake_apply_rl)
    monkeypatch.setattr(worker_mod, "_render_message", fake_render)

    _run(worker_mod.process_job_in_session(_FakeSession(), 1, provider=object()))  # type: ignore

    # Guard must NOT have fired.
    assert job.status != "canceled"
    assert job.last_error != "Skipped: client already has a future appointment (Altegio API)"
    # Pipeline proceeded past the guard and failed at render.
    assert job.status == "failed"
    assert "render" in (job.last_error or "").lower()


def test_repeat_10d_canceled_when_no_altegio_client_id(monkeypatch: Any) -> None:
    """Client with no altegio_client_id → job canceled before API call."""
    job = _make_fake_repeat_job()
    api_mock = AsyncMock()
    _patch_unit_common(
        monkeypatch,
        job,
        client=_FakeClient(id=1, altegio_client_id=None),
    )
    monkeypatch.setattr(worker_mod, "client_has_future_appointments", api_mock)

    _run(worker_mod.process_job_in_session(_FakeSession(), 1, provider=object()))  # type: ignore

    api_mock.assert_not_called()
    assert job.status == "canceled"
    assert "altegio_client_id" in (job.last_error or "")


def test_repeat_10d_guard_fails_permanently_after_max_guard_attempts(
    monkeypatch: Any,
) -> None:
    """API guard failing MAX_API_GUARD_ATTEMPTS times → job permanently failed.

    The send-attempt budget (job.attempts) is never consumed.
    """
    job = _make_fake_repeat_job(payload={"_api_guard_attempts": worker_mod.MAX_API_GUARD_ATTEMPTS - 1})
    _patch_unit_common(monkeypatch, job)

    async def _raise(*_a: Any, **_kw: Any) -> bool:
        raise httpx.ConnectError("down")

    monkeypatch.setattr(worker_mod, "client_has_future_appointments", _raise)

    _run(worker_mod.process_job_in_session(_FakeSession(), 1, provider=object()))  # type: ignore

    assert job.status == "failed"
    assert job.attempts == 0  # send budget untouched
    assert job.payload.get("_api_guard_attempts") == worker_mod.MAX_API_GUARD_ATTEMPTS
    assert "max guard attempts" in (job.last_error or "").lower()


def test_repeat_10d_guard_requeues_on_first_api_failure(monkeypatch: Any) -> None:
    """First API failure → requeued with backoff; guard counter = 1."""
    job = _make_fake_repeat_job()
    _patch_unit_common(monkeypatch, job)

    async def _raise(*_a: Any, **_kw: Any) -> bool:
        raise httpx.ConnectError("timeout")

    monkeypatch.setattr(worker_mod, "client_has_future_appointments", _raise)

    _run(worker_mod.process_job_in_session(_FakeSession(), 1, provider=object()))  # type: ignore

    assert job.status == "queued"
    assert job.attempts == 0
    assert job.payload.get("_api_guard_attempts") == 1
    assert job.run_at == FIXED_NOW + timedelta(seconds=30)  # _retry_delay_seconds(1)
    assert "Altegio API error" in (job.last_error or "")


def test_repeat_10d_ambiguous_date_triggers_guard_error_path(monkeypatch: Any) -> None:
    """AmbiguousRecordError from client_has_future_appointments uses guard error path.

    This is a unit-level check that AmbiguousRecordError is treated identically
    to a network error — requeue with backoff, not cancel, not send.
    """
    job = _make_fake_repeat_job()
    _patch_unit_common(monkeypatch, job)

    async def _raise(*_a: Any, **_kw: Any) -> bool:
        raise AmbiguousRecordError("record id=42: no parseable date")

    monkeypatch.setattr(worker_mod, "client_has_future_appointments", _raise)

    _run(worker_mod.process_job_in_session(_FakeSession(), 1, provider=object()))  # type: ignore

    assert job.status == "queued"
    assert job.attempts == 0
    assert job.payload.get("_api_guard_attempts") == 1
    assert "Altegio API error" in (job.last_error or "")


# ---------------------------------------------------------------------------
# Regression: review_3d guard is unaffected by repeat_10d changes
# ---------------------------------------------------------------------------


def test_review_3d_guard_still_cancels_over_visit_limit(monkeypatch: Any) -> None:
    """review_3d guard must still cancel jobs when visit count exceeds the limit.

    This is a regression check: changes to repeat_10d guard logic must not
    affect the review_3d path.
    """
    from altegio_bot.message_planner import MAX_VISITS_FOR_REVIEW

    job = _FakeJob(
        id=1,
        company_id=1,
        job_type="review_3d",
        status="queued",
        run_at=FIXED_NOW,
        record_id=10,
        client_id=1,
    )
    _patch_unit_common(monkeypatch, job)
    monkeypatch.setattr(
        worker_mod,
        "count_attended_client_visits",
        AsyncMock(return_value=MAX_VISITS_FOR_REVIEW + 1),
    )

    _run(worker_mod.process_job_in_session(_FakeSession(), 1, provider=object()))  # type: ignore

    assert job.status == "canceled"
    assert str(MAX_VISITS_FOR_REVIEW) in (job.last_error or "")
    assert "Altegio API" in (job.last_error or "")


def test_review_3d_guard_requeues_on_api_error_using_guard_counter(
    monkeypatch: Any,
) -> None:
    """review_3d API error → guard counter in payload, send budget untouched."""
    job = _FakeJob(
        id=1,
        company_id=1,
        job_type="review_3d",
        status="queued",
        run_at=FIXED_NOW,
        record_id=10,
        client_id=1,
    )
    _patch_unit_common(monkeypatch, job)

    async def _raise(*_a: Any, **_kw: Any) -> int:
        raise httpx.ConnectError("timeout")

    monkeypatch.setattr(worker_mod, "count_attended_client_visits", _raise)

    _run(worker_mod.process_job_in_session(_FakeSession(), 1, provider=object()))  # type: ignore

    assert job.status == "queued"
    assert job.attempts == 0
    assert job.payload.get("_api_guard_attempts") == 1
    assert job.run_at == FIXED_NOW + timedelta(seconds=30)
    assert "Altegio API error" in (job.last_error or "")
