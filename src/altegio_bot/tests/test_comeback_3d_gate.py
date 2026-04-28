from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from altegio_bot.models.models import Client, MessageJob, OutboxMessage, Record
from altegio_bot.providers.dummy import DummyProvider
from altegio_bot.workers import outbox_worker as worker_mod

UTC = timezone.utc


async def _make_comeback_job(
    session,
    *,
    source_starts_at: datetime | None = None,
    has_source_starts_at: bool = True,
    source_cancelled_at: datetime | None = None,
    include_source_cancelled_at: bool = True,
    job_created_at: datetime | None = None,
    job_run_at: datetime | None = None,
) -> MessageJob:
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

    if source_starts_at is None and has_source_starts_at:
        source_starts_at = worker_mod.utcnow() - timedelta(days=4)
    if not has_source_starts_at:
        source_starts_at = None
    if source_cancelled_at is None:
        source_cancelled_at = worker_mod.utcnow() - timedelta(days=4)

    source_record = Record(
        company_id=1,
        altegio_record_id=111,
        client_id=client.id,
        altegio_client_id=9001,
        starts_at=source_starts_at,
        is_deleted=True,
        raw={},
    )
    session.add(source_record)
    await session.flush()

    payload = {"kind": "comeback_3d"}
    if include_source_cancelled_at:
        payload[worker_mod.COMEBACK_3D_SOURCE_CANCELLED_AT_KEY] = source_cancelled_at.isoformat()

    if job_run_at is None:
        if include_source_cancelled_at or job_created_at is not None:
            job_run_at = source_cancelled_at + timedelta(days=3)
        else:
            job_run_at = worker_mod.utcnow() - timedelta(minutes=1)

    job = MessageJob(
        company_id=1,
        record_id=source_record.id,
        client_id=client.id,
        job_type="comeback_3d",
        run_at=job_run_at,
        status="queued",
        attempts=0,
        max_attempts=5,
        locked_at=worker_mod.utcnow(),
        dedupe_key=f"comeback_3d:1:{source_record.id}:test",
        payload=payload,
    )
    if job_created_at is not None:
        job.created_at = job_created_at
    session.add(job)
    await session.flush()

    return job


def _patch_text_send(monkeypatch, message_id: str = "msg-comeback-001"):
    monkeypatch.setattr(worker_mod.settings, "whatsapp_send_mode", "text")
    send_mock = AsyncMock(return_value=(message_id, None))
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)

    async def _fake_render(
        session,
        *,
        company_id,
        template_code,
        record,
        client,
    ):
        return (
            "Hallo {client_name}!",
            None,
            "de",
            {"client_name": client.display_name if client else ""},
        )

    monkeypatch.setattr(worker_mod, "_render_message", _fake_render)
    return send_mock


def test_resolve_comeback_cancelled_at_falls_back_to_job_created_at() -> None:
    cancelled_at = datetime(2026, 4, 1, 12, 0, tzinfo=UTC)
    job = SimpleNamespace(
        payload={},
        created_at=cancelled_at,
        run_at=cancelled_at + worker_mod.COMEBACK_3D_DELAY,
    )

    assert worker_mod._resolve_comeback_cancelled_at(job, None) == cancelled_at


def test_resolve_comeback_cancelled_at_falls_back_to_run_at_minus_delay() -> None:
    cancelled_at = datetime(2026, 4, 1, 12, 0, tzinfo=UTC)
    job = SimpleNamespace(
        payload={},
        created_at=None,
        run_at=cancelled_at + worker_mod.COMEBACK_3D_DELAY,
    )

    assert worker_mod._resolve_comeback_cancelled_at(job, None) == cancelled_at


@pytest.mark.asyncio
async def test_client_returned_since_filters_invalid_records(session_maker, monkeypatch) -> None:
    processing_now = datetime(2026, 4, 4, 12, 0, tzinfo=UTC)
    cutoff = datetime(2026, 4, 1, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(worker_mod, "utcnow", lambda: processing_now)

    async with session_maker() as session:
        async with session.begin():
            invalid_records = [
                Record(
                    company_id=1,
                    altegio_record_id=501,
                    altegio_client_id=9001,
                    starts_at=datetime(2026, 3, 30, 10, 0, tzinfo=UTC),
                    is_deleted=False,
                    raw={},
                ),
                Record(
                    company_id=1,
                    altegio_record_id=502,
                    altegio_client_id=9001,
                    starts_at=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
                    is_deleted=True,
                    raw={},
                ),
                Record(
                    company_id=1,
                    altegio_record_id=503,
                    altegio_client_id=9001,
                    starts_at=datetime(2026, 4, 5, 10, 0, tzinfo=UTC),
                    is_deleted=False,
                    raw={},
                ),
                Record(
                    company_id=1,
                    altegio_record_id=504,
                    altegio_client_id=9001,
                    starts_at=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
                    confirmed=0,
                    is_deleted=False,
                    raw={},
                ),
                Record(
                    company_id=1,
                    altegio_record_id=505,
                    altegio_client_id=9002,
                    starts_at=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
                    is_deleted=False,
                    raw={},
                ),
            ]
            session.add_all(invalid_records)
            await session.flush()

            assert not await worker_mod._client_returned_since(session, 1, 9001, cutoff)

            valid_record = Record(
                company_id=1,
                altegio_record_id=506,
                altegio_client_id=9001,
                starts_at=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
                is_deleted=False,
                raw={},
            )
            session.add(valid_record)
            await session.flush()

            assert await worker_mod._client_returned_since(session, 1, 9001, cutoff)


@pytest.mark.asyncio
async def test_comeback_3d_is_canceled_when_client_has_future_appointment(
    session_maker,
    monkeypatch,
) -> None:
    send_mock = AsyncMock()
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)
    monkeypatch.setattr(worker_mod, "safe_send_template", send_mock)
    returned_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(worker_mod, "_client_returned_since", returned_mock)

    async with session_maker() as session:
        async with session.begin():
            job = await _make_comeback_job(session)
            source_record = await session.get(Record, job.record_id)
            assert source_record is not None
            future_record = Record(
                company_id=1,
                altegio_record_id=222,
                client_id=source_record.client_id,
                altegio_client_id=9001,
                starts_at=worker_mod.utcnow() + timedelta(days=1),
                is_deleted=False,
                raw={},
            )
            session.add(future_record)

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
    returned_mock.assert_not_awaited()
    assert refreshed_job is not None
    assert refreshed_job.status == "canceled"
    assert refreshed_job.locked_at is None
    assert refreshed_job.last_error == ("Skipped: client already has a future appointment")
    assert outbox_rows == []


@pytest.mark.asyncio
async def test_comeback_3d_canceled_when_client_returned_after_cancel_before_source_date(
    session_maker,
    monkeypatch,
) -> None:
    processing_now = datetime(2026, 4, 4, 12, 0, tzinfo=UTC)
    source_starts_at = datetime(2026, 4, 10, 10, 0, tzinfo=UTC)
    source_cancelled_at = datetime(2026, 4, 1, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(worker_mod, "utcnow", lambda: processing_now)

    send_mock = AsyncMock()
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)
    monkeypatch.setattr(worker_mod, "safe_send_template", send_mock)

    async with session_maker() as session:
        async with session.begin():
            job = await _make_comeback_job(
                session,
                source_starts_at=source_starts_at,
                source_cancelled_at=source_cancelled_at,
                job_run_at=processing_now,
            )
            source_record = await session.get(Record, job.record_id)
            assert source_record is not None
            later_record = Record(
                company_id=1,
                altegio_record_id=333,
                client_id=source_record.client_id,
                altegio_client_id=9001,
                starts_at=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
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
    assert refreshed_job.last_error == worker_mod.COMEBACK_3D_ALREADY_RETURNED_REASON
    assert outbox_rows == []


@pytest.mark.asyncio
async def test_comeback_3d_not_canceled_when_client_visited_before_cancel(
    session_maker,
    monkeypatch,
) -> None:
    processing_now = datetime(2026, 4, 4, 12, 0, tzinfo=UTC)
    source_starts_at = datetime(2026, 4, 10, 10, 0, tzinfo=UTC)
    source_cancelled_at = datetime(2026, 4, 1, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(worker_mod, "utcnow", lambda: processing_now)
    send_mock = _patch_text_send(monkeypatch, "msg-before-cancel-001")

    async with session_maker() as session:
        async with session.begin():
            job = await _make_comeback_job(
                session,
                source_starts_at=source_starts_at,
                source_cancelled_at=source_cancelled_at,
                job_run_at=processing_now,
            )
            source_record = await session.get(Record, job.record_id)
            assert source_record is not None
            before_cancel_record = Record(
                company_id=1,
                altegio_record_id=334,
                client_id=source_record.client_id,
                altegio_client_id=9001,
                starts_at=datetime(2026, 3, 30, 10, 0, tzinfo=UTC),
                is_deleted=False,
                raw={},
            )
            session.add(before_cancel_record)

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
    assert refreshed_job.locked_at is None
    assert refreshed_job.last_error is None


@pytest.mark.asyncio
async def test_comeback_3d_canceled_when_client_returned_after_source_date(
    session_maker,
    monkeypatch,
) -> None:
    processing_now = datetime(2026, 4, 12, 12, 0, tzinfo=UTC)
    source_starts_at = datetime(2026, 4, 10, 10, 0, tzinfo=UTC)
    source_cancelled_at = datetime(2026, 4, 1, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(worker_mod, "utcnow", lambda: processing_now)

    send_mock = AsyncMock()
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)
    monkeypatch.setattr(worker_mod, "safe_send_template", send_mock)

    async with session_maker() as session:
        async with session.begin():
            job = await _make_comeback_job(
                session,
                source_starts_at=source_starts_at,
                source_cancelled_at=source_cancelled_at,
                job_run_at=processing_now,
            )
            source_record = await session.get(Record, job.record_id)
            assert source_record is not None
            returned_record = Record(
                company_id=1,
                altegio_record_id=335,
                client_id=source_record.client_id,
                altegio_client_id=9001,
                starts_at=datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                is_deleted=False,
                raw={},
            )
            session.add(returned_record)

        async with session.begin():
            await worker_mod.process_job_in_session(
                session=session,
                job_id=job.id,
                provider=DummyProvider(),
            )

        refreshed_job = await session.get(MessageJob, job.id)

    send_mock.assert_not_called()
    assert refreshed_job is not None
    assert refreshed_job.status == "canceled"
    assert refreshed_job.locked_at is None
    assert refreshed_job.last_error == worker_mod.COMEBACK_3D_ALREADY_RETURNED_REASON


@pytest.mark.asyncio
async def test_comeback_3d_not_canceled_when_no_appointment_after_source(
    session_maker,
    monkeypatch,
) -> None:
    send_mock = _patch_text_send(monkeypatch, "msg-no-later-001")

    async with session_maker() as session:
        async with session.begin():
            job = await _make_comeback_job(session)

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

    send_mock.assert_awaited_once()
    assert refreshed_job is not None
    assert refreshed_job.status == "done"
    assert refreshed_job.locked_at is None
    assert refreshed_job.last_error is None
    assert len(outbox_rows) == 1
    assert outbox_rows[0].status == "sent"


@pytest.mark.asyncio
async def test_comeback_3d_not_canceled_when_only_deleted_after_source(
    session_maker,
    monkeypatch,
) -> None:
    send_mock = _patch_text_send(monkeypatch, "msg-deleted-later-001")

    async with session_maker() as session:
        async with session.begin():
            job = await _make_comeback_job(session)
            source_record = await session.get(Record, job.record_id)
            assert source_record is not None
            deleted_later = Record(
                company_id=1,
                altegio_record_id=444,
                client_id=source_record.client_id,
                altegio_client_id=9001,
                starts_at=worker_mod.utcnow() - timedelta(days=1),
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
    assert refreshed_job.locked_at is None
    assert refreshed_job.last_error is None


@pytest.mark.asyncio
async def test_comeback_3d_keeps_last_30_days_sent_guard(
    session_maker,
    monkeypatch,
) -> None:
    send_mock = AsyncMock()
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)
    monkeypatch.setattr(worker_mod, "safe_send_template", send_mock)
    returned_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(worker_mod, "_client_returned_since", returned_mock)

    async with session_maker() as session:
        async with session.begin():
            job = await _make_comeback_job(session)
            previous_sent = OutboxMessage(
                company_id=job.company_id,
                client_id=job.client_id,
                record_id=None,
                job_id=None,
                sender_id=None,
                phone_e164="+491234567890",
                template_code="comeback_3d",
                language="de",
                body="previous",
                status="sent",
                error=None,
                provider_message_id="previous-comeback",
                scheduled_at=worker_mod.utcnow() - timedelta(days=1),
                sent_at=worker_mod.utcnow() - timedelta(days=1),
                meta={},
            )
            session.add(previous_sent)

        async with session.begin():
            await worker_mod.process_job_in_session(
                session=session,
                job_id=job.id,
                provider=DummyProvider(),
            )

        refreshed_job = await session.get(MessageJob, job.id)

    send_mock.assert_not_called()
    returned_mock.assert_not_awaited()
    assert refreshed_job is not None
    assert refreshed_job.status == "canceled"
    assert refreshed_job.locked_at is None
    assert refreshed_job.last_error == ("Skipped: comeback_3d already sent in the last 30 days")


@pytest.mark.asyncio
async def test_comeback_3d_canceled_when_source_record_is_missing(
    session_maker,
    monkeypatch,
) -> None:
    send_mock = AsyncMock()
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)
    monkeypatch.setattr(worker_mod, "safe_send_template", send_mock)

    async with session_maker() as session:
        async with session.begin():
            job = MessageJob(
                company_id=1,
                record_id=None,
                client_id=None,
                job_type="comeback_3d",
                run_at=worker_mod.utcnow() - timedelta(minutes=1),
                status="queued",
                attempts=0,
                max_attempts=5,
                locked_at=worker_mod.utcnow(),
                dedupe_key="comeback_3d:1:missing-source:test",
                payload={},
            )
            session.add(job)
            await session.flush()

        async with session.begin():
            await worker_mod.process_job_in_session(
                session=session,
                job_id=job.id,
                provider=DummyProvider(),
            )

        refreshed_job = await session.get(MessageJob, job.id)

    send_mock.assert_not_called()
    assert refreshed_job is not None
    assert refreshed_job.status == "canceled"
    assert refreshed_job.locked_at is None
    assert refreshed_job.last_error == "Skipped: source record missing for comeback_3d"


@pytest.mark.asyncio
async def test_comeback_3d_canceled_when_source_starts_at_is_missing(
    session_maker,
    monkeypatch,
) -> None:
    send_mock = AsyncMock()
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)
    monkeypatch.setattr(worker_mod, "safe_send_template", send_mock)

    async with session_maker() as session:
        async with session.begin():
            job = await _make_comeback_job(
                session,
                has_source_starts_at=False,
            )

        async with session.begin():
            await worker_mod.process_job_in_session(
                session=session,
                job_id=job.id,
                provider=DummyProvider(),
            )

        refreshed_job = await session.get(MessageJob, job.id)

    send_mock.assert_not_called()
    assert refreshed_job is not None
    assert refreshed_job.status == "canceled"
    assert refreshed_job.locked_at is None
    assert refreshed_job.last_error == ("Skipped: source record starts_at missing for comeback_3d")


@pytest.mark.asyncio
async def test_comeback_3d_not_suppressed_when_below_131026_threshold(
    session_maker,
    monkeypatch,
) -> None:
    """comeback_3d passes all guards and reaches send when 131026 failures < threshold."""
    send_mock = _patch_text_send(monkeypatch, "msg-131026-below-001")
    monkeypatch.setattr(worker_mod, "_count_131026_failures", AsyncMock(return_value=1))

    async with session_maker() as session:
        async with session.begin():
            job = await _make_comeback_job(session)

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
    assert "suppressed_131026" not in (refreshed_job.last_error or "")


@pytest.mark.asyncio
async def test_comeback_3d_suppressed_when_above_131026_threshold(
    session_maker,
    monkeypatch,
) -> None:
    """comeback_3d is canceled (not sent) when 131026 failures >= threshold."""
    send_mock = AsyncMock()
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)
    monkeypatch.setattr(worker_mod, "safe_send_template", send_mock)
    monkeypatch.setattr(worker_mod, "_count_131026_failures", AsyncMock(return_value=2))

    async with session_maker() as session:
        async with session.begin():
            job = await _make_comeback_job(session)

        async with session.begin():
            await worker_mod.process_job_in_session(
                session=session,
                job_id=job.id,
                provider=DummyProvider(),
            )

        refreshed_job = await session.get(MessageJob, job.id)

    send_mock.assert_not_called()
    assert refreshed_job is not None
    assert refreshed_job.status == "canceled"
    assert refreshed_job.locked_at is None
    assert (refreshed_job.last_error or "").startswith("suppressed_131026")
