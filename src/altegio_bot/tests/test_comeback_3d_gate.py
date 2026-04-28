from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from altegio_bot.models.models import Client, MessageJob, OutboxMessage, Record
from altegio_bot.providers.dummy import DummyProvider
from altegio_bot.workers import outbox_worker as worker_mod


async def _make_comeback_job(
    session,
    *,
    source_starts_at: datetime | None = None,
    has_source_starts_at: bool = True,
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

    job = MessageJob(
        company_id=1,
        record_id=source_record.id,
        client_id=client.id,
        job_type="comeback_3d",
        run_at=worker_mod.utcnow() - timedelta(minutes=1),
        status="queued",
        attempts=0,
        max_attempts=5,
        locked_at=worker_mod.utcnow(),
        dedupe_key=f"comeback_3d:1:{source_record.id}:test",
        payload={},
    )
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


@pytest.mark.asyncio
async def test_comeback_3d_is_canceled_when_client_has_future_appointment(
    session_maker,
    monkeypatch,
) -> None:
    send_mock = AsyncMock()
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)
    monkeypatch.setattr(worker_mod, "safe_send_template", send_mock)

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
    assert refreshed_job is not None
    assert refreshed_job.status == "canceled"
    assert refreshed_job.locked_at is None
    assert refreshed_job.last_error == ("Skipped: client already has a future appointment")
    assert outbox_rows == []


@pytest.mark.asyncio
async def test_comeback_3d_canceled_when_client_returned_after_source(
    session_maker,
    monkeypatch,
) -> None:
    send_mock = AsyncMock()
    monkeypatch.setattr(worker_mod, "safe_send", send_mock)
    monkeypatch.setattr(worker_mod, "safe_send_template", send_mock)

    async with session_maker() as session:
        async with session.begin():
            job = await _make_comeback_job(session)
            source_record = await session.get(Record, job.record_id)
            assert source_record is not None
            later_record = Record(
                company_id=1,
                altegio_record_id=333,
                client_id=source_record.client_id,
                altegio_client_id=9001,
                starts_at=worker_mod.utcnow() - timedelta(days=1),
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
    assert refreshed_job.last_error == ("Skipped: client already returned within comeback_3d window")
    assert outbox_rows == []


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
