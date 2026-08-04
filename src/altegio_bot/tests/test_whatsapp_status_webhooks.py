"""Tests: WhatsApp delivery status webhook handling.

Covers:
- status=delivered updates OutboxMessage from sent to delivered
- status=read updates OutboxMessage from delivered to read
- duplicate delivered/read webhook is idempotent
- status regression is ignored (read must not become delivered)
- campaign run recompute triggered after status update
- inbound message flow still works after the change
- STOP/START flow still works after the change
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from altegio_bot.models.models import (
    CampaignRecipient,
    CampaignRun,
    MessageJob,
    OutboxMessage,
    Record,
    WhatsAppEvent,
    WhatsAppSender,
)
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.workers import outbox_worker as ow
from altegio_bot.workers.whatsapp_inbox_worker import (
    _apply_status_updates,
    _extract_status_updates,
    handle_event,
)

WAMID = "wamid.TEST001"
PHONE_NUMBER_ID = "PNID_STATUS"
PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 4, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class _NullProvider(WhatsAppProvider):
    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
        contact_name: str | None = None,
    ) -> str:
        return "msg-null"


def _status_payload(
    phone_number_id: str,
    wamid: str,
    status: str,
) -> dict[str, Any]:
    """Build a minimal Meta status-webhook payload."""
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {
                                "phone_number_id": phone_number_id,
                            },
                            "statuses": [
                                {
                                    "id": wamid,
                                    "status": status,
                                    "timestamp": "1700000001",
                                    "recipient_id": "4915100000001",
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


def _failed_status_payload(
    phone_number_id: str,
    wamid: str,
    *,
    code: int,
    title: str = "Delivery failed",
    details: str = "temporary provider failure",
) -> dict[str, Any]:
    payload = _status_payload(phone_number_id, wamid, "failed")
    status = payload["entry"][0]["changes"][0]["value"]["statuses"][0]
    status["errors"] = [
        {
            "code": code,
            "title": title,
            "error_data": {"details": details},
        }
    ]
    return payload


def _message_payload(
    phone_number_id: str,
    from_phone: str,
    text: str,
) -> dict[str, Any]:
    """Build a minimal Meta inbound-message payload."""
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {
                                "phone_number_id": phone_number_id,
                            },
                            "messages": [
                                {
                                    "from": from_phone,
                                    "id": "wamid.INBOUND",
                                    "timestamp": "1700000002",
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


async def _setup_outbox_with_campaign(
    session_maker,
    *,
    outbox_status: str = "sent",
) -> tuple[int, int, int]:
    """Create CampaignRun + CampaignRecipient + OutboxMessage.

    Returns (run_id, recipient_id, outbox_id).
    """
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code="test_campaign",
                mode="send-real",
                company_ids=[1],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
                total_clients_seen=1,
                candidates_count=1,
                sent_count=1,
                provider_accepted_count=1,
            )
            session.add(run)
            await session.flush()

            ob = OutboxMessage(
                company_id=1,
                phone_e164="+4915100000001",
                template_code="test_tpl",
                body="Hello",
                status=outbox_status,
                provider_message_id=WAMID,
                scheduled_at=_utcnow(),
                sent_at=_utcnow(),
            )
            session.add(ob)
            await session.flush()

            recipient = CampaignRecipient(
                campaign_run_id=run.id,
                company_id=1,
                phone_e164="+4915100000001",
                status="provider_accepted",
                outbox_message_id=ob.id,
                provider_message_id=WAMID,
            )
            session.add(recipient)
            await session.flush()

            return int(run.id), int(recipient.id), int(ob.id)


async def _setup_service_outbox(
    session_maker,
    *,
    job_type: str = "record_created",
    outbox_status: str = "sent",
    wamid: str = WAMID,
    altegio_record_id: int = 777,
) -> tuple[int, int, int]:
    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=1,
                altegio_record_id=altegio_record_id,
                client_id=1,
                altegio_client_id=1,
                starts_at=_utcnow().replace(microsecond=0),
                raw={},
            )
            record.starts_at = record.starts_at.replace(year=2035)
            session.add(record)
            await session.flush()

            job = MessageJob(
                company_id=1,
                record_id=record.id,
                client_id=1,
                job_type=job_type,
                run_at=_utcnow(),
                status="done",
                attempts=1,
                max_attempts=5,
                dedupe_key=f"orig:{wamid}",
                payload={},
            )
            session.add(job)
            await session.flush()

            ob = OutboxMessage(
                company_id=1,
                client_id=1,
                record_id=record.id,
                job_id=job.id,
                phone_e164="+4915100000001",
                template_code=job_type,
                body="Hello",
                status=outbox_status,
                provider_message_id=wamid,
                scheduled_at=_utcnow(),
                sent_at=_utcnow(),
            )
            session.add(ob)
            await session.flush()
            return int(record.id), int(job.id), int(ob.id)


async def _setup_retry_outbox(
    session_maker,
    *,
    original_wamid: str = "wamid.ORIGINAL",
    retry_wamid: str = WAMID,
) -> tuple[int, int, int]:
    record_id, _, original_outbox_id = await _setup_service_outbox(
        session_maker,
        job_type="record_created",
        wamid=original_wamid,
    )
    async with session_maker() as session:
        async with session.begin():
            original = await session.get(OutboxMessage, original_outbox_id)
            assert original is not None
            original_job = await session.get(MessageJob, original.job_id)
            assert original_job is not None
            retry_job = MessageJob(
                provider=original_job.provider,
                company_id=original_job.company_id,
                record_id=record_id,
                client_id=original_job.client_id,
                job_type=original_job.job_type,
                run_at=_utcnow(),
                status="done",
                attempts=1,
                max_attempts=5,
                dedupe_key=f"delivery_retry:{original_outbox_id}:1",
                payload={
                    "kind": "delivery_failed_retry",
                    "delivery_retry_of_outbox_id": original_outbox_id,
                    "delivery_retry_attempt": 1,
                    "delivery_retry_original_outbox_id": original_outbox_id,
                },
            )
            session.add(retry_job)
            await session.flush()
            retry_outbox = OutboxMessage(
                company_id=original.company_id,
                client_id=original.client_id,
                record_id=original.record_id,
                job_id=retry_job.id,
                phone_e164=original.phone_e164,
                template_code=original.template_code,
                body="Retry",
                status="sent",
                provider_message_id=retry_wamid,
                scheduled_at=_utcnow(),
                sent_at=_utcnow(),
                meta={
                    "delivery_retry": True,
                    "delivery_retry_of_outbox_id": original_outbox_id,
                    "delivery_retry_attempt": 1,
                },
            )
            session.add(retry_outbox)
            await session.flush()
            return original_outbox_id, int(retry_job.id), int(retry_outbox.id)


# ---------------------------------------------------------------------------
# Unit tests: _extract_status_updates
# ---------------------------------------------------------------------------


def test_extract_status_updates_delivered() -> None:
    payload = _status_payload(PHONE_NUMBER_ID, WAMID, "delivered")
    updates = _extract_status_updates(payload)
    assert len(updates) == 1
    assert updates[0]["wamid"] == WAMID
    assert updates[0]["status"] == "delivered"


def test_extract_status_updates_read() -> None:
    payload = _status_payload(PHONE_NUMBER_ID, WAMID, "read")
    updates = _extract_status_updates(payload)
    assert len(updates) == 1
    assert updates[0]["status"] == "read"


def test_extract_status_updates_unknown_skipped() -> None:
    payload = _status_payload(PHONE_NUMBER_ID, WAMID, "deleted")
    updates = _extract_status_updates(payload)
    assert updates == []


def test_extract_status_updates_empty_payload() -> None:
    assert _extract_status_updates({}) == []


def test_extract_status_updates_message_payload_returns_empty() -> None:
    """Inbound message payloads have no statuses — must return empty."""
    payload = _message_payload(PHONE_NUMBER_ID, "4915100000001", "hello")
    updates = _extract_status_updates(payload)
    assert updates == []


# ---------------------------------------------------------------------------
# Integration: _apply_status_updates
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delivered_advances_outbox_from_sent(session_maker) -> None:
    run_id, recipient_id, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="sent")

    async with session_maker() as session:
        async with session.begin():
            updates = [
                {
                    "wamid": WAMID,
                    "status": "delivered",
                    "timestamp": "1700000001",
                    "raw": {"id": WAMID, "status": "delivered"},
                }
            ]
            affected_run_ids = await _apply_status_updates(session, updates)

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "delivered"
        assert "wa_status_delivered" in (ob.meta or {})

    assert run_id in affected_run_ids


@pytest.mark.asyncio
async def test_read_advances_outbox_from_delivered(session_maker) -> None:
    run_id, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="delivered")

    async with session_maker() as session:
        async with session.begin():
            updates = [
                {
                    "wamid": WAMID,
                    "status": "read",
                    "timestamp": "1700000002",
                    "raw": {"id": WAMID, "status": "read"},
                }
            ]
            await _apply_status_updates(session, updates)

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "read"


@pytest.mark.asyncio
async def test_duplicate_delivered_is_idempotent(session_maker) -> None:
    _, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="delivered")

    async with session_maker() as session:
        async with session.begin():
            updates = [
                {
                    "wamid": WAMID,
                    "status": "delivered",
                    "timestamp": "1700000001",
                    "raw": {},
                }
            ]
            run_ids = await _apply_status_updates(session, updates)

    # No advancement — rank did not increase → no run_ids returned.
    assert run_ids == []

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "delivered"


@pytest.mark.asyncio
async def test_status_regression_ignored(session_maker) -> None:
    """read must not regress to delivered."""
    _, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="read")

    async with session_maker() as session:
        async with session.begin():
            updates = [
                {
                    "wamid": WAMID,
                    "status": "delivered",
                    "timestamp": "1700000001",
                    "raw": {},
                }
            ]
            run_ids = await _apply_status_updates(session, updates)

    assert run_ids == []

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "read"  # unchanged


@pytest.mark.asyncio
async def test_unknown_wamid_no_crash(session_maker) -> None:
    """Webhook for an unknown wamid must not raise."""
    async with session_maker() as session:
        async with session.begin():
            updates = [
                {
                    "wamid": "wamid.DOESNOTEXIST",
                    "status": "delivered",
                    "timestamp": "1700000001",
                    "raw": {},
                }
            ]
            run_ids = await _apply_status_updates(session, updates)

    assert run_ids == []


# ---------------------------------------------------------------------------
# Integration: handle_event with status payload
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_event_delivered_triggers_recompute(
    session_maker,
) -> None:
    """handle_event with a delivered status payload advances OutboxMessage
    and calls recompute_campaign_run_stats for the linked run."""
    run_id, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="sent")

    provider = _NullProvider()
    recomputed: list[int] = []

    async def _fake_recompute(session, run_id_arg: int) -> dict:
        recomputed.append(run_id_arg)
        return {}

    payload = _status_payload(PHONE_NUMBER_ID, WAMID, "delivered")

    with patch(
        "altegio_bot.workers.whatsapp_inbox_worker.recompute_campaign_run_stats",
        side_effect=_fake_recompute,
    ):
        async with session_maker() as session:
            async with session.begin():
                evt = WhatsAppEvent(
                    dedupe_key="wa:status-delivered-1",
                    status="received",
                    query={},
                    headers={},
                    payload=payload,
                )
                session.add(evt)
                await session.flush()
                await handle_event(session, evt, provider)

    assert run_id in recomputed

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "delivered"


@pytest.mark.asyncio
async def test_handle_event_read_advances_to_read(session_maker) -> None:
    run_id, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="delivered")

    provider = _NullProvider()

    payload = _status_payload(PHONE_NUMBER_ID, WAMID, "read")

    with patch(
        "altegio_bot.workers.whatsapp_inbox_worker.recompute_campaign_run_stats",
        new_callable=AsyncMock,
        return_value={},
    ):
        async with session_maker() as session:
            async with session.begin():
                evt = WhatsAppEvent(
                    dedupe_key="wa:status-read-1",
                    status="received",
                    query={},
                    headers={},
                    payload=payload,
                )
                session.add(evt)
                await session.flush()
                await handle_event(session, evt, provider)

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "read"


# ---------------------------------------------------------------------------
# Status callback: sent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sent_callback_noop_when_already_sent(session_maker) -> None:
    """Meta 'sent' callback when OutboxMessage is already 'sent' must be a no-op."""
    _, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="sent")

    async with session_maker() as session:
        async with session.begin():
            updates = [
                {
                    "wamid": WAMID,
                    "status": "sent",
                    "timestamp": "1700000000",
                    "raw": {"id": WAMID, "status": "sent"},
                }
            ]
            run_ids = await _apply_status_updates(session, updates)

    assert run_ids == []

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "sent"


@pytest.mark.asyncio
async def test_sent_callback_advances_from_queued(session_maker) -> None:
    """Meta 'sent' callback when OutboxMessage is still 'queued' must advance to 'sent'."""
    _, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="queued")

    async with session_maker() as session:
        async with session.begin():
            updates = [
                {
                    "wamid": WAMID,
                    "status": "sent",
                    "timestamp": "1700000000",
                    "raw": {"id": WAMID, "status": "sent"},
                }
            ]
            run_ids = await _apply_status_updates(session, updates)

    assert run_ids != []

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "sent"


@pytest.mark.asyncio
async def test_no_match_by_provider_message_id(session_maker) -> None:
    """Callback for a different wamid must not update unrelated OutboxMessage."""
    _, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="sent")

    async with session_maker() as session:
        async with session.begin():
            updates = [
                {
                    "wamid": "wamid.DIFFERENT_WAMID",
                    "status": "delivered",
                    "timestamp": "1700000001",
                    "raw": {},
                }
            ]
            run_ids = await _apply_status_updates(session, updates)

    assert run_ids == []

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "sent"  # unchanged


@pytest.mark.asyncio
async def test_full_status_path_sent_delivered_read(session_maker) -> None:
    """Full progression: sent → delivered → read via sequential callbacks."""
    _, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="sent")

    # Step 1: delivered
    async with session_maker() as session:
        async with session.begin():
            await _apply_status_updates(
                session,
                [{"wamid": WAMID, "status": "delivered", "timestamp": "1700000001", "raw": {}}],
            )

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "delivered"

    # Step 2: read
    async with session_maker() as session:
        async with session.begin():
            await _apply_status_updates(
                session,
                [{"wamid": WAMID, "status": "read", "timestamp": "1700000002", "raw": {}}],
            )

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "read"


@pytest.mark.asyncio
async def test_failed_131026_marks_failed_without_retry(session_maker) -> None:
    _, _, outbox_id = await _setup_service_outbox(session_maker, job_type="record_created")
    payload = _failed_status_payload(PHONE_NUMBER_ID, WAMID, code=131026, title="Undeliverable")

    async with session_maker() as session:
        async with session.begin():
            evt = WhatsAppEvent(dedupe_key="wa:failed-131026", status="received", payload=payload, query={}, headers={})
            session.add(evt)
            await session.flush()
            await handle_event(session, evt, _NullProvider())

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "failed"
        assert (ob.meta or {})["delivery_failed_code"] == 131026
        retry_jobs = await session.execute(select(MessageJob).where(MessageJob.dedupe_key.like("delivery_retry:%")))
        assert list(retry_jobs.scalars().all()) == []


@pytest.mark.asyncio
async def test_failed_code_10_permission_marks_failed_without_retry(session_maker) -> None:
    _, _, outbox_id = await _setup_service_outbox(session_maker, job_type="record_created")
    payload = _failed_status_payload(PHONE_NUMBER_ID, WAMID, code=10, title="Permission")

    async with session_maker() as session:
        async with session.begin():
            evt = WhatsAppEvent(
                dedupe_key="wa:failed-code-10-permission",
                status="received",
                payload=payload,
                query={},
                headers={},
            )
            session.add(evt)
            await session.flush()
            await handle_event(session, evt, _NullProvider())

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "failed"
        stmt = select(MessageJob).where(MessageJob.dedupe_key == f"delivery_retry:{outbox_id}:1")
        retry = (await session.execute(stmt)).scalar_one_or_none()
        assert retry is None


@pytest.mark.asyncio
async def test_failed_code_10_ambiguous_wording_stays_permanent(session_maker) -> None:
    """code=10 with BOTH a transient ('temporarily') and permanent ('access')
    hint stays permanent.

    This pins the intended wording trade-off: for code=10, permanent keywords
    take precedence over transient ones, so an ambiguous failure is never
    auto-retried (fail-safe against silently retrying a real permission/auth
    problem).
    """
    _, _, outbox_id = await _setup_service_outbox(session_maker, job_type="record_created")
    payload = _failed_status_payload(
        PHONE_NUMBER_ID,
        WAMID,
        code=10,
        title="Service temporarily unavailable",
        details="access restricted for this number",
    )

    async with session_maker() as session:
        async with session.begin():
            evt = WhatsAppEvent(
                dedupe_key="wa:failed-code-10-ambiguous",
                status="received",
                payload=payload,
                query={},
                headers={},
            )
            session.add(evt)
            await session.flush()
            await handle_event(session, evt, _NullProvider())

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "failed"
        stmt = select(MessageJob).where(MessageJob.dedupe_key == f"delivery_retry:{outbox_id}:1")
        retry = (await session.execute(stmt)).scalar_one_or_none()
        assert retry is None


@pytest.mark.asyncio
async def test_failed_code_10_unknown_wording_defaults_permanent(session_maker) -> None:
    """code=10 with no permanent and no transient hint defaults to permanent.

    code=10 is no longer unconditionally retryable; absent an explicit transient
    hint it is treated as permanent (no delivery retry).
    """
    _, _, outbox_id = await _setup_service_outbox(session_maker, job_type="record_created")
    payload = _failed_status_payload(
        PHONE_NUMBER_ID,
        WAMID,
        code=10,
        title="Generic delivery failure",
        details="unspecified",
    )

    async with session_maker() as session:
        async with session.begin():
            evt = WhatsAppEvent(
                dedupe_key="wa:failed-code-10-unknown",
                status="received",
                payload=payload,
                query={},
                headers={},
            )
            session.add(evt)
            await session.flush()
            await handle_event(session, evt, _NullProvider())

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "failed"
        stmt = select(MessageJob).where(MessageJob.dedupe_key == f"delivery_retry:{outbox_id}:1")
        retry = (await session.execute(stmt)).scalar_one_or_none()
        assert retry is None


@pytest.mark.asyncio
async def test_failed_code_10_transient_only_wording_retries(session_maker) -> None:
    """code=10 with ONLY a transient hint (no permission/auth/access wording)
    is the explicit escape hatch and schedules a delivery retry.

    This documents that the permanent-by-default for code=10 can still be
    overridden by an unambiguous transient signal.
    """
    _, _, outbox_id = await _setup_service_outbox(session_maker, job_type="record_created")
    payload = _failed_status_payload(
        PHONE_NUMBER_ID,
        WAMID,
        code=10,
        title="Please try again later",
        details="service temporarily overloaded",
    )

    async with session_maker() as session:
        async with session.begin():
            evt = WhatsAppEvent(
                dedupe_key="wa:failed-code-10-transient",
                status="received",
                payload=payload,
                query={},
                headers={},
            )
            session.add(evt)
            await session.flush()
            await handle_event(session, evt, _NullProvider())

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "failed"
        stmt = select(MessageJob).where(MessageJob.dedupe_key == f"delivery_retry:{outbox_id}:1")
        retry = (await session.execute(stmt)).scalar_one_or_none()
        assert retry is not None
        assert retry.status == "queued"
        assert retry.payload["delivery_retry_of_outbox_id"] == outbox_id


@pytest.mark.asyncio
async def test_failed_transient_code_schedules_service_delivery_retry(session_maker) -> None:
    _, _, outbox_id = await _setup_service_outbox(session_maker, job_type="record_created")
    payload = _failed_status_payload(PHONE_NUMBER_ID, WAMID, code=131000, title="Temporary failure")

    async with session_maker() as session:
        async with session.begin():
            evt = WhatsAppEvent(
                dedupe_key="wa:failed-transient-131000",
                status="received",
                payload=payload,
                query={},
                headers={},
            )
            session.add(evt)
            await session.flush()
            await handle_event(session, evt, _NullProvider())

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "failed"
        stmt = select(MessageJob).where(MessageJob.dedupe_key == f"delivery_retry:{outbox_id}:1")
        retry = (await session.execute(stmt)).scalar_one_or_none()
        assert retry is not None
        assert retry.status == "queued"
        assert retry.job_type == "record_created"
        assert retry.payload["kind"] == "delivery_failed_retry"
        assert retry.payload["delivery_retry_of_outbox_id"] == outbox_id
        assert retry.payload["delivery_retry_attempt"] == 1
        assert "_original_run_at" in retry.payload


@pytest.mark.asyncio
async def test_duplicate_failed_webhook_dedupes_retry_job(session_maker) -> None:
    _, _, outbox_id = await _setup_service_outbox(session_maker, job_type="record_created")
    payload = _failed_status_payload(PHONE_NUMBER_ID, WAMID, code=131000, title="Temporary failure")

    for idx in range(2):
        async with session_maker() as session:
            async with session.begin():
                evt = WhatsAppEvent(
                    dedupe_key=f"wa:failed-131000-dupe-{idx}",
                    status="received",
                    payload=payload,
                    query={},
                    headers={},
                )
                session.add(evt)
                await session.flush()
                await handle_event(session, evt, _NullProvider())

    async with session_maker() as session:
        stmt = select(MessageJob).where(MessageJob.dedupe_key.like(f"delivery_retry:{outbox_id}:%"))
        retries = list((await session.execute(stmt)).scalars().all())
        assert len(retries) == 1


@pytest.mark.asyncio
async def test_valid_altegio_retry_callback_creates_next_attempt(session_maker) -> None:
    original_id, _, retry_outbox_id = await _setup_retry_outbox(session_maker)

    async with session_maker() as session:
        async with session.begin():
            await _apply_status_updates(
                session,
                [{"wamid": WAMID, "status": "failed", "timestamp": "1", "raw": {}}],
            )

    async with session_maker() as session:
        current = await session.get(OutboxMessage, retry_outbox_id)
        assert current is not None
        assert current.status == "failed"
        next_retry = (
            await session.execute(select(MessageJob).where(MessageJob.dedupe_key == f"delivery_retry:{original_id}:2"))
        ).scalar_one_or_none()
        assert next_retry is not None
        assert next_retry.payload["delivery_retry_attempt"] == 2


@pytest.mark.asyncio
async def test_failed_retry_callback_does_not_follow_foreign_meta_pointer(session_maker) -> None:
    original_id, _, retry_outbox_id = await _setup_retry_outbox(session_maker)
    _, _, foreign_outbox_id = await _setup_service_outbox(
        session_maker,
        wamid="wamid.FOREIGN",
        altegio_record_id=778,
    )

    async with session_maker() as session:
        async with session.begin():
            current = await session.get(OutboxMessage, retry_outbox_id)
            assert current is not None
            current.meta = {
                **(current.meta or {}),
                "delivery_retry_of_outbox_id": foreign_outbox_id,
            }
            await _apply_status_updates(
                session,
                [{"wamid": WAMID, "status": "failed", "timestamp": "1", "raw": {}}],
            )

    async with session_maker() as session:
        current = await session.get(OutboxMessage, retry_outbox_id)
        assert current is not None
        assert current.status == "failed"
        assert current.meta["delivery_retry_chain_refusal_reason"] == "retry_outbox_audit_reference_mismatch"
        created = await session.execute(
            select(MessageJob).where(
                MessageJob.dedupe_key.in_([f"delivery_retry:{original_id}:2", f"delivery_retry:{foreign_outbox_id}:2"])
            )
        )
        assert list(created.scalars().all()) == []


@pytest.mark.asyncio
async def test_invalid_retry_meta_does_not_use_foreign_success_for_failed_callback(session_maker) -> None:
    _, _, retry_outbox_id = await _setup_retry_outbox(session_maker)
    _, _, foreign_outbox_id = await _setup_service_outbox(
        session_maker,
        outbox_status="delivered",
        wamid="wamid.FOREIGN-SUCCESS",
        altegio_record_id=779,
    )

    async with session_maker() as session:
        async with session.begin():
            current = await session.get(OutboxMessage, retry_outbox_id)
            assert current is not None
            current.meta = {
                **(current.meta or {}),
                "delivery_retry_of_outbox_id": foreign_outbox_id,
            }
            await _apply_status_updates(
                session,
                [{"wamid": WAMID, "status": "failed", "timestamp": "1", "raw": {}}],
            )

    async with session_maker() as session:
        current = await session.get(OutboxMessage, retry_outbox_id)
        assert current is not None
        assert current.status == "failed", "foreign success must not turn this into a stale failure"
        assert "stale_failed_after_success" not in (current.meta or {})


@pytest.mark.asyncio
@pytest.mark.parametrize("callback_status", ["delivered", "read"])
async def test_success_callback_does_not_cancel_foreign_chain_from_retry_meta(
    session_maker,
    callback_status: str,
) -> None:
    _, _, retry_outbox_id = await _setup_retry_outbox(session_maker)
    foreign_record_id, _, foreign_outbox_id = await _setup_service_outbox(
        session_maker,
        wamid="wamid.FOREIGN-CANCEL",
        altegio_record_id=780,
    )

    async with session_maker() as session:
        async with session.begin():
            foreign_retry = MessageJob(
                company_id=1,
                record_id=foreign_record_id,
                client_id=1,
                job_type="record_created",
                run_at=_utcnow(),
                status="queued",
                dedupe_key=f"delivery_retry:{foreign_outbox_id}:1",
                payload={
                    "kind": "delivery_failed_retry",
                    "delivery_retry_of_outbox_id": foreign_outbox_id,
                    "delivery_retry_attempt": 1,
                },
            )
            session.add(foreign_retry)
            current = await session.get(OutboxMessage, retry_outbox_id)
            assert current is not None
            current.meta = {
                **(current.meta or {}),
                "delivery_retry_of_outbox_id": foreign_outbox_id,
            }
            await _apply_status_updates(
                session,
                [{"wamid": WAMID, "status": callback_status, "timestamp": "1", "raw": {}}],
            )

    async with session_maker() as session:
        current = await session.get(OutboxMessage, retry_outbox_id)
        assert current is not None
        assert current.status == callback_status
        foreign_retry = (
            await session.execute(
                select(MessageJob).where(MessageJob.dedupe_key == f"delivery_retry:{foreign_outbox_id}:1")
            )
        ).scalar_one()
        assert foreign_retry.status == "queued"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "meta_change,expected_reason",
    [
        ({"delivery_retry_of_outbox_id": 2**63}, "retry_outbox_audit_reference_invalid"),
        ({"delivery_retry_of_outbox_id": "not-an-id"}, "retry_outbox_audit_reference_invalid"),
        ({"delivery_retry_attempt": 2}, "retry_outbox_audit_attempt_mismatch"),
    ],
)
async def test_retry_callback_rejects_invalid_audit_meta(
    session_maker,
    meta_change: dict[str, object],
    expected_reason: str,
) -> None:
    original_id, _, retry_outbox_id = await _setup_retry_outbox(session_maker)

    async with session_maker() as session:
        async with session.begin():
            current = await session.get(OutboxMessage, retry_outbox_id)
            assert current is not None
            current.meta = {**(current.meta or {}), **meta_change}
            await _apply_status_updates(
                session,
                [{"wamid": WAMID, "status": "failed", "timestamp": "1", "raw": {}}],
            )

    async with session_maker() as session:
        current = await session.get(OutboxMessage, retry_outbox_id)
        assert current is not None
        assert current.meta["delivery_retry_chain_refusal_reason"] == expected_reason
        next_retry = (
            await session.execute(select(MessageJob).where(MessageJob.dedupe_key == f"delivery_retry:{original_id}:2"))
        ).scalar_one_or_none()
        assert next_retry is None


@pytest.mark.asyncio
async def test_retry_callback_without_job_id_is_local_and_fail_closed(session_maker) -> None:
    original_id, _, retry_outbox_id = await _setup_retry_outbox(session_maker)

    async with session_maker() as session:
        async with session.begin():
            current = await session.get(OutboxMessage, retry_outbox_id)
            assert current is not None
            current.job_id = None
            await _apply_status_updates(
                session,
                [{"wamid": WAMID, "status": "failed", "timestamp": "1", "raw": {}}],
            )

    async with session_maker() as session:
        current = await session.get(OutboxMessage, retry_outbox_id)
        assert current is not None
        assert current.status == "failed"
        assert current.meta["delivery_retry_chain_refusal_reason"] == "retry_outbox_job_id_missing"
        next_retry = (
            await session.execute(select(MessageJob).where(MessageJob.dedupe_key == f"delivery_retry:{original_id}:2"))
        ).scalar_one_or_none()
        assert next_retry is None


@pytest.mark.asyncio
async def test_retry_callback_after_current_job_was_deleted_is_fail_closed(session_maker) -> None:
    original_id, retry_job_id, retry_outbox_id = await _setup_retry_outbox(session_maker)

    async with session_maker() as session:
        async with session.begin():
            retry_job = await session.get(MessageJob, retry_job_id)
            assert retry_job is not None
            await session.delete(retry_job)

    async with session_maker() as session:
        async with session.begin():
            await _apply_status_updates(
                session,
                [{"wamid": WAMID, "status": "failed", "timestamp": "1", "raw": {}}],
            )

    async with session_maker() as session:
        current = await session.get(OutboxMessage, retry_outbox_id)
        assert current is not None
        assert current.status == "failed"
        assert current.meta["delivery_retry_chain_refusal_reason"] in {
            "retry_outbox_job_id_missing",
            "retry_outbox_job_missing",
        }
        next_retry = (
            await session.execute(select(MessageJob).where(MessageJob.dedupe_key == f"delivery_retry:{original_id}:2"))
        ).scalar_one_or_none()
        assert next_retry is None


@pytest.mark.asyncio
async def test_retry_callback_rejects_conflicting_job_reference(session_maker) -> None:
    original_id, retry_job_id, retry_outbox_id = await _setup_retry_outbox(session_maker)
    _, _, foreign_outbox_id = await _setup_service_outbox(
        session_maker,
        wamid="wamid.FOREIGN-JOB-REF",
        altegio_record_id=781,
    )

    async with session_maker() as session:
        async with session.begin():
            retry_job = await session.get(MessageJob, retry_job_id)
            assert retry_job is not None
            retry_job.payload = {
                **(retry_job.payload or {}),
                "delivery_retry_of_outbox_id": foreign_outbox_id,
            }
            await _apply_status_updates(
                session,
                [{"wamid": WAMID, "status": "failed", "timestamp": "1", "raw": {}}],
            )

    async with session_maker() as session:
        current = await session.get(OutboxMessage, retry_outbox_id)
        assert current is not None
        assert current.meta["delivery_retry_chain_refusal_reason"] == "delivery_retry_outbox_reference_mismatch"
        created = await session.execute(
            select(MessageJob).where(
                MessageJob.dedupe_key.in_([f"delivery_retry:{original_id}:2", f"delivery_retry:{foreign_outbox_id}:2"])
            )
        )
        assert list(created.scalars().all()) == []


@pytest.mark.asyncio
async def test_delivered_cancels_queued_delivery_retries(session_maker) -> None:
    _, _, outbox_id = await _setup_service_outbox(session_maker, job_type="record_created")
    async with session_maker() as session:
        async with session.begin():
            session.add(
                MessageJob(
                    company_id=1,
                    record_id=1,
                    client_id=1,
                    job_type="record_created",
                    run_at=_utcnow(),
                    status="queued",
                    attempts=0,
                    max_attempts=5,
                    dedupe_key=f"delivery_retry:{outbox_id}:1",
                    payload={"kind": "delivery_failed_retry", "delivery_retry_of_outbox_id": outbox_id},
                )
            )

    payload = _status_payload(PHONE_NUMBER_ID, WAMID, "delivered")
    async with session_maker() as session:
        async with session.begin():
            evt = WhatsAppEvent(dedupe_key="wa:delivered-cancel-retry", status="received", payload=payload)
            session.add(evt)
            await session.flush()
            await handle_event(session, evt, _NullProvider())

    async with session_maker() as session:
        retry = (
            await session.execute(select(MessageJob).where(MessageJob.dedupe_key == f"delivery_retry:{outbox_id}:1"))
        ).scalar_one()
        assert retry.status == "canceled"


@pytest.mark.asyncio
async def test_late_failed_after_delivered_does_not_downgrade_or_retry(session_maker) -> None:
    _, _, outbox_id = await _setup_service_outbox(
        session_maker,
        job_type="record_created",
        outbox_status="delivered",
    )
    payload = _failed_status_payload(PHONE_NUMBER_ID, WAMID, code=131000, title="Temporary failure")

    async with session_maker() as session:
        async with session.begin():
            evt = WhatsAppEvent(dedupe_key="wa:late-failed-after-delivered", status="received", payload=payload)
            session.add(evt)
            await session.flush()
            await handle_event(session, evt, _NullProvider())

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "delivered"
        assert (ob.meta or {})["stale_failed_after_success"] is True
        stmt = select(MessageJob).where(MessageJob.dedupe_key.like(f"delivery_retry:{outbox_id}:%"))
        assert list((await session.execute(stmt)).scalars().all()) == []


@pytest.mark.asyncio
async def test_marketing_failed_delivery_does_not_auto_retry(session_maker) -> None:
    _, _, outbox_id = await _setup_service_outbox(session_maker, job_type="review_3d")
    payload = _failed_status_payload(PHONE_NUMBER_ID, WAMID, code=131000, title="Temporary failure")

    async with session_maker() as session:
        async with session.begin():
            evt = WhatsAppEvent(dedupe_key="wa:marketing-failed-no-retry", status="received", payload=payload)
            session.add(evt)
            await session.flush()
            await handle_event(session, evt, _NullProvider())

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "failed"
        stmt = select(MessageJob).where(MessageJob.dedupe_key.like(f"delivery_retry:{outbox_id}:%"))
        assert list((await session.execute(stmt)).scalars().all()) == []


@pytest.mark.asyncio
async def test_delivery_retry_presend_guard_cancels_when_chain_already_succeeded(session_maker) -> None:
    record_id, _, outbox_id = await _setup_service_outbox(
        session_maker,
        job_type="record_created",
        outbox_status="delivered",
    )

    async with session_maker() as session:
        record = await session.get(Record, record_id)
        job = SimpleNamespace(
            job_type="record_created",
            dedupe_key=f"delivery_retry:{outbox_id}:1",
            run_at=_utcnow(),
            payload={
                "kind": "delivery_failed_retry",
                "delivery_retry_of_outbox_id": outbox_id,
                "delivery_retry_attempt": 1,
            },
        )

        reason = await ow._delivery_retry_presend_guard(session, job, record)

    assert reason == "Canceled: delivery retry chain already succeeded"


@pytest.mark.asyncio
async def test_delivery_retry_presend_guard_cancels_when_original_missing(session_maker) -> None:
    async with session_maker() as session:
        job = SimpleNamespace(
            job_type="record_created",
            dedupe_key="delivery_retry:999999:1",
            run_at=_utcnow(),
            payload={
                "kind": "delivery_failed_retry",
                "delivery_retry_of_outbox_id": 999999,
                "delivery_retry_attempt": 1,
            },
        )

        reason = await ow._delivery_retry_presend_guard(session, job, None)

    assert reason == "Retry deadline exceeded or original outbox missing for delivery retry"


@pytest.mark.asyncio
async def test_delivery_retry_presend_guard_cancels_when_deadline_passed(session_maker) -> None:
    record_id, _, outbox_id = await _setup_service_outbox(session_maker, job_type="record_created")

    async with session_maker() as session:
        async with session.begin():
            record = await session.get(Record, record_id)
            assert record is not None
            record.starts_at = _utcnow() - timedelta(days=1)
            # Identity matches the chain built by _setup_service_outbox, so the
            # guard reaches the deadline branch this test is about.
            job = SimpleNamespace(
                job_type="record_created",
                provider="altegio",
                company_id=1,
                record_id=record_id,
                client_id=1,
                dedupe_key=f"delivery_retry:{outbox_id}:1",
                run_at=_utcnow() - timedelta(days=1),
                payload={
                    "kind": "delivery_failed_retry",
                    "delivery_retry_of_outbox_id": outbox_id,
                    "delivery_retry_attempt": 1,
                },
            )

            reason = await ow._delivery_retry_presend_guard(session, job, record)

    assert reason == "Retry deadline exceeded for record_created"


@pytest.mark.asyncio
async def test_handle_event_sent_callback_noop(session_maker) -> None:
    """handle_event with a 'sent' status payload on already-sent OutboxMessage is a no-op."""
    _, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="sent")

    provider = _NullProvider()
    payload = _status_payload(PHONE_NUMBER_ID, WAMID, "sent")

    with patch(
        "altegio_bot.workers.whatsapp_inbox_worker.recompute_campaign_run_stats",
        new_callable=AsyncMock,
        return_value={},
    ):
        async with session_maker() as session:
            async with session.begin():
                evt = WhatsAppEvent(
                    dedupe_key="wa:status-sent-noop-1",
                    status="received",
                    query={},
                    headers={},
                    payload=payload,
                )
                session.add(evt)
                await session.flush()
                await handle_event(session, evt, provider)

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "sent"  # unchanged


# ---------------------------------------------------------------------------
# Regression: inbound message flow still works
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_inbound_message_still_forwarded_to_chatwoot(
    session_maker,
) -> None:
    """Inbound (non-status) events must still be forwarded to Chatwoot."""
    provider = _NullProvider()

    payload = _message_payload(PHONE_NUMBER_ID, "4915100000001", "Hello")

    logged: list[str] = []

    class _FakeCW:
        async def get_or_create_incoming_conversation(
            self,
            phone: str,
            contact_name: str | None = None,
        ) -> int:
            return 20

        async def send_message(
            self,
            conversation_id: int,
            content: str,
            *,
            message_type: str = "outgoing",
            private: bool = False,
            content_attributes: dict | None = None,
        ) -> int:
            logged.append(content)
            return 200

        async def aclose(self) -> None:
            pass

    with patch(
        "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
        return_value=_FakeCW(),
    ):
        async with session_maker() as session:
            async with session.begin():
                evt = WhatsAppEvent(
                    dedupe_key="wa:inbound-1",
                    status="received",
                    query={},
                    headers={},
                    payload=payload,
                )
                session.add(evt)
                await session.flush()
                await handle_event(session, evt, provider)

    assert "Hello" in logged


# ---------------------------------------------------------------------------
# Regression: STOP/START flow still works
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_command_still_sets_opt_out(session_maker) -> None:
    """STOP command must still set wa_opted_out after our changes."""
    from altegio_bot.models.models import Client

    provider = _NullProvider()
    phone = "+10000000010"

    async with session_maker() as session:
        async with session.begin():
            c = await session.get(Client, 10)
            assert c is not None
            c.phone_e164 = phone
            c.wa_opted_out = False

            session.add(
                WhatsAppSender(
                    id=99,
                    company_id=1,
                    sender_code="default",
                    phone_number_id=PHONE_NUMBER_ID,
                    display_phone="+49",
                    is_active=True,
                )
            )

            payload = _message_payload(PHONE_NUMBER_ID, "10000000010", "STOP")
            evt = WhatsAppEvent(
                dedupe_key="wa:stop-1",
                status="received",
                query={},
                headers={},
                payload=payload,
            )
            session.add(evt)
            await session.flush()

            await handle_event(session, evt, provider)

    async with session_maker() as session:
        c2 = await session.get(Client, 10)
        assert c2 is not None
        assert c2.wa_opted_out is True
