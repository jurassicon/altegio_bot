"""Tests for wa_err_code=131026 temporary auto-suppression.

Production pattern being tested:
- outbox_messages.status = 'sent'  (Meta accepted the API call)
- whatsapp_events webhook: statuses[0].status='failed', code=131026
- The webhook worker does NOT downgrade 'sent' to 'failed' (rank 0 < 3).
- _count_131026_failures must detect this via whatsapp_events alone.

Covers:
1.  phone with no 131026 failures -> not suppressed
2.  om.status='sent' + webhook failed+131026 -> counted (real prod case)
3.  repeated 131026 within window -> suppressed
4.  other error codes -> not suppressed
5.  old 131026 failures outside window -> not suppressed
6.  outbox row without matching webhook -> not suppressed
7.  transactional job types are not suppressed before send
8.  marketing job types are still suppressed before send
9.  provider.send is NOT called for suppressed marketing sends
10. outbox row is created with the suppression reason
11. not suppressed when below threshold (1 < 2)
12. ops data model: status=canceled, error starts with suppressed_131026
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock

import pytest

from altegio_bot.models.models import OutboxMessage, WhatsAppEvent
from altegio_bot.workers import outbox_worker as ow

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

UTC = timezone.utc
NOW = datetime(2026, 4, 24, 12, 0, tzinfo=UTC)
PHONE = "+491786916228"
WAMID_BASE = "wamid.test131026"


def _wa_failed_payload(wamid: str, code: int = 131026) -> dict:
    """Minimal Meta webhook payload for a delivery failure."""
    return {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "statuses": [
                                {
                                    "id": wamid,
                                    "status": "failed",
                                    "errors": [{"code": code, "title": "unreachable"}],
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


async def _insert_outbox(
    session: Any,
    *,
    wamid: str,
    phone: str = PHONE,
    sent_at: datetime = NOW,
    message_source: str = "bot",
    status: str = "sent",
) -> None:
    """Insert an outbox_messages row.

    Default status='sent' matches the real production 131026 pattern:
    Meta accepted the API call, but the delivery webhook later reports
    a 131026 failure.  The webhook worker does not downgrade 'sent' to
    'failed' because 'failed' rank (0) < 'sent' rank (3).
    """
    session.add(
        OutboxMessage(
            company_id=1,
            phone_e164=phone,
            template_code="reminder_24h",
            language="de",
            body="",
            status=status,
            error=None,
            provider_message_id=wamid,
            scheduled_at=sent_at,
            sent_at=sent_at,
            message_source=message_source,
            meta={},
        )
    )
    await session.flush()


async def _insert_wa_event(
    session: Any,
    *,
    wamid: str,
    code: int = 131026,
    dedupe_key: str | None = None,
) -> None:
    """Insert a whatsapp_events row with the given error code."""
    key = dedupe_key or f"test-{wamid}-{code}"
    session.add(
        WhatsAppEvent(
            dedupe_key=key,
            status="processed",
            payload=_wa_failed_payload(wamid, code),
        )
    )
    await session.flush()


# ---------------------------------------------------------------------------
# DB-level helper tests (require real DB via session_maker fixture)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_failures_returns_zero(session_maker: Any) -> None:
    """No outbox/webhook rows -> count = 0."""
    async with session_maker() as session:
        async with session.begin():
            count = await ow._count_131026_failures(session, PHONE, 14)
            assert count == 0


@pytest.mark.asyncio
async def test_sent_outbox_with_failed_webhook_counted(
    session_maker: Any,
) -> None:
    """Real production case: om.status='sent' + webhook failed+131026.

    The outbox row is NOT failed — Meta accepted the API call.
    The delivery failure only lives in whatsapp_events.
    _count_131026_failures must count this without checking om.status.
    """
    async with session_maker() as session:
        async with session.begin():
            wamid = f"{WAMID_BASE}-prod"
            await _insert_outbox(session, wamid=wamid, status="sent")
            await _insert_wa_event(session, wamid=wamid)

            count = await ow._count_131026_failures(session, PHONE, 14)
            assert count == 1


@pytest.mark.asyncio
async def test_repeated_131026_within_window_counted(
    session_maker: Any,
) -> None:
    """2 linked 131026 webhook events within window -> count >= 2."""
    async with session_maker() as session:
        async with session.begin():
            for i in range(2):
                wamid = f"{WAMID_BASE}-{i}"
                await _insert_outbox(session, wamid=wamid, status="sent")
                await _insert_wa_event(session, wamid=wamid)

            count = await ow._count_131026_failures(session, PHONE, 14)
            assert count >= 2


@pytest.mark.asyncio
async def test_other_error_code_not_counted(
    session_maker: Any,
) -> None:
    """Code 131047 does NOT count toward 131026 suppression."""
    async with session_maker() as session:
        async with session.begin():
            wamid = f"{WAMID_BASE}-other"
            await _insert_outbox(session, wamid=wamid, status="sent")
            await _insert_wa_event(session, wamid=wamid, code=131047)

            count = await ow._count_131026_failures(session, PHONE, 14)
            assert count == 0


@pytest.mark.asyncio
async def test_old_failures_outside_window_not_counted(
    session_maker: Any,
) -> None:
    """131026 failures older than window_days are NOT counted."""
    old_sent_at = NOW - timedelta(days=20)
    async with session_maker() as session:
        async with session.begin():
            for i in range(3):
                wamid = f"{WAMID_BASE}-old-{i}"
                await _insert_outbox(session, wamid=wamid, status="sent", sent_at=old_sent_at)
                await _insert_wa_event(session, wamid=wamid)

            count = await ow._count_131026_failures(session, PHONE, 14)
            assert count == 0


@pytest.mark.asyncio
async def test_outbox_without_webhook_not_counted(
    session_maker: Any,
) -> None:
    """Outbox row with no matching whatsapp_events entry -> not counted.

    Just having an outbox row for this phone is not enough;
    a 131026 webhook event must exist.
    """
    async with session_maker() as session:
        async with session.begin():
            wamid = f"{WAMID_BASE}-no-event"
            await _insert_outbox(session, wamid=wamid, status="sent")
            # Intentionally NOT inserting a whatsapp_events row.

            count = await ow._count_131026_failures(session, PHONE, 14)
            assert count == 0


# ---------------------------------------------------------------------------
# Worker behaviour tests (FakeSession + monkeypatching)
# ---------------------------------------------------------------------------


@dataclass
class _FakeJob:
    id: int
    company_id: int
    job_type: str
    status: str = "queued"
    run_at: datetime = field(default_factory=lambda: NOW)
    record_id: int | None = None
    client_id: int | None = None
    last_error: str | None = None
    attempts: int = 0
    max_attempts: int = 5
    locked_at: datetime | None = None
    payload: dict = field(default_factory=dict)


@dataclass
class _FakeClient:
    id: int
    phone_e164: str = PHONE
    wa_opted_out: bool = False
    display_name: str = "Anna"
    altegio_client_id: int | None = 9001


@dataclass
class _FakeRecord:
    id: int = 10
    client_id: int | None = 1
    starts_at: datetime | None = field(default_factory=lambda: NOW - timedelta(days=4))
    is_deleted: bool = False
    altegio_client_id: int | None = 9001


class _FakeScalarResult:
    def scalar_one_or_none(self) -> None:
        return None


class _FakeSession:
    def __init__(self) -> None:
        self.added: list[Any] = []

    def add(self, obj: Any) -> None:
        self.added.append(obj)

    async def execute(self, stmt: Any) -> _FakeScalarResult:
        return _FakeScalarResult()


def _base_patches(
    monkeypatch: Any,
    *,
    job: _FakeJob,
    n_failures: int = 2,
    record: Any = None,
    client: Any = None,
) -> AsyncMock:
    """Apply common outbox_worker monkeypatches."""
    _j = job
    _client = client or _FakeClient(id=1)

    async def _fake_load_job(session: Any, job_id: int) -> _FakeJob:
        return _j

    monkeypatch.setattr(ow, "_load_job", _fake_load_job)
    monkeypatch.setattr(ow, "_find_success_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_find_existing_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_record", AsyncMock(return_value=record))
    monkeypatch.setattr(ow, "_load_client", AsyncMock(return_value=_client))
    count_mock = AsyncMock(return_value=n_failures)
    monkeypatch.setattr(ow, "_count_131026_failures", count_mock)
    monkeypatch.setattr(ow, "utcnow", lambda: NOW)
    return count_mock


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def _patch_text_send_path(monkeypatch: Any, *, message_id: str = "wamid.sent") -> AsyncMock:
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "text")
    monkeypatch.setattr(ow, "_apply_rate_limit", AsyncMock(return_value=None))
    monkeypatch.setattr(
        ow,
        "_render_message",
        AsyncMock(
            return_value=(
                "Hallo {client_name}",
                123,
                "de",
                {"client_name": "Anna"},
            )
        ),
    )
    send_mock = AsyncMock(return_value=(message_id, None))
    monkeypatch.setattr(ow, "safe_send", send_mock)
    monkeypatch.setattr(
        ow,
        "safe_send_template",
        AsyncMock(
            side_effect=AssertionError(
                "safe_send_template не должен вызываться в text mode",
            )
        ),
    )
    return send_mock


def _patch_marketing_guards(monkeypatch: Any) -> None:
    monkeypatch.setattr(ow, "count_attended_client_visits", AsyncMock(return_value=1))
    monkeypatch.setattr(ow, "client_has_future_appointments", AsyncMock(return_value=False))
    monkeypatch.setattr(ow, "_client_returned_since", AsyncMock(return_value=False))


def _record_for_job_type(job_type: str) -> _FakeRecord | None:
    if job_type != "comeback_3d":
        return None

    return _FakeRecord(is_deleted=True)


@pytest.mark.parametrize(
    "job_type",
    [
        "review_3d",
        "repeat_10d",
        "comeback_3d",
        "newsletter_new_clients_monthly",
        "newsletter_new_clients_followup",
    ],
)
def test_131026_suppression_allowed_for_marketing_job_types(job_type: str) -> None:
    assert ow._job_type_allows_131026_suppression(job_type) is True


@pytest.mark.parametrize(
    "job_type",
    [
        "record_created",
        "record_updated",
        "record_canceled",
        "reminder_24h",
        "reminder_2h",
        "operator_relay",
        "unknown_future_transactional",
    ],
)
def test_131026_suppression_not_allowed_for_transactional_job_types(job_type: str) -> None:
    assert ow._job_type_allows_131026_suppression(job_type) is False


@pytest.mark.parametrize(
    "job_type",
    [
        "record_created",
        "record_updated",
        "record_canceled",
        "reminder_24h",
        "reminder_2h",
        "operator_relay",
        "unknown_future_transactional",
    ],
)
def test_transactional_job_not_suppressed_when_131026_threshold_reached(
    monkeypatch: Any,
    job_type: str,
) -> None:
    """Transactional job доходит до provider send даже при repeated 131026."""
    job = _FakeJob(id=2, company_id=758285, job_type=job_type)
    count_mock = _base_patches(monkeypatch, job=job, n_failures=3)
    session = _FakeSession()
    send_mock = _patch_text_send_path(monkeypatch)

    _run(ow.process_job_in_session(session, 2, provider=object()))

    send_mock.assert_awaited_once()
    count_mock.assert_not_awaited()
    assert job.status == "done"
    assert job.last_error is None
    assert all(not (getattr(out, "error", "") or "").startswith("suppressed_131026") for out in session.added)


@pytest.mark.parametrize(
    "job_type",
    [
        "review_3d",
        "repeat_10d",
        "comeback_3d",
        "newsletter_new_clients_monthly",
        "newsletter_new_clients_followup",
    ],
)
def test_marketing_job_suppressed_when_131026_threshold_reached(
    monkeypatch: Any,
    job_type: str,
) -> None:
    """Маркетинговый job сохраняет pre-send suppression при repeated 131026."""
    job = _FakeJob(id=1, company_id=758285, job_type=job_type)
    record = _record_for_job_type(job_type)
    count_mock = _base_patches(monkeypatch, job=job, n_failures=2, record=record)
    _patch_marketing_guards(monkeypatch)
    session = _FakeSession()
    send_mock = AsyncMock()

    monkeypatch.setattr(ow, "safe_send", send_mock)
    monkeypatch.setattr(ow, "safe_send_template", send_mock)

    _run(ow.process_job_in_session(session, 1, provider=object()))

    send_mock.assert_not_called()
    count_mock.assert_awaited_once()
    assert job.status == "canceled"
    assert job.last_error is not None
    assert job.last_error.startswith("suppressed_131026")


def test_suppressed_outbox_row_created(monkeypatch: Any) -> None:
    """A canceled OutboxMessage row is persisted for audit when suppressed."""
    job = _FakeJob(id=3, company_id=758285, job_type="newsletter_new_clients_monthly")
    _base_patches(monkeypatch, job=job, n_failures=2)
    session = _FakeSession()

    monkeypatch.setattr(ow, "safe_send", AsyncMock(return_value=("x", None)))
    monkeypatch.setattr(ow, "safe_send_template", AsyncMock(return_value=("x", None)))

    _run(ow.process_job_in_session(session, 3, provider=object()))

    assert len(session.added) == 1
    out = session.added[0]
    assert out.status == "canceled"
    assert out.error is not None
    assert out.error.startswith("suppressed_131026")
    assert out.phone_e164 == PHONE
    assert out.job_id == 3
    assert "suppression_code" in out.meta
    assert out.meta["suppression_code"] == "131026"
    assert "matched_failures" in out.meta


def test_not_suppressed_when_below_threshold(monkeypatch: Any) -> None:
    """1 failure below default threshold of 2 -> send proceeds."""
    job = _FakeJob(id=4, company_id=758285, job_type="newsletter_new_clients_monthly")
    _base_patches(monkeypatch, job=job, n_failures=1)
    send_mock = _patch_text_send_path(monkeypatch)
    session = _FakeSession()

    _run(ow.process_job_in_session(session, 4, provider=object()))

    send_mock.assert_awaited_once()
    assert job.status == "done"
    assert "suppressed_131026" not in (job.last_error or "")


def test_ops_suppressed_row_has_expected_fields(monkeypatch: Any) -> None:
    """Suppressed row has status=canceled and error starting with suppressed_131026.

    Verifies the data contract that ops/router.py relies on:
    - _error_cell() checks error.startswith('suppressed_131026') for badge
    - row_class is set to 'suppressed' for CSS highlight
    """
    job = _FakeJob(id=5, company_id=758285, job_type="newsletter_new_clients_followup")
    _base_patches(monkeypatch, job=job, n_failures=2)
    session = _FakeSession()
    monkeypatch.setattr(ow, "safe_send", AsyncMock(return_value=("x", None)))
    monkeypatch.setattr(ow, "safe_send_template", AsyncMock(return_value=("x", None)))

    _run(ow.process_job_in_session(session, 5, provider=object()))

    assert len(session.added) == 1
    out = session.added[0]
    assert out.status == "canceled"
    assert (out.error or "").startswith("suppressed_131026")
    assert "in" in (out.error or "")  # '(N in Xd)' part is present


def test_operator_relay_not_in_outbox_worker_path(monkeypatch: Any) -> None:
    """operator_relay goes through whatsapp_inbox_worker, not outbox_worker.

    _count_131026_failures must NOT be reached when a job returns early
    (e.g. max_attempts exhausted) before the suppression gate.
    """
    call_count: list[int] = [0]

    async def _counting_131026(session: Any, phone: str, wd: int) -> int:
        call_count[0] += 1
        return 0

    monkeypatch.setattr(ow, "_count_131026_failures", _counting_131026)

    exhausted_job = _FakeJob(
        id=10,
        company_id=758285,
        job_type="reminder_24h",
        attempts=5,
        max_attempts=5,
    )

    async def _fake_load_job(session: Any, job_id: int) -> _FakeJob:
        return exhausted_job

    monkeypatch.setattr(ow, "_load_job", _fake_load_job)
    monkeypatch.setattr(ow, "_find_success_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_find_existing_outbox", AsyncMock(return_value=None))

    _run(ow.process_job_in_session(_FakeSession(), 10, provider=object()))

    assert call_count[0] == 0, "_count_131026_failures must not be called when job returns early"
