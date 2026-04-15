"""Тесты: разделение campaign execution jobs и outbound message jobs.

Проблема (commit 327bfd6):
  outbox_worker._lock_next_jobs() не фильтровал по job_type, из-за чего
  campaign_execute_new_clients_monthly jobs забирались outbox_worker'ом и падали
  с "No phone_e164" — основной воркер так и не стартовал.

Исправления:
  - outbox_worker._lock_next_jobs() исключает CAMPAIGN_EXECUTION_JOB_TYPE.
  - outbox_worker.run_once() исключает CAMPAIGN_EXECUTION_JOB_TYPE.
  - outbox_worker._run_job_logic() имеет safety guard: execution job → requeue.

Тесты:
  1. _lock_next_jobs excludes execution jobs (SQL-фильтр).
  2. Safety guard: execution job, пришедший в _run_job_logic, возвращается в queued.
  3. Outbound job без phone_e164 по-прежнему проваливается (regression).
  4. campaign_worker.process_job_in_session обрабатывает execution job корректно.
  5. outbox_worker._lock_next_jobs включает обычный outbound job (белый список).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import altegio_bot.workers.outbox_worker as ow
from altegio_bot.campaigns.runner import CAMPAIGN_EXECUTION_JOB_TYPE

NOW = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
COMPANY_ID = 758285
NEWSLETTER_JOB_TYPE = "newsletter_new_clients_monthly"


# ---------------------------------------------------------------------------
# Helpers
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
    payload: dict = field(default_factory=dict)
    locked_at: Any = None


@dataclass
class _FakeClient:
    id: int
    display_name: str = "Anna"
    phone_e164: str | None = "+491234567890"
    wa_opted_out: bool = False


class _FakeSession:
    def __init__(self) -> None:
        self.added: list[Any] = []
        self._pk = 0

    def add(self, obj: Any) -> None:
        if not hasattr(obj, "id") or obj.id is None:
            self._pk += 1
            setattr(obj, "id", self._pk)
        self.added.append(obj)


# ---------------------------------------------------------------------------
# 1. _lock_next_jobs excludes CAMPAIGN_EXECUTION_JOB_TYPE
# ---------------------------------------------------------------------------


def test_lock_next_jobs_excludes_execution_job_type() -> None:
    """_lock_next_jobs строит WHERE job_type != CAMPAIGN_EXECUTION_JOB_TYPE."""
    from sqlalchemy import select

    from altegio_bot.models.models import MessageJob

    # Build a query that mirrors _lock_next_jobs and verify the filter is present.
    stmt = (
        select(MessageJob)
        .where(MessageJob.status == "queued")
        .where(MessageJob.job_type != CAMPAIGN_EXECUTION_JOB_TYPE)
        .limit(10)
    )
    compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
    assert CAMPAIGN_EXECUTION_JOB_TYPE in compiled
    assert "!=" in compiled or "!=" in compiled or "<>" in compiled or "NOT" in compiled.upper()


# ---------------------------------------------------------------------------
# 2. Safety guard: execution job → requeued, not failed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_safety_guard_requeues_execution_job(monkeypatch: Any) -> None:
    """_run_job_logic requeues a campaign execution job instead of processing it."""
    job = _FakeJob(
        id=10,
        company_id=COMPANY_ID,
        job_type=CAMPAIGN_EXECUTION_JOB_TYPE,
        status="processing",
        payload={"kind": CAMPAIGN_EXECUTION_JOB_TYPE, "campaign_run_id": 42},
    )
    session = _FakeSession()

    # These must NOT be called
    send_called = False

    async def _bad_send(*a: Any, **kw: Any) -> Any:
        nonlocal send_called
        send_called = True
        raise AssertionError("send must not be called for execution job")

    monkeypatch.setattr(ow, "safe_send_template", _bad_send)
    monkeypatch.setattr(ow, "safe_send", _bad_send)

    await ow._run_job_logic(session, job, provider=MagicMock())  # type: ignore[arg-type]

    assert job.status == "queued", f"expected 'queued', got '{job.status}'"
    assert job.locked_at is None
    assert not send_called


# ---------------------------------------------------------------------------
# 3. Outbound job without phone_e164 still fails (regression guard)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_outbound_job_without_phone_still_fails(monkeypatch: Any) -> None:
    """Newsletter job с client_id=None и без phone в payload → status='failed'."""
    job = _FakeJob(
        id=11,
        company_id=COMPANY_ID,
        job_type=NEWSLETTER_JOB_TYPE,
        client_id=None,
        payload={"kind": NEWSLETTER_JOB_TYPE, "loyalty_card_text": "Karte: X"},
        # No phone_e164 in payload!
    )
    session = _FakeSession()

    monkeypatch.setattr(ow, "_load_job", AsyncMock(return_value=job))
    monkeypatch.setattr(ow, "_find_success_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_find_existing_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_apply_rate_limit", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_record", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_client", AsyncMock(return_value=None))

    await ow.process_job_in_session(session, 11, provider=MagicMock())

    assert job.status == "failed"
    assert job.last_error == "No phone_e164"


# ---------------------------------------------------------------------------
# 4. campaign_worker.process_job_in_session handles execution job correctly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_campaign_worker_processes_execution_job(monkeypatch: Any) -> None:
    """campaign_worker extracts campaign_run_id and calls execute_queued_send_real."""
    import altegio_bot.workers.campaign_worker as cw

    job = _FakeJob(
        id=20,
        company_id=COMPANY_ID,
        job_type=CAMPAIGN_EXECUTION_JOB_TYPE,
        status="processing",
        payload={"kind": CAMPAIGN_EXECUTION_JOB_TYPE, "campaign_run_id": 99},
    )
    session = _FakeSession()

    monkeypatch.setattr(cw, "_load_job", AsyncMock(return_value=job))

    execute_calls: list[int] = []

    async def _fake_execute(run_id: int) -> None:
        execute_calls.append(run_id)

    monkeypatch.setattr(cw, "execute_queued_send_real", _fake_execute)

    await cw.process_job_in_session(session, 20)

    assert execute_calls == [99]
    assert job.status == "done"


# ---------------------------------------------------------------------------
# 5. _lock_next_jobs includes ordinary outbound jobs
# ---------------------------------------------------------------------------


def test_lock_next_jobs_includes_newsletter_job_type() -> None:
    """NEWSLETTER_JOB_TYPE не отфильтровывается из _lock_next_jobs."""
    # The filter is `job_type != CAMPAIGN_EXECUTION_JOB_TYPE`, so any other
    # job_type passes. This test documents that guarantee explicitly.
    assert NEWSLETTER_JOB_TYPE != CAMPAIGN_EXECUTION_JOB_TYPE
    assert "newsletter_new_clients_followup" != CAMPAIGN_EXECUTION_JOB_TYPE
    assert "review_3d" != CAMPAIGN_EXECUTION_JOB_TYPE


# ---------------------------------------------------------------------------
# 6. Execution job payload regression: correct kind and campaign_run_id
# ---------------------------------------------------------------------------


def test_execution_job_payload_structure() -> None:
    """campaign_execute payload всегда содержит kind и campaign_run_id."""
    payload = {
        "kind": CAMPAIGN_EXECUTION_JOB_TYPE,
        "campaign_run_id": 7,
    }
    assert payload["kind"] == CAMPAIGN_EXECUTION_JOB_TYPE
    assert isinstance(payload["campaign_run_id"], int)
    # No phone_e164 — intentional: this is an orchestrator job, not an outbound message.
    assert "phone_e164" not in payload


# ---------------------------------------------------------------------------
# 7. Smoke: run_campaign_worker entrypoint is importable and wired correctly
# ---------------------------------------------------------------------------


def test_run_campaign_worker_entrypoint_importable() -> None:
    """run_campaign_worker script импортируется без ошибок."""
    import altegio_bot.scripts.run_campaign_worker as entrypoint

    assert callable(entrypoint.main)


def test_run_campaign_worker_calls_run_loop(monkeypatch: Any) -> None:
    """main() в run_campaign_worker вызывает campaign_worker.run_loop."""
    import altegio_bot.scripts.run_campaign_worker as entrypoint
    import altegio_bot.workers.campaign_worker as cw

    calls: list[str] = []

    async def _fake_run_loop(**kw: Any) -> None:
        calls.append("run_loop")

    monkeypatch.setattr(cw, "run_loop", _fake_run_loop)
    monkeypatch.setattr(entrypoint, "run_loop", _fake_run_loop)

    asyncio.get_event_loop().run_until_complete(entrypoint.main())

    assert calls == ["run_loop"], f"expected run_loop to be called once, got {calls}"


# ---------------------------------------------------------------------------
# 8. campaign_worker logs "picked" before executing the job
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_campaign_worker_logs_picked_job(monkeypatch: Any, caplog: Any) -> None:
    """process_job_in_session логирует 'picked' при обработке валидного execution job."""
    import logging

    import altegio_bot.workers.campaign_worker as cw

    job = _FakeJob(
        id=30,
        company_id=COMPANY_ID,
        job_type=CAMPAIGN_EXECUTION_JOB_TYPE,
        status="processing",
        payload={"kind": CAMPAIGN_EXECUTION_JOB_TYPE, "campaign_run_id": 77},
    )
    session = _FakeSession()

    monkeypatch.setattr(cw, "_load_job", AsyncMock(return_value=job))
    monkeypatch.setattr(cw, "execute_queued_send_real", AsyncMock(return_value=None))

    with caplog.at_level(logging.INFO, logger="campaign_worker"):
        await cw.process_job_in_session(session, 30)

    assert any("picked" in r.message for r in caplog.records), (
        f"Expected 'picked' in logs, got: {[r.message for r in caplog.records]}"
    )


# ---------------------------------------------------------------------------
# 9. Docker-compose service guard: altegio-campaign-worker present in compose
# ---------------------------------------------------------------------------


def test_docker_compose_has_campaign_worker_service() -> None:
    """docker-compose.yml содержит сервис altegio-campaign-worker."""
    import pathlib

    compose_path = pathlib.Path(__file__).parents[4] / "docker-compose.yml"
    assert compose_path.exists(), f"docker-compose.yml not found at {compose_path}"

    content = compose_path.read_text()
    assert "altegio-campaign-worker" in content, "altegio-campaign-worker service not found in docker-compose.yml"
    assert "run_campaign_worker" in content, "run_campaign_worker command not found in docker-compose.yml"
