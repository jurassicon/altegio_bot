from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock

import pytest

from altegio_bot.workers import outbox_worker as ow


def run(coro: Any) -> Any:
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
    locked_at: datetime | None = None
    payload: dict = field(default_factory=dict)


@dataclass
class FakeClient:
    id: int
    phone_e164: str | None = "+491234567890"
    wa_opted_out: bool = True
    altegio_client_id: int = 99001


class FakeSession:
    def __init__(self) -> None:
        self.added: list[Any] = []

    def add(self, obj: Any) -> None:
        self.added.append(obj)

    async def get(self, model: Any, pk: Any) -> Any:
        from types import SimpleNamespace

        name = getattr(model, "__name__", "")
        if name == "CampaignRecipient":
            return SimpleNamespace(
                status="delivered",
                read_at=None,
                booked_after_at=None,
                client_id=None,
                phone_e164=None,
                altegio_client_id=99001,
                company_id=0,
                sent_at=None,
                outbox_message_id=None,
                provider_message_id=None,
            )
        if name == "CampaignRun":
            from datetime import datetime, timezone

            return SimpleNamespace(completed_at=datetime(2025, 1, 1, tzinfo=timezone.utc))
        return None

    async def execute(self, stmt: Any) -> Any:
        from types import SimpleNamespace

        return SimpleNamespace(
            scalar_one_or_none=lambda: None,
            scalars=lambda: SimpleNamespace(first=lambda: None, all=lambda: []),
        )


@pytest.mark.parametrize("job_type", ow.MARKETING_JOB_TYPES)
def test_outbox_skips_marketing_when_opted_out(monkeypatch: Any, job_type: str) -> None:
    fixed_now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)

    job = FakeJob(
        id=1,
        company_id=758285,
        job_type=job_type,
        status="queued",
        run_at=fixed_now,
        record_id=None,
        client_id=1,
        payload=(
            {"campaign_recipient_id": 99999, "campaign_run_id": 88888}
            if job_type == "newsletter_new_clients_followup"
            else {}
        ),
    )

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_find_success(session: Any, job_id: int) -> Any:
        return None

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return None

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1)

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    monkeypatch.setattr(ow, "_find_success_outbox", fake_find_success)
    monkeypatch.setattr(ow, "_load_record", fake_load_record)
    monkeypatch.setattr(ow, "_load_client", fake_load_client)
    monkeypatch.setattr(ow, "client_has_any_future_record", AsyncMock(return_value=False))
    safe_send = AsyncMock()
    safe_send_template = AsyncMock()
    monkeypatch.setattr(ow, "safe_send", safe_send)
    monkeypatch.setattr(ow, "safe_send_template", safe_send_template)

    session = FakeSession()
    run(ow.process_job_in_session(session, 1, provider=object()))

    assert job.status == "canceled"
    assert job.locked_at is None
    assert job.last_error == "Skipped: client unsubscribed"
    safe_send.assert_not_called()
    safe_send_template.assert_not_called()
    assert session.added == []
