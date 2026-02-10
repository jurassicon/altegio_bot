from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import altegio_bot.message_planner as mp


@dataclass
class FakeRecord:
    id: int
    company_id: int
    starts_at: datetime | None


@dataclass
class FakeClient:
    id: int


class DummySession:
    pass


def run(coro: Any) -> Any:
    return asyncio.run(coro)


def test_create_schedules_record_created_and_24h_only(
        monkeypatch: Any,
) -> None:
    fixed_now = datetime(2026, 2, 6, 22, 0, tzinfo=timezone.utc)
    record = FakeRecord(
        id=2,
        company_id=758285,
        starts_at=fixed_now + timedelta(days=3),
    )
    client = FakeClient(id=1)

    calls: list[tuple[str, datetime]] = []

    async def fake_enqueue_job(
            session: Any,
            *,
            company_id: int,
            record_id: int,
            client_id: int | None,
            job_type: str,
            run_at: datetime,
    ) -> None:
        calls.append((job_type, run_at))

    async def fake_cancel_queued_jobs(
            session: Any,
            record_id: int,
            job_types: Any,
    ) -> None:
        raise AssertionError(
            "cancel_queued_jobs should not be called on create"
        )

    monkeypatch.setattr(mp, "utcnow", lambda: fixed_now)
    monkeypatch.setattr(mp, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(mp, "cancel_queued_jobs", fake_cancel_queued_jobs)

    run(
        mp.plan_jobs_for_record_event(
            session=DummySession(),
            record=record,  # type: ignore[arg-type]
            client=client,  # type: ignore[arg-type]
            event_status="create",
        )
    )

    job_types = [t for t, _ in calls]
    assert "record_created" in job_types
    assert "reminder_24h" in job_types
    assert "reminder_2h" not in job_types


def test_create_schedules_2h_only_when_less_or_equal_24h(
        monkeypatch: Any,
) -> None:
    fixed_now = datetime(2026, 2, 6, 22, 0, tzinfo=timezone.utc)
    record = FakeRecord(
        id=2,
        company_id=758285,
        starts_at=fixed_now + timedelta(hours=10),
    )

    calls: list[str] = []

    async def fake_enqueue_job(*args: Any, **kwargs: Any) -> None:
        calls.append(kwargs["job_type"])

    monkeypatch.setattr(mp, "utcnow", lambda: fixed_now)
    monkeypatch.setattr(mp, "enqueue_job", fake_enqueue_job)

    run(
        mp.plan_jobs_for_record_event(
            session=DummySession(),
            record=record,  # type: ignore[arg-type]
            client=None,
            event_status="create",
        )
    )

    assert "reminder_2h" in calls
    assert "reminder_24h" not in calls


def test_create_no_reminders_when_less_or_equal_2h(monkeypatch: Any) -> None:
    fixed_now = datetime(2026, 2, 6, 22, 0, tzinfo=timezone.utc)
    record = FakeRecord(
        id=2,
        company_id=758285,
        starts_at=fixed_now + timedelta(hours=2),
    )

    calls: list[str] = []

    async def fake_enqueue_job(*args: Any, **kwargs: Any) -> None:
        calls.append(kwargs["job_type"])

    monkeypatch.setattr(mp, "utcnow", lambda: fixed_now)
    monkeypatch.setattr(mp, "enqueue_job", fake_enqueue_job)

    run(
        mp.plan_jobs_for_record_event(
            session=DummySession(),
            record=record,  # type: ignore[arg-type]
            client=None,
            event_status="create",
        )
    )

    assert "reminder_24h" not in calls
    assert "reminder_2h" not in calls


def test_update_debounced_record_updated(monkeypatch: Any) -> None:
    fixed_now = datetime(2026, 2, 6, 22, 0, tzinfo=timezone.utc)
    record = FakeRecord(
        id=2,
        company_id=758285,
        starts_at=fixed_now + timedelta(days=2),
    )

    calls: list[tuple[str, datetime]] = []
    canceled: list[tuple[int, tuple[str, ...]]] = []

    async def fake_enqueue_job(
            session: Any,
            *,
            company_id: int,
            record_id: int,
            client_id: int | None,
            job_type: str,
            run_at: datetime,
    ) -> None:
        calls.append((job_type, run_at))

    async def fake_cancel_queued_jobs(
            session: Any,
            record_id: int,
            job_types: Any,
    ) -> None:
        canceled.append((record_id, tuple(job_types)))

    monkeypatch.setattr(mp, "utcnow", lambda: fixed_now)
    monkeypatch.setattr(mp, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(mp, "cancel_queued_jobs", fake_cancel_queued_jobs)

    run(
        mp.plan_jobs_for_record_event(
            session=DummySession(),
            record=record,  # type: ignore[arg-type]
            client=None,
            event_status="update",
        )
    )

    assert canceled

    upd = [c for c in calls if c[0] == "record_updated"]
    assert len(upd) == 1

    expected = fixed_now + timedelta(seconds=mp.UPDATE_DEBOUNCE_SEC)
    assert upd[0][1] == expected


def test_delete_schedules_canceled_and_comeback(monkeypatch: Any) -> None:
    fixed_now = datetime(2026, 2, 6, 22, 0, tzinfo=timezone.utc)
    record = FakeRecord(
        id=2,
        company_id=758285,
        starts_at=fixed_now + timedelta(days=2),
    )

    calls: list[str] = []

    async def fake_enqueue_job(*args: Any, **kwargs: Any) -> None:
        calls.append(kwargs["job_type"])

    async def fake_cancel_queued_jobs(
            session: Any,
            record_id: int,
            job_types: Any,
    ) -> None:
        return None

    monkeypatch.setattr(mp, "utcnow", lambda: fixed_now)
    monkeypatch.setattr(mp, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(mp, "cancel_queued_jobs", fake_cancel_queued_jobs)

    run(
        mp.plan_jobs_for_record_event(
            session=DummySession(),
            record=record,  # type: ignore[arg-type]
            client=None,
            event_status="delete",
        )
    )

    assert "record_canceled" in calls
    assert "comeback_3d" in calls


def test_record_updated_dedupe_bucket() -> None:
    fixed_now = datetime(2026, 2, 6, 22, 0, tzinfo=timezone.utc)

    k1 = mp._dedupe_key(
        "record_updated",
        2,
        fixed_now + timedelta(seconds=60),
    )
    k2 = mp._dedupe_key(
        "record_updated",
        2,
        fixed_now + timedelta(seconds=90),
    )
    k3 = mp._dedupe_key(
        "record_updated",
        2,
        fixed_now + timedelta(seconds=121),
    )

    assert k1 == k2
    assert k1 != k3
