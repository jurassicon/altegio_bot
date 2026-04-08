"""
Tests for the review_3d visit-limit guard.

Source of truth for visit counts is the Altegio API, not the local
``records`` table.  All tests mock ``count_attended_client_visits``
from ``altegio_records`` to simulate the API response.

Covers three layers:
- Backfill script: dry-run, cancel over limit, keep at limit,
  skip when no altegio_client_id, skip on API error.
- Outbox worker: cancel over limit, keep at limit, cancel when no
  altegio_client_id, requeue with backoff on API error, fail after max_attempts.
- Planner: does not schedule over limit, schedules at limit,
  skips when no altegio_client_id.
- Regression: local DB and Altegio API disagree -> API wins.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock

import httpx

from altegio_bot import message_planner as planner
from altegio_bot.message_planner import MAX_VISITS_FOR_REVIEW
from altegio_bot.scripts import cancel_review_3d_over_visit_limit as script
from altegio_bot.workers import outbox_worker as ow

# ─────────────────────────────────────────────────────────────────────
# Shared fixtures
# ─────────────────────────────────────────────────────────────────────


def run(coro: Any) -> Any:
    return asyncio.run(coro)


FIXED_NOW = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)
COMPANY_ID = 758285
CLIENT_ID = 42
ALTEGIO_CLIENT_ID = 12345
JOB_ID = 99


@dataclass
class FakeJob:
    id: int
    company_id: int
    job_type: str
    status: str
    run_at: datetime
    record_id: int | None = None
    client_id: int | None = CLIENT_ID
    last_error: str | None = None
    attempts: int = 0
    max_attempts: int = 5
    locked_at: datetime | None = None
    payload: dict = field(default_factory=dict)


@dataclass
class FakeRecord:
    id: int
    company_id: int
    client_id: int | None = CLIENT_ID
    attendance: int = 1
    visit_attendance: int = 0
    is_deleted: bool = False
    starts_at: datetime | None = None
    staff_name: str = "Tanja"
    short_link: str = ""


@dataclass
class FakeClient:
    id: int
    phone_e164: str | None = "+491234567890"
    wa_opted_out: bool = False
    altegio_client_id: int | None = ALTEGIO_CLIENT_ID


class FakeSession:
    def __init__(self) -> None:
        self.added: list[Any] = []

    def add(self, obj: Any) -> None:
        self.added.append(obj)


# ─────────────────────────────────────────────────────────────────────
# Backfill script tests (no real DB; SessionLocal + API are patched)
# ─────────────────────────────────────────────────────────────────────


class _FakeSessionCtx:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def __aenter__(self) -> Any:
        return self._session

    async def __aexit__(self, *_: Any) -> None:
        pass


class _FakeBeginCtx:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_: Any) -> None:
        pass


class _FakeWriteSession:
    def __init__(self) -> None:
        self.executed: list[Any] = []

    def begin(self) -> _FakeBeginCtx:
        return _FakeBeginCtx()

    async def execute(self, stmt: Any) -> None:
        self.executed.append(stmt)


@dataclass
class _FakeRow:
    """Mimics a SQLAlchemy Row with positional unpacking."""

    _values: tuple[Any, ...]

    def __iter__(self):  # type: ignore[override]
        return iter(self._values)


class _FakeResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = [_FakeRow(r) for r in rows]

    def all(self) -> list[_FakeRow]:
        return self._rows


class _Phase1Session:
    """Returns job rows (4-tuple including altegio_client_id)."""

    def __init__(
        self,
        rows: list[tuple[int, int | None, int | None, int | None]],
    ) -> None:
        self._rows = rows

    async def execute(self, _stmt: Any) -> _FakeResult:
        return _FakeResult(self._rows)


class _FakeSessionLocalFactory:
    """
    Simulates SessionLocal (async context-manager factory).

    Call 1 -> Phase 1 read session (job rows + altegio_client_id).
    Call 2 -> Phase 3 write session (UPDATE).
    Phase 2 (Altegio API calls) uses no DB session.
    """

    def __init__(
        self,
        job_rows: list[tuple[int, int | None, int | None, int | None]],
        write_session: _FakeWriteSession,
    ) -> None:
        self._job_rows = job_rows
        self._write_session = write_session
        self._call = 0

    def __call__(self) -> _FakeSessionCtx:
        self._call += 1
        if self._call == 1:
            return _FakeSessionCtx(_Phase1Session(self._job_rows))
        return _FakeSessionCtx(self._write_session)


def _make_factory(
    job_rows: list[tuple[int, int | None, int | None, int | None]],
    write_session: _FakeWriteSession,
) -> _FakeSessionLocalFactory:
    return _FakeSessionLocalFactory(job_rows, write_session)


# ── script scenarios ──────────────────────────────────────────────────


def test_script_dry_run_does_not_cancel(monkeypatch: Any) -> None:
    """dry-run=True: API is queried but no DB writes happen."""
    write_session = _FakeWriteSession()
    factory = _make_factory(
        job_rows=[(JOB_ID, CLIENT_ID, 10, ALTEGIO_CLIENT_ID)],
        write_session=write_session,
    )
    monkeypatch.setattr(script, "SessionLocal", factory)
    monkeypatch.setattr(
        script,
        "count_attended_client_visits",
        AsyncMock(return_value=MAX_VISITS_FOR_REVIEW + 1),
    )

    run(
        script.run_cancel(
            company_id=COMPANY_ID,
            dry_run=True,
            limit=None,
        )
    )

    assert write_session.executed == []


def test_script_cancels_job_when_visits_over_limit(
    monkeypatch: Any,
) -> None:
    """Real run: job is cancelled when Altegio count > limit."""
    write_session = _FakeWriteSession()
    factory = _make_factory(
        job_rows=[(JOB_ID, CLIENT_ID, 10, ALTEGIO_CLIENT_ID)],
        write_session=write_session,
    )
    monkeypatch.setattr(script, "SessionLocal", factory)
    monkeypatch.setattr(
        script,
        "count_attended_client_visits",
        AsyncMock(return_value=MAX_VISITS_FOR_REVIEW + 1),
    )

    run(
        script.run_cancel(
            company_id=COMPANY_ID,
            dry_run=False,
            limit=None,
        )
    )

    assert len(write_session.executed) == 1


def test_script_keeps_job_when_visits_at_limit(
    monkeypatch: Any,
) -> None:
    """Altegio count == MAX_VISITS_FOR_REVIEW -> job is kept."""
    write_session = _FakeWriteSession()
    factory = _make_factory(
        job_rows=[(JOB_ID, CLIENT_ID, 10, ALTEGIO_CLIENT_ID)],
        write_session=write_session,
    )
    monkeypatch.setattr(script, "SessionLocal", factory)
    monkeypatch.setattr(
        script,
        "count_attended_client_visits",
        AsyncMock(return_value=MAX_VISITS_FOR_REVIEW),
    )

    run(
        script.run_cancel(
            company_id=COMPANY_ID,
            dry_run=False,
            limit=None,
        )
    )

    assert write_session.executed == []


def test_script_skips_null_client_id(monkeypatch: Any) -> None:
    """Jobs with NULL client_id are skipped; API is not called."""
    write_session = _FakeWriteSession()
    api_mock = AsyncMock()
    factory = _make_factory(
        job_rows=[(JOB_ID, None, 10, None)],
        write_session=write_session,
    )
    monkeypatch.setattr(script, "SessionLocal", factory)
    monkeypatch.setattr(
        script,
        "count_attended_client_visits",
        api_mock,
    )

    run(
        script.run_cancel(
            company_id=COMPANY_ID,
            dry_run=False,
            limit=None,
        )
    )

    api_mock.assert_not_called()
    assert write_session.executed == []


def test_script_skips_null_altegio_client_id(
    monkeypatch: Any,
) -> None:
    """Jobs whose client has no altegio_client_id are skipped."""
    write_session = _FakeWriteSession()
    api_mock = AsyncMock()
    factory = _make_factory(
        job_rows=[(JOB_ID, CLIENT_ID, 10, None)],
        write_session=write_session,
    )
    monkeypatch.setattr(script, "SessionLocal", factory)
    monkeypatch.setattr(
        script,
        "count_attended_client_visits",
        api_mock,
    )

    run(
        script.run_cancel(
            company_id=COMPANY_ID,
            dry_run=False,
            limit=None,
        )
    )

    api_mock.assert_not_called()
    assert write_session.executed == []


def test_script_skips_on_api_error(monkeypatch: Any) -> None:
    """API error for a job: skip it, do not cancel, do not crash."""
    write_session = _FakeWriteSession()

    async def _raise(*_a: Any, **_kw: Any) -> int:
        raise httpx.ConnectError("timeout")

    factory = _make_factory(
        job_rows=[(JOB_ID, CLIENT_ID, 10, ALTEGIO_CLIENT_ID)],
        write_session=write_session,
    )
    monkeypatch.setattr(script, "SessionLocal", factory)
    monkeypatch.setattr(
        script,
        "count_attended_client_visits",
        _raise,
    )

    run(
        script.run_cancel(
            company_id=COMPANY_ID,
            dry_run=False,
            limit=None,
        )
    )

    assert write_session.executed == []


# ─────────────────────────────────────────────────────────────────────
# Outbox worker tests (no real DB; internals patched)
# ─────────────────────────────────────────────────────────────────────


def _make_review_3d_job() -> FakeJob:
    return FakeJob(
        id=JOB_ID,
        company_id=COMPANY_ID,
        job_type="review_3d",
        status="queued",
        run_at=FIXED_NOW,
        record_id=10,
        client_id=CLIENT_ID,
    )


def _patch_worker_common(
    monkeypatch: Any,
    job: FakeJob,
    client: FakeClient | None = None,
) -> None:
    """Patch mandatory stubs shared by all worker visit-limit tests."""
    if client is None:
        client = FakeClient(id=CLIENT_ID)

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_find_success(session: Any, job_id: int) -> Any:
        return None

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=COMPANY_ID)

    async def fake_load_client(
        session: Any,
        job_obj: Any,
        record: Any,
    ) -> Any:
        return client

    async def fake_safe_send(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("safe_send must not be called")

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    monkeypatch.setattr(ow, "_find_success_outbox", fake_find_success)
    monkeypatch.setattr(ow, "_load_record", fake_load_record)
    monkeypatch.setattr(ow, "_load_client", fake_load_client)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)
    monkeypatch.setattr(ow, "safe_send_template", fake_safe_send)
    monkeypatch.setattr(ow, "utcnow", lambda: FIXED_NOW)


def test_worker_cancels_review_3d_when_visits_over_limit(
    monkeypatch: Any,
) -> None:
    """Worker cancels review_3d when Altegio count > MAX."""
    job = _make_review_3d_job()
    _patch_worker_common(monkeypatch, job)
    monkeypatch.setattr(
        ow,
        "count_attended_client_visits",
        AsyncMock(return_value=MAX_VISITS_FOR_REVIEW + 1),
    )

    run(
        ow.process_job_in_session(
            FakeSession(),
            JOB_ID,
            provider=object(),  # type: ignore
        )
    )

    assert job.status == "canceled"
    assert job.last_error is not None
    assert str(MAX_VISITS_FOR_REVIEW) in job.last_error
    assert "Altegio API" in (job.last_error or "")


def test_worker_does_not_cancel_review_3d_when_visits_at_limit(
    monkeypatch: Any,
) -> None:
    """Worker must NOT cancel review_3d when Altegio count == MAX."""
    job = _make_review_3d_job()
    _patch_worker_common(monkeypatch, job)
    monkeypatch.setattr(
        ow,
        "count_attended_client_visits",
        AsyncMock(return_value=MAX_VISITS_FOR_REVIEW),
    )

    async def fake_apply_rl(session: Any, phone: str) -> Any:
        return None

    async def fake_render(*args: Any, **kwargs: Any) -> Any:
        raise ValueError("render error")

    monkeypatch.setattr(ow, "_apply_rate_limit", fake_apply_rl)
    monkeypatch.setattr(ow, "_render_message", fake_render)

    run(
        ow.process_job_in_session(
            FakeSession(),
            JOB_ID,
            provider=object(),  # type: ignore
        )
    )

    # Visit-limit guard must NOT have fired.
    assert job.status != "canceled"
    assert job.last_error != (f"Skipped: client has >{MAX_VISITS_FOR_REVIEW} attended visits (Altegio API)")
    # Reached render stage -> failed with template error.
    assert job.status == "failed"
    assert "render" in (job.last_error or "").lower()


def test_worker_cancels_review_3d_when_no_altegio_client_id(
    monkeypatch: Any,
) -> None:
    """Worker cancels review_3d when client has no altegio_client_id."""
    job = _make_review_3d_job()
    api_mock = AsyncMock()
    _patch_worker_common(
        monkeypatch,
        job,
        client=FakeClient(id=CLIENT_ID, altegio_client_id=None),
    )
    monkeypatch.setattr(
        ow,
        "count_attended_client_visits",
        api_mock,
    )

    run(
        ow.process_job_in_session(
            FakeSession(),
            JOB_ID,
            provider=object(),  # type: ignore
        )
    )

    api_mock.assert_not_called()
    assert job.status == "canceled"
    assert "altegio_client_id" in (job.last_error or "")


def test_worker_requeues_review_3d_on_api_error(
    monkeypatch: Any,
) -> None:
    """Transient Altegio API error: job is requeued with backoff,
    not killed (attempts < max_attempts)."""
    job = _make_review_3d_job()  # attempts=0, max_attempts=5
    _patch_worker_common(monkeypatch, job)

    async def _raise(*_a: Any, **_kw: Any) -> int:
        raise httpx.ConnectError("timeout")

    monkeypatch.setattr(
        ow,
        "count_attended_client_visits",
        _raise,
    )

    run(
        ow.process_job_in_session(
            FakeSession(),
            JOB_ID,
            provider=object(),  # type: ignore
        )
    )

    assert job.status == "queued"
    # Guard failures use the separate _api_guard_attempts counter in payload;
    # the send-attempt budget (job.attempts) must remain untouched.
    assert job.attempts == 0
    assert job.payload.get("_api_guard_attempts") == 1
    assert job.locked_at is None
    assert job.run_at == FIXED_NOW + timedelta(seconds=30)  # _retry_delay_seconds(1)
    assert "Altegio API error" in (job.last_error or "")


def test_worker_fails_review_3d_on_api_error_after_max_guard_attempts(
    monkeypatch: Any,
) -> None:
    """API guard error at MAX_API_GUARD_ATTEMPTS: job is marked failed (terminal).

    Guard retries use payload['_api_guard_attempts'], not job.attempts.
    Setting the guard counter to MAX_API_GUARD_ATTEMPTS - 1 means the next
    failure will tip it over the limit and permanently fail the job.
    """
    job = _make_review_3d_job()
    # Simulate the job having already exhausted all but one guard retry.
    job.payload = {"_api_guard_attempts": ow.MAX_API_GUARD_ATTEMPTS - 1}
    _patch_worker_common(monkeypatch, job)

    async def _raise(*_a: Any, **_kw: Any) -> int:
        raise httpx.ConnectError("timeout")

    monkeypatch.setattr(
        ow,
        "count_attended_client_visits",
        _raise,
    )

    run(
        ow.process_job_in_session(
            FakeSession(),
            JOB_ID,
            provider=object(),  # type: ignore
        )
    )

    assert job.status == "failed"
    assert job.attempts == 0  # send budget untouched
    assert job.payload.get("_api_guard_attempts") == ow.MAX_API_GUARD_ATTEMPTS
    assert "max guard attempts" in (job.last_error or "").lower()


# ─────────────────────────────────────────────────────────────────────
# Planner tests (add_job + count_attended_client_visits patched)
# ─────────────────────────────────────────────────────────────────────

# starts_at far enough ahead so review_at (starts_at+3d) > FIXED_NOW.
FUTURE_STARTS_AT = datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc)


class _PlannerFakeSession:
    """Minimal async session for planner tests."""

    async def get(self, model: Any, pk: Any) -> Any:
        if getattr(model, "__tablename__", None) == "records":
            return FakeRecord(
                id=10,
                company_id=COMPANY_ID,
                starts_at=FUTURE_STARTS_AT,
            )
        if getattr(model, "__tablename__", None) == "clients":
            return FakeClient(id=CLIENT_ID)
        return None


def test_planner_does_not_schedule_review_3d_when_visits_over_limit(
    monkeypatch: Any,
) -> None:
    """plan_jobs_for_record_event must not add review_3d when
    Altegio count > MAX_VISITS_FOR_REVIEW."""
    scheduled: list[str] = []

    async def fake_add_job(
        session: Any,
        *,
        job_type: str,
        **kwargs: Any,
    ) -> None:
        scheduled.append(job_type)

    monkeypatch.setattr(planner, "add_job", fake_add_job)
    monkeypatch.setattr(
        planner,
        "count_attended_client_visits",
        AsyncMock(return_value=MAX_VISITS_FOR_REVIEW + 1),
    )
    monkeypatch.setattr(planner, "utcnow", lambda: FIXED_NOW)

    run(
        planner.plan_jobs_for_record_event(
            _PlannerFakeSession(),
            record_id=10,
            company_id=COMPANY_ID,
            client_id=CLIENT_ID,
            event_status="create",
        )
    )

    assert "review_3d" not in scheduled


def test_planner_schedules_review_3d_when_visits_at_limit(
    monkeypatch: Any,
) -> None:
    """plan_jobs_for_record_event must add review_3d when
    Altegio count == MAX_VISITS_FOR_REVIEW."""
    scheduled: list[str] = []

    async def fake_add_job(
        session: Any,
        *,
        job_type: str,
        **kwargs: Any,
    ) -> None:
        scheduled.append(job_type)

    monkeypatch.setattr(planner, "add_job", fake_add_job)
    monkeypatch.setattr(
        planner,
        "count_attended_client_visits",
        AsyncMock(return_value=MAX_VISITS_FOR_REVIEW),
    )
    monkeypatch.setattr(planner, "utcnow", lambda: FIXED_NOW)

    run(
        planner.plan_jobs_for_record_event(
            _PlannerFakeSession(),
            record_id=10,
            company_id=COMPANY_ID,
            client_id=CLIENT_ID,
            event_status="create",
        )
    )

    assert "review_3d" in scheduled


def test_planner_skips_review_3d_when_no_altegio_client_id(
    monkeypatch: Any,
) -> None:
    """plan_jobs_for_record_event must not add review_3d when
    the client has no altegio_client_id."""
    scheduled: list[str] = []
    api_mock = AsyncMock()

    class _NoAltegioIdSession(_PlannerFakeSession):
        async def get(self, model: Any, pk: Any) -> Any:
            if getattr(model, "__tablename__", None) == "clients":
                return FakeClient(
                    id=CLIENT_ID,
                    altegio_client_id=None,
                )
            return await super().get(model, pk)

    async def fake_add_job(
        session: Any,
        *,
        job_type: str,
        **kwargs: Any,
    ) -> None:
        scheduled.append(job_type)

    monkeypatch.setattr(planner, "add_job", fake_add_job)
    monkeypatch.setattr(
        planner,
        "count_attended_client_visits",
        api_mock,
    )
    monkeypatch.setattr(planner, "utcnow", lambda: FIXED_NOW)

    run(
        planner.plan_jobs_for_record_event(
            _NoAltegioIdSession(),
            record_id=10,
            company_id=COMPANY_ID,
            client_id=CLIENT_ID,
            event_status="create",
        )
    )

    api_mock.assert_not_called()
    assert "review_3d" not in scheduled


# ─────────────────────────────────────────────────────────────────────
# Regression: local DB and Altegio API disagree -> API wins
# ─────────────────────────────────────────────────────────────────────


def test_planner_follows_altegio_api_not_local_records(
    monkeypatch: Any,
) -> None:
    """Regression: local DB shows 1 attended record (below limit),
    Altegio API returns 19 (above limit).  The planner must NOT
    schedule review_3d -- it follows the API, not the local table.

    This mirrors the real-world case of client Evelina
    (+4917681426372, client_id=306, company_id=758285):
    19 visits in Altegio UI, only 1 attended record locally.
    """
    scheduled: list[str] = []

    async def fake_add_job(
        session: Any,
        *,
        job_type: str,
        **kwargs: Any,
    ) -> None:
        scheduled.append(job_type)

    monkeypatch.setattr(planner, "add_job", fake_add_job)
    # Altegio API: 19 real visits (above MAX_VISITS_FOR_REVIEW=3)
    monkeypatch.setattr(
        planner,
        "count_attended_client_visits",
        AsyncMock(return_value=19),
    )
    monkeypatch.setattr(planner, "utcnow", lambda: FIXED_NOW)

    run(
        planner.plan_jobs_for_record_event(
            _PlannerFakeSession(),
            record_id=10,
            company_id=COMPANY_ID,
            client_id=CLIENT_ID,
            event_status="create",
        )
    )

    assert "review_3d" not in scheduled
