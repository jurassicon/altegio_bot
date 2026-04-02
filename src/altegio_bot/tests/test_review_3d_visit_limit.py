"""
Tests for the review_3d visit-limit guard.

Covers three layers of the fix:
- Backfill script: dry-run, cancel over limit, keep at limit, skip null client_id.
- Outbox worker: cancel over limit, keep at limit (falls through to render).
- Planner: does not schedule review_3d over limit, schedules at limit.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from altegio_bot import message_planner as planner
from altegio_bot.message_planner import MAX_VISITS_FOR_REVIEW
from altegio_bot.scripts import cancel_review_3d_over_visit_limit as script
from altegio_bot.workers import outbox_worker as ow

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def run(coro: Any) -> Any:
    return asyncio.run(coro)


FIXED_NOW = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)
COMPANY_ID = 758285
CLIENT_ID = 42
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


class FakeSession:
    def __init__(self) -> None:
        self.added: list[Any] = []

    def add(self, obj: Any) -> None:
        self.added.append(obj)


# ──────────────────────────────────────────────────────────────────────────────
# Script tests (unit – no real DB, SessionLocal is patched)
# ──────────────────────────────────────────────────────────────────────────────


class _FakeSessionCtx:
    """Minimal async-context-manager that wraps a pre-built FakeSession."""

    def __init__(self, session: Any) -> None:
        self._session = session

    async def __aenter__(self) -> Any:
        return self._session

    async def __aexit__(self, *_: Any) -> None:
        pass


class _FakeBeginCtx:
    """Minimal async-context-manager for session.begin()."""

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_: Any) -> None:
        pass


class _FakeWriteSession:
    """Session that records which UPDATE statements were executed."""

    def __init__(self) -> None:
        self.executed: list[Any] = []

    def begin(self) -> _FakeBeginCtx:
        return _FakeBeginCtx()

    async def execute(self, stmt: Any) -> None:
        self.executed.append(stmt)


class _FakeSessionLocalFactory:
    """
    Simulates SessionLocal (async context manager factory).

    The first two calls return read-only responses (phase 1 + phase 2 read).
    The third call returns a writable session (phase 2 write).
    """

    def __init__(
        self,
        job_rows: list[tuple[int, int | None, int | None]],
        attended_counts: dict[int, int],
        write_session: _FakeWriteSession,
    ) -> None:
        self._job_rows = job_rows
        self._attended = attended_counts
        self._write_session = write_session
        self._call = 0

    def __call__(self) -> _FakeSessionCtx:
        self._call += 1
        if self._call == 1:
            # Phase 1: return jobs list
            return _FakeSessionCtx(_Phase1Session(self._job_rows))
        if self._call == 2:
            # Phase 2 read: attended-visit counts
            return _FakeSessionCtx(_Phase2ReadSession(self._attended))
        # Phase 3: write
        return _FakeSessionCtx(self._write_session)


@dataclass
class _FakeRow:
    """Mimics a SQLAlchemy Row with positional access."""

    _values: tuple[Any, ...]

    def __iter__(self):  # type: ignore[override]
        return iter(self._values)


class _FakeResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = [_FakeRow(r) for r in rows]

    def all(self) -> list[_FakeRow]:
        return self._rows

    def scalar_one(self) -> Any:
        return self._rows[0]._values[0]


class _Phase1Session:
    def __init__(self, rows: list[tuple[int, int | None, int | None]]) -> None:
        self._rows = rows

    async def execute(self, _stmt: Any) -> _FakeResult:
        return _FakeResult(self._rows)


class _Phase2ReadSession:
    """Returns per-client attended-visit counts via count_client_visits."""

    def __init__(self, attended: dict[int, int]) -> None:
        self._attended = attended

    async def execute(self, stmt: Any) -> _FakeResult:
        # count_client_visits issues SELECT count() — we need to return the
        # count for the client embedded in the WHERE clauses.
        # Inspect the compiled WHERE params via the whereclause elements:
        client_id = _extract_client_id(stmt)
        count = self._attended.get(client_id, 0)
        return _FakeResult([(count,)])


def _extract_client_id(stmt: Any) -> int:
    """Walk the statement's WHERE clauses to find the client_id literal."""
    try:
        for clause in stmt.whereclause.clauses:
            right = getattr(clause, "right", None)
            if right is not None:
                val = getattr(right, "value", None)
                if isinstance(val, int) and val == CLIENT_ID:
                    return val
    except AttributeError:
        pass
    return CLIENT_ID  # fallback for single-clause stmts


def _make_factory(
    job_rows: list[tuple[int, int | None, int | None]],
    attended_counts: dict[int, int],
    write_session: _FakeWriteSession,
) -> _FakeSessionLocalFactory:
    return _FakeSessionLocalFactory(job_rows, attended_counts, write_session)


def test_script_dry_run_does_not_cancel(monkeypatch: Any) -> None:
    """dry-run=True: no write session is used, job statuses must not change."""
    write_session = _FakeWriteSession()
    factory = _make_factory(
        job_rows=[(JOB_ID, CLIENT_ID, 10)],
        attended_counts={CLIENT_ID: MAX_VISITS_FOR_REVIEW + 1},  # exceeds limit
        write_session=write_session,
    )
    monkeypatch.setattr(script, "SessionLocal", factory)

    run(script.run_cancel(company_id=COMPANY_ID, dry_run=True, limit=None))

    # No writes must have been issued
    assert write_session.executed == []


def test_script_cancels_job_when_visits_over_limit(monkeypatch: Any) -> None:
    """Real run: job is added to the UPDATE when attended visits > limit."""
    write_session = _FakeWriteSession()
    factory = _make_factory(
        job_rows=[(JOB_ID, CLIENT_ID, 10)],
        attended_counts={CLIENT_ID: MAX_VISITS_FOR_REVIEW + 1},
        write_session=write_session,
    )
    monkeypatch.setattr(script, "SessionLocal", factory)

    run(script.run_cancel(company_id=COMPANY_ID, dry_run=False, limit=None))

    assert len(write_session.executed) == 1


def test_script_keeps_job_when_visits_at_limit(monkeypatch: Any) -> None:
    """Client has exactly MAX_VISITS_FOR_REVIEW attended visits → keep."""
    write_session = _FakeWriteSession()
    factory = _make_factory(
        job_rows=[(JOB_ID, CLIENT_ID, 10)],
        attended_counts={CLIENT_ID: MAX_VISITS_FOR_REVIEW},  # at limit, not over
        write_session=write_session,
    )
    monkeypatch.setattr(script, "SessionLocal", factory)

    run(script.run_cancel(company_id=COMPANY_ID, dry_run=False, limit=None))

    assert write_session.executed == []


def test_script_skips_null_client_id(monkeypatch: Any) -> None:
    """Jobs with NULL client_id must be skipped (no crash, no cancellation)."""
    write_session = _FakeWriteSession()
    factory = _make_factory(
        job_rows=[(JOB_ID, None, 10)],  # client_id is NULL
        attended_counts={},
        write_session=write_session,
    )
    monkeypatch.setattr(script, "SessionLocal", factory)

    run(script.run_cancel(company_id=COMPANY_ID, dry_run=False, limit=None))

    assert write_session.executed == []


# ──────────────────────────────────────────────────────────────────────────────
# Worker tests (unit – no real DB, SessionLocal + count_client_visits patched)
# ──────────────────────────────────────────────────────────────────────────────


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


def _patch_worker_common(monkeypatch: Any, job: FakeJob, attended: int) -> None:
    """Patch the mandatory stubs shared by all worker visit-limit tests."""

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_find_success(session: Any, job_id: int) -> Any:
        return None

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=COMPANY_ID)

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=CLIENT_ID)

    async def fake_count_visits(session: Any, *, client_id: int, company_id: int, attended_only: bool = False) -> int:
        return attended

    async def fake_safe_send(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("safe_send must not be called")

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    monkeypatch.setattr(ow, "_find_success_outbox", fake_find_success)
    monkeypatch.setattr(ow, "_load_record", fake_load_record)
    monkeypatch.setattr(ow, "_load_client", fake_load_client)
    monkeypatch.setattr(ow, "count_client_visits", fake_count_visits)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)
    monkeypatch.setattr(ow, "safe_send_template", fake_safe_send)


def test_worker_cancels_review_3d_when_visits_over_limit(monkeypatch: Any) -> None:
    """Worker must cancel review_3d when client has >MAX_VISITS_FOR_REVIEW attended visits."""
    job = _make_review_3d_job()
    _patch_worker_common(monkeypatch, job, attended=MAX_VISITS_FOR_REVIEW + 1)

    run(ow.process_job_in_session(FakeSession(), JOB_ID, provider=object()))  # type: ignore

    assert job.status == "canceled"
    assert job.last_error is not None
    assert str(MAX_VISITS_FOR_REVIEW) in job.last_error


def test_worker_does_not_cancel_review_3d_when_visits_at_limit(monkeypatch: Any) -> None:
    """Worker must NOT cancel review_3d when client has exactly MAX_VISITS_FOR_REVIEW attended visits."""
    job = _make_review_3d_job()
    _patch_worker_common(monkeypatch, job, attended=MAX_VISITS_FOR_REVIEW)

    # The job will proceed past the visit-limit guard but will fail later (no
    # phone / render / send).  We only care that the visit-limit check does NOT
    # cancel here.
    async def fake_apply_rl(session: Any, phone: str) -> Any:
        return None

    async def fake_render(*args: Any, **kwargs: Any) -> Any:
        raise ValueError("render error")

    monkeypatch.setattr(ow, "_apply_rate_limit", fake_apply_rl)
    monkeypatch.setattr(ow, "_render_message", fake_render)

    run(ow.process_job_in_session(FakeSession(), JOB_ID, provider=object()))  # type: ignore

    # Visit-limit guard must NOT have fired.
    assert job.status != "canceled"
    assert job.last_error != f"Skipped: client has >{MAX_VISITS_FOR_REVIEW} attended visits"
    # Reached render stage → failed with template error, not visit-limit.
    assert job.status == "failed"
    assert "render" in (job.last_error or "").lower()


# ──────────────────────────────────────────────────────────────────────────────
# Planner tests (unit – add_job and _count_client_visits patched)
# ──────────────────────────────────────────────────────────────────────────────

# starts_at far enough in the future so that review_at (starts_at + 3d) > FIXED_NOW.
FUTURE_STARTS_AT = datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc)


class _PlannerFakeSession:
    """Minimal async session for planner tests: serves Record and Client via get()."""

    async def get(self, model: Any, pk: Any) -> Any:
        if getattr(model, "__tablename__", None) == "records":
            return FakeRecord(id=10, company_id=COMPANY_ID, starts_at=FUTURE_STARTS_AT)
        if getattr(model, "__tablename__", None) == "clients":
            return FakeClient(id=CLIENT_ID)
        return None


def test_planner_does_not_schedule_review_3d_when_visits_over_limit(monkeypatch: Any) -> None:
    """plan_jobs_for_record_event must not add review_3d when attended > MAX_VISITS_FOR_REVIEW."""
    scheduled: list[str] = []

    async def fake_add_job(session: Any, *, job_type: str, **kwargs: Any) -> None:
        scheduled.append(job_type)

    async def fake_count_visits(session: Any, *, client_id: int, company_id: int, attended_only: bool = False) -> int:
        return MAX_VISITS_FOR_REVIEW + 1

    monkeypatch.setattr(planner, "add_job", fake_add_job)
    monkeypatch.setattr(planner, "_count_client_visits", fake_count_visits)
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


def test_planner_schedules_review_3d_when_visits_at_limit(monkeypatch: Any) -> None:
    """plan_jobs_for_record_event must add review_3d when attended == MAX_VISITS_FOR_REVIEW."""
    scheduled: list[str] = []

    async def fake_add_job(session: Any, *, job_type: str, **kwargs: Any) -> None:
        scheduled.append(job_type)

    async def fake_count_visits(session: Any, *, client_id: int, company_id: int, attended_only: bool = False) -> int:
        return MAX_VISITS_FOR_REVIEW

    monkeypatch.setattr(planner, "add_job", fake_add_job)
    monkeypatch.setattr(planner, "_count_client_visits", fake_count_visits)
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
