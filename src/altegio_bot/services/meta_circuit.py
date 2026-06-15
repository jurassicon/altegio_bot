"""Global Meta send circuit breaker.

The circuit state is stored in Postgres and shared by all worker processes.
State names are intentional:

* open: sends are allowed, Meta is treated as healthy.
* closed: sends are paused during a Meta/network outage.
* half_open: the guard worker is probing recovery.

Circuit storage failures fail open for reads, so a state-store hiccup does not
stop business messaging. Write failures are logged and ignored.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Literal

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import MetaCircuitBreaker

logger = logging.getLogger("meta_circuit")

CircuitState = Literal["open", "closed", "half_open"]
DEFAULT_SCOPE = "global"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class MetaCircuitState:
    state: CircuitState
    reason: str | None
    opened_at: datetime | None
    closed_at: datetime | None
    updated_at: datetime
    next_probe_at: datetime | None
    probe_attempts: int
    last_error_kind: str | None
    last_error_code: str | None

    @property
    def sends_paused(self) -> bool:
        return self.state in ("closed", "half_open")


def _default_state() -> MetaCircuitState:
    now = _utcnow()
    return MetaCircuitState(
        state="open",
        reason=None,
        opened_at=None,
        closed_at=None,
        updated_at=now,
        next_probe_at=None,
        probe_attempts=0,
        last_error_kind=None,
        last_error_code=None,
    )


def _to_state(row: MetaCircuitBreaker) -> MetaCircuitState:
    return MetaCircuitState(
        state=row.state,  # type: ignore[arg-type]
        reason=row.reason,
        opened_at=row.opened_at,
        closed_at=row.closed_at,
        updated_at=row.updated_at,
        next_probe_at=row.next_probe_at,
        probe_attempts=int(row.probe_attempts or 0),
        last_error_kind=row.last_error_kind,
        last_error_code=row.last_error_code,
    )


def _session_factory(session_factory: Callable[..., Any] | None) -> Callable[..., Any]:
    return session_factory if session_factory is not None else SessionLocal


async def _get_or_create_row(session: Any, scope: str) -> MetaCircuitBreaker:
    stmt = select(MetaCircuitBreaker).where(MetaCircuitBreaker.scope == scope).with_for_update()
    row = (await session.execute(stmt)).scalar_one_or_none()
    if row is not None:
        return row

    new_row = MetaCircuitBreaker(scope=scope, state="open", probe_attempts=0)
    try:
        async with session.begin_nested():
            session.add(new_row)
            await session.flush()
        return new_row
    except IntegrityError:
        row = (await session.execute(stmt)).scalar_one_or_none()
        if row is None:  # pragma: no cover - defensive
            raise
        return row


async def get_meta_circuit_state(
    *,
    session_factory: Callable[..., Any] | None = None,
    scope: str = DEFAULT_SCOPE,
) -> MetaCircuitState:
    factory = _session_factory(session_factory)
    try:
        async with factory() as session:
            async with session.begin():
                row = await _get_or_create_row(session, scope)
                return _to_state(row)
    except Exception:
        logger.warning("meta circuit state read failed; failing open scope=%s", scope)
        return _default_state()


async def get_meta_circuit_state_from_session(
    session: Any,
    *,
    scope: str = DEFAULT_SCOPE,
) -> MetaCircuitState:
    """Read circuit state through an existing worker session.

    Lightweight fake sessions used by worker unit tests may not implement the
    full SQLAlchemy result API. Those default open without logging so unrelated
    tests do not receive non-perf log records.
    """
    execute = getattr(session, "execute", None)
    if not callable(execute):
        return _default_state()

    try:
        result = await execute(select(MetaCircuitBreaker).where(MetaCircuitBreaker.scope == scope))
        scalar_one_or_none = getattr(result, "scalar_one_or_none", None)
        if not callable(scalar_one_or_none):
            return _default_state()
        row = scalar_one_or_none()
        if row is None:
            return _default_state()
        return _to_state(row)
    except Exception:
        logger.warning("meta circuit state read failed; failing open scope=%s", scope)
        return _default_state()


async def should_pause_meta_sends(
    *,
    session: Any | None = None,
    session_factory: Callable[..., Any] | None = None,
    scope: str = DEFAULT_SCOPE,
) -> bool:
    if session is not None:
        state = await get_meta_circuit_state_from_session(session, scope=scope)
    else:
        state = await get_meta_circuit_state(session_factory=session_factory, scope=scope)
    return state.sends_paused


async def close_meta_circuit(
    *,
    reason: str,
    error_kind: str | None,
    error_code: str | None,
    next_probe_at: datetime,
    session_factory: Callable[..., Any] | None = None,
    scope: str = DEFAULT_SCOPE,
) -> None:
    factory = _session_factory(session_factory)
    now = _utcnow()
    try:
        async with factory() as session:
            async with session.begin():
                row = await _get_or_create_row(session, scope)
                already_closed = row.state == "closed"
                row.state = "closed"
                row.reason = reason
                row.last_error_kind = error_kind
                row.last_error_code = error_code
                row.next_probe_at = next_probe_at
                row.updated_at = now
                if not already_closed:
                    row.closed_at = now
                    row.probe_attempts = 0
    except Exception:
        logger.warning(
            "meta circuit close failed; ignored scope=%s error_kind=%s error_code=%s",
            scope,
            error_kind,
            error_code,
        )
        return

    logger.warning(
        "meta circuit closed scope=%s reason=%s error_kind=%s error_code=%s next_probe_at=%s",
        scope,
        reason,
        error_kind,
        error_code,
        next_probe_at.isoformat(),
    )


async def open_meta_circuit(
    *,
    reason: str = "probe_succeeded",
    session_factory: Callable[..., Any] | None = None,
    scope: str = DEFAULT_SCOPE,
) -> None:
    factory = _session_factory(session_factory)
    now = _utcnow()
    try:
        async with factory() as session:
            async with session.begin():
                row = await _get_or_create_row(session, scope)
                row.state = "open"
                row.reason = reason
                row.opened_at = now
                row.next_probe_at = None
                row.probe_attempts = 0
                row.last_error_kind = None
                row.last_error_code = None
                row.updated_at = now
    except Exception:
        logger.warning("meta circuit open failed; ignored scope=%s reason=%s", scope, reason)
        return

    logger.warning("meta circuit open scope=%s reason=%s", scope, reason)


async def mark_meta_circuit_probing(
    *,
    session_factory: Callable[..., Any] | None = None,
    scope: str = DEFAULT_SCOPE,
) -> None:
    factory = _session_factory(session_factory)
    now = _utcnow()
    try:
        async with factory() as session:
            async with session.begin():
                row = await _get_or_create_row(session, scope)
                if row.state == "open":
                    return
                row.state = "half_open"
                row.updated_at = now
    except Exception:
        logger.warning("meta circuit probing mark failed; ignored scope=%s", scope)


async def mark_meta_circuit_probe_failed(
    *,
    reason: str,
    next_probe_at: datetime,
    session_factory: Callable[..., Any] | None = None,
    scope: str = DEFAULT_SCOPE,
) -> int:
    factory = _session_factory(session_factory)
    now = _utcnow()
    attempts = 0
    try:
        async with factory() as session:
            async with session.begin():
                row = await _get_or_create_row(session, scope)
                attempts = int(row.probe_attempts or 0) + 1
                row.state = "closed"
                row.reason = reason
                row.probe_attempts = attempts
                row.next_probe_at = next_probe_at
                row.updated_at = now
    except Exception:
        logger.warning("meta circuit probe failure mark failed; ignored scope=%s", scope)
        return 0

    logger.warning(
        "meta circuit probe failed scope=%s probe_attempt=%s next_probe_at=%s",
        scope,
        attempts,
        next_probe_at.isoformat(),
    )
    return attempts
