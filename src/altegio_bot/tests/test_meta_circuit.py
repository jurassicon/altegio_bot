from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import func, select

from altegio_bot.models.models import MetaCircuitBreaker
from altegio_bot.services import meta_circuit as mc


def _probe_at(seconds: int = 300) -> Any:
    return mc._utcnow() + timedelta(seconds=seconds)


async def _row_count(session_maker: Any) -> int:
    async with session_maker() as session:
        res = await session.execute(select(func.count()).select_from(MetaCircuitBreaker))
        return int(res.scalar_one())


@pytest.mark.asyncio
async def test_default_state_is_open(session_maker: Any) -> None:
    state = await mc.get_meta_circuit_state(session_factory=session_maker)
    assert state.state == "open"
    assert state.sends_paused is False
    assert state.probe_token is None
    assert state.probe_lease_until is None
    assert await mc.should_pause_meta_sends(session_factory=session_maker) is False
    assert await _row_count(session_maker) == 1


@pytest.mark.asyncio
async def test_close_meta_circuit_stores_safe_fields(session_maker: Any) -> None:
    await mc.close_meta_circuit(
        session_factory=session_maker,
        reason="transient_send_error",
        error_kind="http",
        error_code="500",
        next_probe_at=_probe_at(),
    )

    state = await mc.get_meta_circuit_state(session_factory=session_maker)
    assert state.state == "closed"
    assert state.sends_paused is True
    assert state.reason == "transient_send_error"
    assert state.last_error_kind == "http"
    assert state.last_error_code == "500"
    assert state.next_probe_at is not None
    assert state.closed_at is not None


@pytest.mark.asyncio
async def test_open_meta_circuit_resets_probe_fields(session_maker: Any) -> None:
    await mc.close_meta_circuit(
        session_factory=session_maker,
        reason="transient_send_error",
        error_kind="meta_code",
        error_code="2",
        next_probe_at=_probe_at(),
    )
    await mc.mark_meta_circuit_probe_failed(
        session_factory=session_maker,
        reason="probe_failed",
        next_probe_at=_probe_at(600),
    )
    await mc.open_meta_circuit(session_factory=session_maker)

    state = await mc.get_meta_circuit_state(session_factory=session_maker)
    assert state.state == "open"
    assert state.sends_paused is False
    assert state.probe_attempts == 0
    assert state.next_probe_at is None
    assert state.last_error_kind is None
    assert state.last_error_code is None
    assert state.opened_at is not None


@pytest.mark.asyncio
async def test_mark_probing_only_when_not_open(session_maker: Any) -> None:
    assert await mc.mark_meta_circuit_probing(session_factory=session_maker) is None
    assert (await mc.get_meta_circuit_state(session_factory=session_maker)).state == "open"

    await mc.close_meta_circuit(
        session_factory=session_maker,
        reason="transient_send_error",
        error_kind="http",
        error_code="503",
        next_probe_at=_probe_at(-1),
    )
    token = await mc.mark_meta_circuit_probing(session_factory=session_maker)
    state = await mc.get_meta_circuit_state(session_factory=session_maker)
    assert token is not None
    assert state.state == "half_open"
    assert state.probe_token == token
    assert state.probe_started_at is not None
    assert state.probe_lease_until is not None


@pytest.mark.asyncio
async def test_probe_failed_increments_attempts(session_maker: Any) -> None:
    await mc.close_meta_circuit(
        session_factory=session_maker,
        reason="transient_send_error",
        error_kind="http",
        error_code="503",
        next_probe_at=_probe_at(),
    )
    n1 = await mc.mark_meta_circuit_probe_failed(
        session_factory=session_maker,
        reason="probe_failed",
        next_probe_at=_probe_at(600),
    )
    n2 = await mc.mark_meta_circuit_probe_failed(
        session_factory=session_maker,
        reason="probe_failed",
        next_probe_at=_probe_at(900),
    )

    state = await mc.get_meta_circuit_state(session_factory=session_maker)
    assert n1 == 1
    assert n2 == 2
    assert state.state == "closed"
    assert state.probe_attempts == 2


@pytest.mark.asyncio
async def test_active_probe_lease_blocks_second_probe(session_maker: Any) -> None:
    await mc.close_meta_circuit(
        session_factory=session_maker,
        reason="transient_send_error",
        error_kind="http",
        error_code="503",
        next_probe_at=mc._utcnow() - timedelta(seconds=1),
    )

    token = await mc.mark_meta_circuit_probing(session_factory=session_maker)
    second = await mc.mark_meta_circuit_probing(session_factory=session_maker)

    assert token is not None
    assert second is None
    state = await mc.get_meta_circuit_state(session_factory=session_maker)
    assert state.state == "half_open"
    assert state.probe_token == token


@pytest.mark.asyncio
async def test_expired_probe_lease_can_be_acquired_again(session_maker: Any) -> None:
    await mc.close_meta_circuit(
        session_factory=session_maker,
        reason="transient_send_error",
        error_kind="http",
        error_code="503",
        next_probe_at=mc._utcnow() - timedelta(seconds=1),
    )
    first = await mc.mark_meta_circuit_probing(session_factory=session_maker)
    assert first is not None

    async with session_maker() as session:
        async with session.begin():
            row = (await session.execute(select(MetaCircuitBreaker))).scalar_one()
            row.probe_lease_until = mc._utcnow() - timedelta(seconds=1)

    second = await mc.mark_meta_circuit_probing(session_factory=session_maker)

    assert second is not None
    assert second != first
    state = await mc.get_meta_circuit_state(session_factory=session_maker)
    assert state.probe_token == second


@pytest.mark.asyncio
async def test_stale_probe_results_are_noop(session_maker: Any) -> None:
    await mc.close_meta_circuit(
        session_factory=session_maker,
        reason="transient_send_error",
        error_kind="http",
        error_code="503",
        next_probe_at=mc._utcnow() - timedelta(seconds=1),
    )
    token = await mc.mark_meta_circuit_probing(session_factory=session_maker)
    assert token is not None

    assert await mc.open_meta_circuit(session_factory=session_maker, probe_token="wrong-token") is False
    assert (await mc.get_meta_circuit_state(session_factory=session_maker)).state == "half_open"

    assert (
        await mc.mark_meta_circuit_probe_failed(
            session_factory=session_maker,
            reason="probe_failed",
            next_probe_at=_probe_at(600),
            probe_token="wrong-token",
        )
        == 0
    )
    state = await mc.get_meta_circuit_state(session_factory=session_maker)
    assert state.state == "half_open"
    assert state.probe_token == token

    assert await mc.open_meta_circuit(session_factory=session_maker, probe_token=token) is True
    await mc.mark_meta_circuit_probe_failed(
        session_factory=session_maker,
        reason="late_probe_failed",
        next_probe_at=_probe_at(600),
        probe_token=token,
    )
    state = await mc.get_meta_circuit_state(session_factory=session_maker)
    assert state.state == "open"
    assert state.probe_attempts == 0


@pytest.mark.asyncio
async def test_get_and_pause_check_fail_open(caplog: Any) -> None:
    class _BoomSession:
        async def __aenter__(self) -> Any:
            raise RuntimeError("db down")

        async def __aexit__(self, *args: Any) -> None:
            return None

    def _boom_factory() -> Any:
        return _BoomSession()

    with caplog.at_level(logging.WARNING, logger="meta_circuit"):
        state = await mc.get_meta_circuit_state(session_factory=_boom_factory)
        paused = await mc.should_pause_meta_sends(session_factory=_boom_factory)

    assert state.state == "open"
    assert paused is False
    assert any("failing open" in r.message for r in caplog.records)


class _Result:
    def __init__(self, val: Any) -> None:
        self._val = val

    def scalar_one_or_none(self) -> Any:
        return self._val


class _Savepoint:
    async def __aenter__(self) -> Any:
        return self

    async def __aexit__(self, *exc: Any) -> bool:
        return False


class _FakeRaceSession:
    def __init__(self, existing: Any) -> None:
        self._existing = existing
        self.select_calls = 0
        self.added: list[Any] = []

    async def execute(self, stmt: Any) -> Any:
        self.select_calls += 1
        return _Result(None if self.select_calls == 1 else self._existing)

    def add(self, obj: Any) -> None:
        self.added.append(obj)

    def begin_nested(self) -> Any:
        return _Savepoint()

    async def flush(self) -> None:
        from sqlalchemy.exc import IntegrityError

        raise IntegrityError("INSERT", {}, Exception("duplicate key scope"))


@pytest.mark.asyncio
async def test_get_or_create_handles_integrity_race() -> None:
    sentinel = object()
    sess = _FakeRaceSession(sentinel)

    row = await mc._get_or_create_row(sess, "global")

    assert row is sentinel
    assert sess.select_calls == 2
    assert len(sess.added) == 1
