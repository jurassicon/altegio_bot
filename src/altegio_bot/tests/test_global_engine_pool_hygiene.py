"""Regression: no pooled connection on the GLOBAL app engine outlives its event loop.

Production code may legitimately use the module-global ``SessionLocal`` instead of
an injected factory — ``meta_circuit.close_meta_circuit()`` is the one the
operator-relay send-failure paths reach. Under pytest-asyncio each test runs on its
own event loop, so a connection left in that engine's QueuePool stays bound to a
loop that is about to close. asyncpg then schedules ``Connection._cancel`` on the
dead loop, which is never awaited (unraisable RuntimeWarning) and poisons later,
unrelated tests with order-dependent failures.

The autouse ``_dispose_global_engine_pool`` fixture in conftest closes those
connections while their own loop is still alive. These two tests pin that contract:
the first proves the global engine really is used (so the risk is real and this
file cannot silently rot into a no-op), the second proves the pool was cleaned up
before the next test starts.

The tests are order-dependent BY DESIGN and rely on definition order within this
module, which is how pytest collects them.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import altegio_bot.db as app_db
from altegio_bot.services import meta_circuit


def _global_pool_counts() -> tuple[int, int]:
    pool = app_db.engine.pool
    return pool.checkedin(), pool.checkedout()


@pytest.mark.asyncio
async def test_a_global_engine_is_really_used_by_circuit_close(session_maker) -> None:
    """Closing the Meta circuit the way production does uses the GLOBAL engine.

    ``close_meta_circuit`` is called by the relay send-failure path without a
    session factory, so it opens a connection on the global pool. If this ever
    stops being true, the sibling test below would pass vacuously — hence this
    positive assertion.
    """
    await meta_circuit.close_meta_circuit(
        reason="pool_hygiene_probe",
        error_kind="http",
        error_code="503",
        next_probe_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )

    checked_in, checked_out = _global_pool_counts()
    assert checked_in + checked_out >= 1, "expected close_meta_circuit to use the global engine pool"


@pytest.mark.asyncio
async def test_b_global_pool_is_empty_at_the_start_of_the_next_test() -> None:
    """The previous test's global connection must NOT survive into this loop.

    Without the autouse dispose fixture the connection from the test above is
    still checked in here — bound to an event loop that has already been closed —
    which is exactly the state that produced
    ``RuntimeWarning: coroutine 'Connection._cancel' was never awaited``.
    """
    assert _global_pool_counts() == (0, 0)
