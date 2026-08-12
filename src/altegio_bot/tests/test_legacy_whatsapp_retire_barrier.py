"""The DB-side barrier really synchronises with an in-flight claim.

`docker pause` freezes the client, not its PostgreSQL backend: a COMMIT already
on the wire completes regardless, so a SELECT from another session can read 0
just before the stranded rows become visible. Only a lock the claim path
actually conflicts with closes that window.

These tests drive REAL concurrent PostgreSQL transactions — one playing the
legacy worker's claim, one running the barrier — and assert the ordering the
retirement depends on. They are not runnable without a database, by design: the
whole point is the lock manager's behaviour.

`LOCK TABLE whatsapp_events IN SHARE MODE` is the barrier because the claim
flushes `UPDATE whatsapp_events SET status='processing'`, which holds ROW
EXCLUSIVE; SHARE is the weakest mode that conflicts with it.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.models.models import WhatsAppEvent
from altegio_bot.scripts import retire_legacy_whatsapp_worker as retire_module
from altegio_bot.scripts.retire_legacy_whatsapp_worker import (
    LegacyRetirementError,
    retire_with_barrier,
)


class FakeDocker:
    """Records what the procedure did to the container, and can fail on cue."""

    def __init__(self, *, remove_fails: bool = False, exists: bool = True) -> None:
        self.calls: list[str] = []
        self.remove_fails = remove_fails
        self._exists = exists

    def pause(self, container: str) -> None:
        self.calls.append("pause")

    def unpause(self, container: str) -> None:
        self.calls.append("unpause")

    def remove(self, container: str) -> None:
        self.calls.append("remove")
        if self.remove_fails:
            raise LegacyRetirementError("docker rm failed")
        self._exists = False

    def exists(self, container: str) -> bool:
        return self._exists


async def _seed_event(session: AsyncSession, event_id: int, status: str = "received") -> None:
    session.add(
        WhatsAppEvent(
            id=event_id,
            dedupe_key=f"wa:barrier:{event_id}",
            status=status,
            payload={},
            query={},
            headers={},
            received_at=datetime(2026, 8, 12, 9, 0, tzinfo=timezone.utc),
        )
    )
    await session.commit()


async def _processing_count(session_maker: async_sessionmaker[AsyncSession]) -> int:
    async with session_maker() as session:
        result = await session.execute(text("SELECT count(*) FROM whatsapp_events WHERE status = 'processing'"))
        return int(result.scalar_one())


# ---------------------------------------------------------------------------
# An open claim blocks the barrier
# ---------------------------------------------------------------------------


async def test_an_open_claim_transaction_stops_the_barrier_within_its_timeout(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """The claim holds ROW EXCLUSIVE, so SHARE cannot be granted."""
    async with session_maker() as seeder:
        await _seed_event(seeder, 1)

    docker = FakeDocker()

    async with session_maker() as claimer:
        await claimer.execute(text("UPDATE whatsapp_events SET status = 'processing' WHERE id = 1"))
        # Deliberately NOT committed: this is the in-flight claim.

        with pytest.raises(LegacyRetirementError) as caught:
            await retire_with_barrier(
                session_maker,
                container="legacy-cid",
                docker=docker,
                lock_timeout_ms=700,
            )

        await claimer.rollback()

    assert "barrier" in str(caught.value)
    assert "remove" not in docker.calls, "the container must survive an unproven barrier"
    assert docker.calls == ["pause", "unpause"], "the frozen worker is released for analysis"


async def test_a_commit_landing_during_the_wait_is_seen_by_the_barrier(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """The race `docker pause` cannot close, closed by the lock.

    The claim's COMMIT is still in flight when the retirement starts. The
    barrier must be granted only AFTER that commit, and must then see the row
    it produced — so the retirement is refused rather than stranding it.
    """
    async with session_maker() as seeder:
        await _seed_event(seeder, 2)

    docker = FakeDocker()

    async with session_maker() as claimer:
        await claimer.execute(text("UPDATE whatsapp_events SET status = 'processing' WHERE id = 2"))

        async def commit_shortly() -> None:
            # Long enough that the barrier is already queued on the lock.
            await asyncio.sleep(0.3)
            await claimer.commit()

        async def run_barrier() -> None:
            await retire_with_barrier(
                session_maker,
                container="legacy-cid",
                docker=docker,
                lock_timeout_ms=5_000,
            )

        with pytest.raises(LegacyRetirementError) as caught:
            await asyncio.gather(run_barrier(), commit_shortly())

    message = str(caught.value)
    assert "processing" in message, f"the barrier must report the committed rows, got: {message}"
    assert "remove" not in docker.calls, "a committed batch must block the retirement"
    assert await _processing_count(session_maker) == 1, "and must not be rewritten"


# ---------------------------------------------------------------------------
# The clean path
# ---------------------------------------------------------------------------


async def test_an_empty_queue_retires_the_container_under_the_held_lock(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as seeder:
        await _seed_event(seeder, 3)

    docker = FakeDocker()
    outcome = await retire_with_barrier(session_maker, container="legacy-cid", docker=docker, lock_timeout_ms=5_000)

    assert outcome.retired is True
    assert outcome.processing_under_lock == 0
    assert outcome.processing_after_removal == 0
    assert docker.calls == ["pause", "remove"], "no unpause: the container is gone"


async def test_a_concurrent_claim_is_blocked_while_the_barrier_is_held(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """The window between the check and the removal must be closed.

    While the retirement holds SHARE, a claim's UPDATE has to wait — which is
    exactly why nothing can be committed behind its back.
    """
    async with session_maker() as seeder:
        await _seed_event(seeder, 4)

    claim_succeeded_at: list[float] = []
    removal_at: list[float] = []

    class SlowRecordingDocker(FakeDocker):
        """Removal takes a moment, widening the window a claim would need."""

        def remove(self, container: str) -> None:
            super().remove(container)
            removal_at.append(asyncio.get_running_loop().time())
            time.sleep(0.5)

    async def competing_claim() -> None:
        await asyncio.sleep(0.1)  # let the barrier take the lock first
        async with session_maker() as claimer:
            await claimer.execute(text("SET LOCAL lock_timeout = 10000"))
            await claimer.execute(text("UPDATE whatsapp_events SET status = 'processing' WHERE id = 4"))
            await claimer.commit()
        claim_succeeded_at.append(asyncio.get_running_loop().time())

    async def slow_barrier() -> None:
        await retire_with_barrier(
            session_maker, container="legacy-cid", docker=SlowRecordingDocker(), lock_timeout_ms=5_000
        )

    await asyncio.gather(slow_barrier(), competing_claim())

    assert claim_succeeded_at, "the claim eventually runs, after the barrier releases"
    assert removal_at, "the container was removed"
    assert min(removal_at) < claim_succeeded_at[0], (
        "the competing claim must not commit before the retirement completed"
    )


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


async def test_pre_existing_processing_rows_block_the_retirement(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as seeder:
        await _seed_event(seeder, 5, status="processing")

    docker = FakeDocker()

    with pytest.raises(LegacyRetirementError) as caught:
        await retire_with_barrier(session_maker, container="legacy-cid", docker=docker, lock_timeout_ms=5_000)

    assert "processing" in str(caught.value)
    assert "remove" not in docker.calls
    assert docker.calls == ["pause", "unpause"]
    assert await _processing_count(session_maker) == 1, "no automatic rewrite to 'received'"


async def test_a_failing_removal_fails_closed_and_leaves_no_lock_behind(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as seeder:
        await _seed_event(seeder, 6)

    docker = FakeDocker(remove_fails=True)

    with pytest.raises(LegacyRetirementError):
        await retire_with_barrier(session_maker, container="legacy-cid", docker=docker, lock_timeout_ms=5_000)

    # If the transaction had leaked, this second barrier would time out.
    async with session_maker() as prober:
        await prober.execute(text("SET LOCAL lock_timeout = 1000"))
        await prober.execute(text("LOCK TABLE whatsapp_events IN SHARE MODE"))
        await prober.rollback()


async def test_a_container_that_vanished_is_not_unpaused(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """Never resurrect or touch a container that is already gone."""
    async with session_maker() as seeder:
        await _seed_event(seeder, 7, status="processing")

    docker = FakeDocker(exists=False)

    with pytest.raises(LegacyRetirementError):
        await retire_with_barrier(session_maker, container="legacy-cid", docker=docker, lock_timeout_ms=5_000)

    assert docker.calls == ["pause"], "a missing container must not be unpaused"


@pytest.mark.parametrize("bad", [0, -1])
async def test_non_positive_timeouts_are_refused(
    session_maker: async_sessionmaker[AsyncSession],
    bad: int,
) -> None:
    """An unbounded barrier would move the outage instead of failing closed."""
    docker = FakeDocker()

    with pytest.raises(LegacyRetirementError):
        await retire_with_barrier(session_maker, container="legacy-cid", docker=docker, lock_timeout_ms=bad)

    assert docker.calls == [], "nothing may be touched before the arguments are validated"


def test_the_procedure_issues_no_write_sql() -> None:
    """Every SQL literal the module executes, checked as SQL.

    Scanned from the AST rather than by grepping the file: the module docstring
    legitimately quotes the claim's `UPDATE whatsapp_events` when explaining
    which lock mode conflicts with it.
    """
    tree = ast.parse(Path(inspect.getfile(retire_module)).read_text())

    statements = [
        node.args[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "text"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    ]
    # f-strings (the SET LOCAL timeouts) are JoinedStr, not Constant; include
    # their literal parts so nothing executed escapes the check.
    statements += [
        part.value
        for node in ast.walk(tree)
        if isinstance(node, ast.JoinedStr)
        for part in node.values
        if isinstance(part, ast.Constant) and isinstance(part.value, str)
    ]

    assert statements, "the module must execute some SQL"
    for statement in statements:
        upper = statement.upper()
        for write in ("UPDATE ", "DELETE ", "INSERT "):
            assert write not in upper, f"the retirement must never rewrite events: {statement!r}"
