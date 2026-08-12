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
import threading
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
    Presence,
    retire_with_barrier,
)


class FakeDocker:
    """Stands in for the Docker CLI, and can fail on cue.

    Mirrors the real tri-state `presence`: a failed inspect is reported as
    UNREADABLE, never as ABSENT.
    """

    def __init__(
        self,
        *,
        remove_fails: bool = False,
        presence: Presence = Presence.PRESENT,
        container_ids: list[str] | None = None,
        discovery_fails: bool = False,
        on_remove=None,
    ) -> None:
        self.calls: list[str] = []
        self.remove_fails = remove_fails
        self._presence = presence
        self._ids = ["legacy-cid"] if container_ids is None else container_ids
        self._discovery_fails = discovery_fails
        self._on_remove = on_remove

    def pause(self, container: str) -> None:
        self.calls.append("pause")

    def unpause(self, container: str) -> None:
        self.calls.append("unpause")

    def remove(self, container: str) -> None:
        self.calls.append("remove")
        if self._on_remove is not None:
            self._on_remove()
        if self.remove_fails:
            raise LegacyRetirementError("docker rm failed")
        self._presence = Presence.ABSENT

    def presence(self, container: str) -> Presence:
        return self._presence

    def service_container_ids(self) -> list[str] | None:
        return None if self._discovery_fails else self._ids


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


async def test_a_concurrent_claim_really_waits_for_the_barrier_to_be_released(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """The window between the check and the removal is genuinely closed.

    The earlier version of this test slept inside the (synchronous) fake
    `remove` while the competing claim was just another coroutine on the same
    event loop — so the claim never got to run, and the test proved nothing.

    Here the claim runs in its OWN thread with its own connection, and the proof
    is taken from PostgreSQL itself: while the retirement holds SHARE and is
    removing the container, `pg_locks` must show the claim's lock request
    ungranted. Only then is "the claim cannot slip in" a fact rather than a
    scheduling accident.
    """
    async with session_maker() as seeder:
        await _seed_event(seeder, 4)

    update_started = threading.Event()
    claim_finished = threading.Event()
    observations: dict[str, object] = {}

    def competing_claim() -> None:
        """An independent client: own thread, own loop, own connection."""

        async def run() -> None:
            async with session_maker() as claimer:
                await claimer.execute(text("SET LOCAL lock_timeout = 30000"))
                update_started.set()
                # Blocks here for as long as the barrier holds SHARE.
                await claimer.execute(text("UPDATE whatsapp_events SET status = 'processing' WHERE id = 4"))
                await claimer.commit()

        asyncio.run(run())
        claim_finished.set()

    def observe_while_removing() -> None:
        """Runs inside the retirement's `remove`, i.e. under the held lock.

        The claim is started HERE, so the barrier is provably already held when
        its UPDATE is issued — otherwise the claim could simply win the race to
        ROW EXCLUSIVE and the test would prove nothing about the lock.
        """
        claim_thread.start()
        assert update_started.wait(timeout=10), "the competing claim never issued its UPDATE"

        async def blocked_requests() -> int:
            async with session_maker() as probe:
                result = await probe.execute(
                    text(
                        "SELECT count(*) FROM pg_locks l "
                        "JOIN pg_class c ON c.oid = l.relation "
                        "WHERE c.relname = 'whatsapp_events' AND NOT l.granted"
                    )
                )
                return int(result.scalar_one())

        # Give the UPDATE a moment to reach the lock queue, then prove it is there.
        deadline = time.monotonic() + 10
        waiting = 0
        while time.monotonic() < deadline:
            waiting = asyncio.run(blocked_requests())
            if waiting:
                break
            time.sleep(0.05)

        observations["ungranted_lock_requests"] = waiting
        observations["claim_finished_during_removal"] = claim_finished.is_set()

    claim_thread = threading.Thread(target=competing_claim)

    docker = FakeDocker(on_remove=observe_while_removing)

    outcome = await retire_with_barrier(session_maker, container="legacy-cid", docker=docker, lock_timeout_ms=30_000)
    claim_thread.join(timeout=30)

    assert outcome.retired is True
    assert observations["ungranted_lock_requests"], (
        "the competing UPDATE was not actually waiting on whatsapp_events while the barrier was held"
    )
    assert observations["claim_finished_during_removal"] is False, (
        "the claim committed while the container was being removed — the barrier did not hold"
    )
    assert "remove" in docker.calls
    assert not claim_thread.is_alive(), "the claim proceeds once the barrier is released"
    assert await _processing_count(session_maker) == 1, "and then completes normally"


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

    docker = FakeDocker(presence=Presence.ABSENT)

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


# ---------------------------------------------------------------------------
# Identity: the helper resolves its own target and proves what it is
# ---------------------------------------------------------------------------


class IdentityDocker(FakeDocker):
    """Answers the identity probes the resolver makes."""

    def __init__(self, *, fields: dict[str, str | None] | None = None, graceful: bool | None = False, **kw) -> None:
        super().__init__(**kw)
        self.fields = {
            '{{index .Config.Labels "com.docker.compose.project"}}': "altegio_bot",
            '{{index .Config.Labels "com.docker.compose.service"}}': "altegio-whatsapp-inbox-worker",
            '{{index .Config.Labels "com.docker.compose.oneoff"}}': "False",
            "{{.State.Status}}": "running",
        }
        if fields:
            self.fields.update(fields)
        self._graceful = graceful

    def inspect_field(self, container: str, fmt: str) -> str | None:
        return self.fields.get(fmt)

    def supports_graceful_shutdown(self, container: str) -> bool | None:
        return self._graceful


def test_a_correctly_identified_legacy_worker_is_accepted() -> None:
    assert retire_module.resolve_legacy_container(IdentityDocker()) == "legacy-cid"


@pytest.mark.parametrize("label", ["False", "false", "<no value>", ""])
def test_the_one_off_label_is_case_normalised(label: str) -> None:
    """Compose v5.3.1 writes `False`; earlier versions write `false`."""
    docker = IdentityDocker(fields={'{{index .Config.Labels "com.docker.compose.oneoff"}}': label})

    assert retire_module.resolve_legacy_container(docker) == "legacy-cid"


@pytest.mark.parametrize("label", ["True", "true"])
def test_a_one_off_container_is_refused_in_either_casing(label: str) -> None:
    docker = IdentityDocker(fields={'{{index .Config.Labels "com.docker.compose.oneoff"}}': label})

    with pytest.raises(LegacyRetirementError, match="one-off"):
        retire_module.resolve_legacy_container(docker)


def test_python_and_shell_agree_on_the_one_off_label() -> None:
    """Both resolvers must read the same production label the same way."""
    for value in ("False", "false", "<no value>", ""):
        assert retire_module.oneoff_label_is_service_container(value)
        assert not retire_module.oneoff_label_is_one_shot(value)
    for value in ("True", "true"):
        assert retire_module.oneoff_label_is_one_shot(value)
        assert not retire_module.oneoff_label_is_service_container(value)
    for value in ("maybe", "1", None):
        assert not retire_module.oneoff_label_is_service_container(value)
        assert not retire_module.oneoff_label_is_one_shot(value)

    shell_lib = (Path(__file__).resolve().parents[3] / "scripts/lib/whatsapp_drain.sh").read_text()
    assert "tr '[:upper:]' '[:lower:]'" in shell_lib, "the shell resolver must case-fold too"


@pytest.mark.parametrize(
    ("label", "fields", "graceful", "match"),
    [
        (
            "other-service",
            {'{{index .Config.Labels "com.docker.compose.service"}}': "altegio-outbox-worker"},
            False,
            "service",
        ),
        ("other-project", {'{{index .Config.Labels "com.docker.compose.project"}}': "other_stack"}, False, "project"),
        ("unreadable-labels", {'{{index .Config.Labels "com.docker.compose.project"}}': None}, False, "cannot inspect"),
        ("unreadable-oneoff", {'{{index .Config.Labels "com.docker.compose.oneoff"}}': None}, False, "one-off label"),
        ("not-running", {"{{.State.Status}}": "exited"}, False, "not 'running'"),
        ("unreadable-state", {"{{.State.Status}}": None}, False, "cannot read the state"),
    ],
)
def test_every_identity_mismatch_refuses(label: str, fields: dict, graceful: bool, match: str) -> None:
    docker = IdentityDocker(fields=fields, graceful=graceful)

    with pytest.raises(LegacyRetirementError, match=match):
        retire_module.resolve_legacy_container(docker)
    assert docker.calls == [], f"{label}: nothing may be touched before identity is proven"


def test_a_graceful_worker_is_sent_to_the_ordinary_deploy_path() -> None:
    """This helper exists only for images without the SIGTERM contract."""
    with pytest.raises(LegacyRetirementError, match="graceful"):
        retire_module.resolve_legacy_container(IdentityDocker(graceful=True))


def test_an_unprobeable_capability_refuses() -> None:
    with pytest.raises(LegacyRetirementError, match="capability"):
        retire_module.resolve_legacy_container(IdentityDocker(graceful=None))


@pytest.mark.parametrize(
    ("label", "kwargs", "match"),
    [
        ("discovery-failure", {"discovery_fails": True}, "discovery failed"),
        ("no-container", {"container_ids": []}, "no altegio-whatsapp-inbox-worker container"),
        ("two-containers", {"container_ids": ["a", "b"]}, "exactly one"),
    ],
)
def test_topology_must_be_unambiguous(label: str, kwargs: dict, match: str) -> None:
    with pytest.raises(LegacyRetirementError, match=match):
        retire_module.resolve_legacy_container(IdentityDocker(**kwargs))


async def test_a_replica_appearing_after_the_freeze_blocks_the_retirement(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """A second worker started between resolve and pause would keep claiming."""

    class ReplicaAppears(FakeDocker):
        def service_container_ids(self) -> list[str] | None:
            return ["legacy-cid", "surprise-cid"]

    docker = ReplicaAppears()

    with pytest.raises(LegacyRetirementError, match="changed after the freeze"):
        await retire_with_barrier(session_maker, container="legacy-cid", docker=docker, lock_timeout_ms=5_000)

    assert "remove" not in docker.calls
    assert docker.calls == ["pause", "unpause"], "the frozen worker is released again"


async def test_an_unreadable_container_after_a_failed_removal_is_reported(
    session_maker: async_sessionmaker[AsyncSession],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A Docker failure must never be reported as "already gone"."""

    class UnreadableAfterRemove(FakeDocker):
        def remove(self, container: str) -> None:
            self.calls.append("remove")
            self._presence = Presence.UNREADABLE
            raise LegacyRetirementError("docker rm failed")

    docker = UnreadableAfterRemove()

    with caplog.at_level("ERROR"):
        with pytest.raises(LegacyRetirementError):
            await retire_with_barrier(session_maker, container="legacy-cid", docker=docker, lock_timeout_ms=5_000)

    assert "may still be PAUSED" in caplog.text
    assert "unpause" not in docker.calls, "an unreadable container must not be blindly unpaused"


def test_the_probe_is_read_only() -> None:
    """It must answer the runtime questions without touching anything."""
    docker = IdentityDocker()
    report = retire_module.probe_ops_runtime(docker)

    assert report["docker_daemon"] is True
    assert report["whatsapp_worker_containers"] == 1
    assert docker.calls == [], "the probe must not pause, unpause or remove"


def test_the_cli_never_accepts_a_container_id_as_the_target() -> None:
    """A mistyped id must not be able to select what gets paused and removed."""
    source = inspect.getsource(retire_module.main)

    assert "resolve_legacy_container(docker)" in source
    assert "args.container or" not in source, "the resolver must not be bypassable"
    assert "--expect-container" in source
    assert "does not match --expect-container" in source
