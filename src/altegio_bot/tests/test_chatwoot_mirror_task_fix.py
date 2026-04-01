"""Tests verifying that ChatwootHybridProvider uses tracked tasks, not orphan ensure_future."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from altegio_bot.providers.chatwoot_hybrid import ChatwootHybridProvider
from altegio_bot.providers.dummy import _supports_mirror, safe_send, safe_send_template

# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


class _FakeMeta:
    async def send(self, sender_id: int, phone_e164: str, text: str, **_kwargs: object) -> str:
        return f"meta-{uuid4()}"

    async def send_template(
        self,
        sender_id: int,
        phone_e164: str,
        template_name: str,
        language: str,
        params: list[str],
        fallback_text: str = "",
        **_kwargs: object,
    ) -> str:
        return f"meta-tpl-{uuid4()}"


class _FakeChatwoot:
    def __init__(self) -> None:
        self.notes: list[tuple[str, str]] = []

    async def mirror_outbound_as_note(self, phone_e164: str, text: str, *, contact_name: str | None = None) -> None:
        self.notes.append((phone_e164, text))

    async def aclose(self) -> None:
        pass


class _SlowChatwoot:
    """Chatwoot client whose mirror call takes longer than the aclose timeout."""

    def __init__(self, delay: float = 5.0) -> None:
        self._delay = delay
        self.notes: list[tuple[str, str]] = []

    async def mirror_outbound_as_note(self, phone_e164: str, text: str, *, contact_name: str | None = None) -> None:
        await asyncio.sleep(self._delay)
        self.notes.append((phone_e164, text))

    async def aclose(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_send_creates_tracked_task_not_orphan() -> None:
    """After send(), a task must appear in _background_tasks immediately (not an orphan)."""
    meta = _FakeMeta()
    cw = _FakeChatwoot()
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    await provider.send(1, "+49100000001", "hello")

    # The task must be in the set right after send() returns, before it runs.
    assert len(provider._background_tasks) == 1, "task was not added to _background_tasks"
    task = next(iter(provider._background_tasks))
    assert isinstance(task, asyncio.Task), "tracked item must be an asyncio.Task, not a bare Future/coroutine"

    # Let the task run; the discard callback removes it from the set.
    await asyncio.sleep(0.05)
    assert len(provider._background_tasks) == 0
    assert len(cw.notes) == 1


async def test_send_template_creates_tracked_task() -> None:
    """After send_template(), the mirror task must be tracked in _background_tasks."""
    meta = _FakeMeta()
    cw = _FakeChatwoot()
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    await provider.send_template(1, "+49100000002", "tpl", "de", ["p1"])

    await asyncio.sleep(0.05)
    assert len(provider._background_tasks) == 0
    assert len(cw.notes) == 1


async def test_aclose_waits_for_background_tasks() -> None:
    """aclose() must await pending background mirror tasks."""
    meta = _FakeMeta()
    cw = _FakeChatwoot()
    # Inject a slight delay so the task is still pending when aclose is called.
    original_mirror = cw.mirror_outbound_as_note
    completed: list[bool] = []

    async def _slow_mirror(phone: str, text: str, *, contact_name: str | None = None) -> None:
        await asyncio.sleep(0.05)
        await original_mirror(phone, text, contact_name=contact_name)
        completed.append(True)

    cw.mirror_outbound_as_note = _slow_mirror  # type: ignore[method-assign]

    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]
    await provider.send(1, "+49100000003", "wait for me")

    # aclose should block until the task finishes (within 3 s timeout).
    await provider.aclose()
    assert completed == [True]


async def test_aclose_logs_warning_when_tasks_timeout(caplog: pytest.LogCaptureFixture) -> None:
    """aclose() must emit a warning when background tasks exceed the timeout."""
    meta = _FakeMeta()
    cw = _SlowChatwoot(delay=10.0)  # will never finish within 3 s timeout
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    # Monkeypatch the timeout constant so the test runs quickly.
    import altegio_bot.providers.chatwoot_hybrid as _mod

    original_timeout = _mod._ACLOSE_TIMEOUT
    _mod._ACLOSE_TIMEOUT = 0.05  # type: ignore[assignment]
    try:
        await provider.send(1, "+49100000004", "slow")
        with caplog.at_level("WARNING", logger="altegio_bot.providers.chatwoot_hybrid"):
            await provider.aclose()
    finally:
        _mod._ACLOSE_TIMEOUT = original_timeout  # type: ignore[assignment]

    assert any("did not finish" in r.message for r in caplog.records)


async def test_aclose_cancels_pending_tasks_before_closing_client() -> None:
    """After the aclose() timeout, pending tasks must be cancelled before the client is closed.

    This prevents tasks that were still sleeping from waking up and calling into
    an already-closed ChatwootClient instance.
    """
    cancelled_after_close: list[bool] = []

    class _TrackingChatwoot:
        def __init__(self) -> None:
            self.closed = False

        async def mirror_outbound_as_note(self, phone_e164: str, text: str, *, contact_name: str | None = None) -> None:
            # Sleep longer than the patched timeout so the task is pending at aclose().
            await asyncio.sleep(10.0)
            # If we ever reach here AFTER close, that's the bug we're guarding against.
            if self.closed:
                cancelled_after_close.append(True)

        async def aclose(self) -> None:
            self.closed = True

    meta = _FakeMeta()
    cw = _TrackingChatwoot()
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    import altegio_bot.providers.chatwoot_hybrid as _mod

    original_timeout = _mod._ACLOSE_TIMEOUT
    _mod._ACLOSE_TIMEOUT = 0.05  # type: ignore[assignment]
    try:
        await provider.send(1, "+49100000020", "cancel me")
        # Grab the task reference before aclose() discards it.
        assert len(provider._background_tasks) == 1
        task = next(iter(provider._background_tasks))

        await provider.aclose()
    finally:
        _mod._ACLOSE_TIMEOUT = original_timeout  # type: ignore[assignment]

    # The task must have been cancelled, not left running.
    assert task.cancelled(), "pending task was not cancelled by aclose()"
    # The chatwoot client must be closed.
    assert cw.closed
    # The running task must never have used the client after it was closed.
    assert cancelled_after_close == []


async def test_mirror_note_is_actually_posted() -> None:
    """End-to-end: send() → await task → mirror_outbound_as_note was called."""
    meta = _FakeMeta()
    cw = _FakeChatwoot()
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    await provider.send(1, "+49100000005", "end to end", contact_name="Test User")
    # Drain all pending tasks so the mirror coroutine runs.
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert len(cw.notes) == 1
    phone, text = cw.notes[0]
    assert phone == "+49100000005"
    assert text == "end to end"


def test_orphan_task_pattern_is_not_used() -> None:
    """_background_tasks must hold proper asyncio.Task objects, not bare Futures or coroutines.

    This is a behavioral check: it verifies that _schedule_mirror stores the result
    of asyncio.create_task() (which returns an asyncio.Task), not a raw coroutine or
    a Future produced by the older asyncio.ensure_future() pattern.
    """

    # We need a running event loop to create tasks; use asyncio.run with a sync wrapper.
    async def _check() -> None:
        meta = _FakeMeta()
        cw = _FakeChatwoot()
        provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]
        await provider.send(1, "+49100000099", "type check")
        # Grab the live task before it finishes.
        assert provider._background_tasks, "expected at least one tracked task"
        for item in provider._background_tasks:
            assert isinstance(item, asyncio.Task), f"item in _background_tasks must be asyncio.Task, got {type(item)}"
        await asyncio.sleep(0.05)  # let task finish cleanly

    asyncio.run(_check())


async def test_mirror_task_exception_does_not_crash_send() -> None:
    """An exception inside the mirror coroutine must not propagate to the caller of send()."""
    meta = _FakeMeta()

    class _BrokenChatwoot:
        async def mirror_outbound_as_note(self, phone_e164: str, text: str, *, contact_name: str | None = None) -> None:
            raise RuntimeError("Chatwoot is down")

        async def aclose(self) -> None:
            pass

    provider = ChatwootHybridProvider(primary=meta, chatwoot=_BrokenChatwoot())  # type: ignore[arg-type]

    # send() itself must succeed even though the mirror will raise.
    msg_id = await provider.send(1, "+49100000010", "should not crash")
    assert msg_id.startswith("meta-")

    # Let the mirror task run (and swallow its error).
    await asyncio.sleep(0.05)
    assert len(provider._background_tasks) == 0


async def test_realistic_shutdown_flow() -> None:
    """Realistic scenario: multiple sends, then aclose() — all mirrors must be posted."""
    meta = _FakeMeta()
    cw = _FakeChatwoot()

    # Give mirror calls a small delay to simulate network latency.
    original_mirror = cw.mirror_outbound_as_note

    async def _latent_mirror(phone: str, text: str, *, contact_name: str | None = None) -> None:
        await asyncio.sleep(0.02)
        await original_mirror(phone, text, contact_name=contact_name)

    cw.mirror_outbound_as_note = _latent_mirror  # type: ignore[method-assign]

    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    phones = [f"+4910000001{i}" for i in range(5)]
    for phone in phones:
        await provider.send(1, phone, f"msg to {phone}")

    # All five tasks should be in-flight right now.
    assert len(provider._background_tasks) == 5

    # aclose() must wait for all of them.
    await provider.aclose()

    assert len(cw.notes) == 5
    noted_phones = {note[0] for note in cw.notes}
    assert noted_phones == set(phones)


async def test_safe_send_passes_mirror_kwargs() -> None:
    received: dict[str, object] = {}

    class _CaptureMeta:
        _supports_mirror_kwargs = True

        async def send(
            self,
            sender_id: int,
            phone_e164: str,
            text: str,
            *,
            company_id: int = 0,
            staff_id: int | None = None,
            contact_name: str | None = None,
        ) -> str:
            received["company_id"] = company_id
            received["staff_id"] = staff_id
            return "fake-id"

    provider = _CaptureMeta()
    assert _supports_mirror(provider)  # type: ignore[arg-type]

    import os

    os.environ["WHATSAPP_PROVIDER"] = "capture_meta"
    os.environ["ALLOW_REAL_SEND"] = "1"
    try:
        msg_id, err = await safe_send(
            provider,  # type: ignore[arg-type]
            sender_id=1,
            phone="+49100000006",
            text="test",
            company_id=42,
            staff_id=7,
        )
    finally:
        del os.environ["WHATSAPP_PROVIDER"]
        del os.environ["ALLOW_REAL_SEND"]

    assert err is None
    assert msg_id == "fake-id"
    assert received["company_id"] == 42
    assert received["staff_id"] == 7


async def test_safe_send_template_passes_mirror_kwargs() -> None:
    """safe_send_template must forward company_id / staff_id when provider supports them."""
    received: dict[str, object] = {}

    class _CaptureMeta:
        _supports_mirror_kwargs = True

        async def send_template(
            self,
            sender_id: int,
            phone_e164: str,
            template_name: str,
            language: str,
            params: list[str],
            fallback_text: str = "",
            *,
            company_id: int = 0,
            staff_id: int | None = None,
            contact_name: str | None = None,
        ) -> str:
            received["company_id"] = company_id
            received["staff_id"] = staff_id
            return "fake-tpl-id"

    provider = _CaptureMeta()

    import os

    os.environ["WHATSAPP_PROVIDER"] = "capture_meta"
    os.environ["ALLOW_REAL_SEND"] = "1"
    try:
        msg_id, err = await safe_send_template(
            provider,  # type: ignore[arg-type]
            sender_id=1,
            phone="+49100000007",
            template_name="tpl",
            language="de",
            params=["p1"],
            company_id=99,
            staff_id=3,
        )
    finally:
        del os.environ["WHATSAPP_PROVIDER"]
        del os.environ["ALLOW_REAL_SEND"]

    assert err is None
    assert msg_id == "fake-tpl-id"
    assert received["company_id"] == 99
    assert received["staff_id"] == 3
