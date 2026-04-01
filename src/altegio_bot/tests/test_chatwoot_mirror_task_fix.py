"""Tests verifying that ChatwootHybridProvider uses tracked tasks, not orphan ensure_future."""

from __future__ import annotations

import asyncio
import inspect
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
    """After send(), a task must appear in _background_tasks (not an orphan)."""
    meta = _FakeMeta()
    cw = _FakeChatwoot()
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    await provider.send(1, "+49100000001", "hello")

    # The task is scheduled but may not have finished yet; it must be tracked.
    assert len(provider._background_tasks) <= 1
    await asyncio.sleep(0.05)
    # After it finishes, discard callback removes it from the set.
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
    """ensure_future must not be used for mirror tasks in ChatwootHybridProvider."""
    import altegio_bot.providers.chatwoot_hybrid as _mod

    source = inspect.getsource(_mod)
    # asyncio.ensure_future should not appear in mirror-scheduling code paths.
    assert "ensure_future" not in source, "ensure_future found – orphan task pattern must not be used"


async def test_safe_send_passes_mirror_kwargs() -> None:
    """safe_send must forward company_id / staff_id when provider supports them."""
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
