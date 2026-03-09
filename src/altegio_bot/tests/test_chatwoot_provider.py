"""Unit tests for ChatwootHybridProvider."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from altegio_bot.providers.chatwoot_hybrid import ChatwootHybridProvider


class _FakeMetaProvider:
    """Stub MetaCloudProvider that records calls."""

    def __init__(self, raise_on_send: bool = False) -> None:
        self.sent: list[tuple[int, str, str]] = []
        self.templates: list[tuple] = []
        self._raise = raise_on_send

    async def send(self, sender_id: int, phone_e164: str, text: str) -> str:
        if self._raise:
            raise RuntimeError("Meta API failure")
        self.sent.append((sender_id, phone_e164, text))
        return f"meta-{uuid4()}"

    async def send_template(
        self,
        sender_id: int,
        phone_e164: str,
        template_name: str,
        language: str,
        params: list[str],
        fallback_text: str = "",
    ) -> str:
        self.templates.append((sender_id, phone_e164, template_name, language, params))
        return f"meta-tpl-{uuid4()}"


class _FakeChatwootClient:
    """Stub ChatwootClient that records calls."""

    def __init__(self, raise_on_log: bool = False) -> None:
        self.logged: list[tuple[int, str]] = []
        self._raise = raise_on_log
        self._contact_id = 1
        self._conv_id = 10

    async def get_or_create_contact(self, phone: str, *, name: Any = None) -> int:
        return self._contact_id

    async def get_or_create_conversation(self, contact_id: int) -> int:
        return self._conv_id

    async def send_message(
        self, conv_id: int, content: str, *, message_type: str = "outgoing", private: bool = False
    ) -> int:
        if self._raise:
            raise RuntimeError("Chatwoot API failure")
        self.logged.append((conv_id, content))
        return 999

    async def aclose(self) -> None:
        pass


@pytest.mark.asyncio
async def test_send_delegates_to_primary() -> None:
    """Hybrid provider must call the primary Meta provider."""
    meta = _FakeMetaProvider()
    cw = _FakeChatwootClient()
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    msg_id = await provider.send(1, "+49123", "Hello")
    assert msg_id.startswith("meta-")
    assert len(meta.sent) == 1
    # Allow the fire-and-forget task to run
    await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_send_fails_if_primary_fails() -> None:
    """If primary fails, the error must propagate."""
    meta = _FakeMetaProvider(raise_on_send=True)
    cw = _FakeChatwootClient()
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="Meta API failure"):
        await provider.send(1, "+49123", "Hello")


@pytest.mark.asyncio
async def test_send_continues_if_chatwoot_fails() -> None:
    """If Chatwoot logging fails, the message must still succeed."""
    meta = _FakeMetaProvider()
    cw = _FakeChatwootClient(raise_on_log=True)
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    msg_id = await provider.send(1, "+49123", "Hello")
    assert msg_id.startswith("meta-")
    # Give the fire-and-forget task a moment to complete (and swallow the error)
    await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_send_template_delegates_to_primary() -> None:
    """send_template must use the primary provider."""
    meta = _FakeMetaProvider()
    cw = _FakeChatwootClient()
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    msg_id = await provider.send_template(1, "+49123", "my_tpl", "de", ["p1", "p2"])
    assert msg_id.startswith("meta-tpl-")
    assert len(meta.templates) == 1
    await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_aclose_calls_both() -> None:
    """aclose must close both primary and chatwoot client."""
    meta = _FakeMetaProvider()
    closed_meta = False

    async def _aclose() -> None:
        nonlocal closed_meta
        closed_meta = True

    meta.aclose = _aclose  # type: ignore[method-assign]
    cw = _FakeChatwootClient()
    closed_cw = False

    async def _cw_aclose() -> None:
        nonlocal closed_cw
        closed_cw = True

    cw.aclose = _cw_aclose  # type: ignore[method-assign]
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]
    await provider.aclose()
    assert closed_meta
    assert closed_cw
