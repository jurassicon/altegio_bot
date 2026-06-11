"""Provider-level tests for the WhatsApp reply-context payload (PR2).

These tests assert the ACTUAL JSON payload sent to the Meta Cloud API (not just
that ``safe_send`` was called), plus the optional ``reply_to_provider_message_id``
pass-through across the provider layers.  No real HTTP request is made: the
provider's ``httpx.AsyncClient`` is replaced with a capturing fake.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from altegio_bot.providers.dummy import DummyProvider, safe_send
from altegio_bot.providers.meta_cloud import MetaCloudProvider


class _FakeResp:
    status_code = 200

    def __init__(self, wamid: str = "wamid.SENT") -> None:
        self._wamid = wamid

    def json(self) -> dict[str, Any]:
        return {"messages": [{"id": self._wamid}]}


def _meta_provider_with_capture(captured_payloads: list[dict[str, Any]]) -> MetaCloudProvider:
    """Build a MetaCloudProvider whose HTTP client captures the JSON payload.

    Real send is enabled but no network call happens — the fake client records
    the payload and returns a canned Meta response with a NEW sent wamid.
    """
    provider = MetaCloudProvider.__new__(MetaCloudProvider)
    provider._access_token = "test-token"
    provider._api_version = "v21.0"
    provider._graph_url = "https://graph.facebook.com"
    provider._allow_real_send = True
    provider._sender_cache = {1: "12345678901"}

    class _FakeClient:
        async def post(self, url: str, headers: dict[str, str], json: dict[str, Any]) -> _FakeResp:
            captured_payloads.append(json)
            return _FakeResp()

    provider._client = _FakeClient()  # type: ignore[assignment]
    return provider


# ---------------------------------------------------------------------------
# MetaCloudProvider.send – actual Meta JSON payload
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_meta_cloud_send_without_reply_context_omits_context() -> None:
    """No reply context → payload has no 'context'; plain text send preserved."""
    captured_payloads: list[dict[str, Any]] = []
    provider = _meta_provider_with_capture(captured_payloads)

    msg_id = await provider.send(1, "+491234567890", "Hello")

    assert msg_id == "wamid.SENT"
    assert len(captured_payloads) == 1
    payload = captured_payloads[0]
    assert "context" not in payload
    # Old text-send behavior preserved.
    assert payload["messaging_product"] == "whatsapp"
    assert payload["type"] == "text"
    assert payload["to"] == "491234567890"
    assert payload["text"]["body"] == "Hello"
    assert payload["text"]["preview_url"] is False


@pytest.mark.asyncio
async def test_meta_cloud_send_with_reply_context_adds_context_message_id() -> None:
    """Reply context → payload carries context.message_id == TARGET wamid."""
    captured_payloads: list[dict[str, Any]] = []
    provider = _meta_provider_with_capture(captured_payloads)

    msg_id = await provider.send(
        1,
        "+491234567890",
        "Hello back",
        reply_to_provider_message_id="wamid.TARGET",
    )

    # The returned id is the NEW sent message id from the Meta response.
    assert msg_id == "wamid.SENT"

    payload = captured_payloads[0]
    assert payload["context"] == {"message_id": "wamid.TARGET"}
    # context.message_id must be the replied-to target, NOT the new sent wamid.
    assert payload["context"]["message_id"] != msg_id
    # Full text-send shape stays intact.
    assert payload["messaging_product"] == "whatsapp"
    assert payload["type"] == "text"
    assert payload["to"] == "491234567890"
    assert payload["text"]["body"] == "Hello back"
    assert payload["text"]["preview_url"] is False


@pytest.mark.asyncio
async def test_meta_cloud_send_strips_whitespace_reply_context_into_context() -> None:
    """A padded wamid is trimmed before being placed in context.message_id."""
    captured_payloads: list[dict[str, Any]] = []
    provider = _meta_provider_with_capture(captured_payloads)

    await provider.send(
        1,
        "+491234567890",
        "Hi",
        reply_to_provider_message_id="  wamid.PADDED  ",
    )

    assert captured_payloads[0]["context"] == {"message_id": "wamid.PADDED"}


@pytest.mark.parametrize("blank", ["", "   "])
@pytest.mark.asyncio
async def test_meta_cloud_send_blank_reply_context_omits_context(blank: str) -> None:
    """Empty/whitespace reply context → no 'context'; plain send still works."""
    captured_payloads: list[dict[str, Any]] = []
    provider = _meta_provider_with_capture(captured_payloads)

    msg_id = await provider.send(
        1,
        "+491234567890",
        "Hello",
        reply_to_provider_message_id=blank,
    )

    assert msg_id == "wamid.SENT"
    payload = captured_payloads[0]
    assert "context" not in payload
    assert payload["text"]["body"] == "Hello"


# ---------------------------------------------------------------------------
# safe_send – optional kwarg pass-through
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_safe_send_passes_reply_context_when_present() -> None:
    captured: dict[str, Any] = {}

    class _CaptureProvider:
        async def send(
            self,
            sender_id: int,
            phone_e164: str,
            text: str,
            *,
            contact_name: str | None = None,
            reply_to_provider_message_id: str | None = None,
        ) -> str:
            captured["sender_id"] = sender_id
            captured["phone_e164"] = phone_e164
            captured["text"] = text
            captured["reply_to_provider_message_id"] = reply_to_provider_message_id
            return "wamid.NEW"

    msg_id, err = await safe_send(
        _CaptureProvider(),  # type: ignore[arg-type]
        sender_id=1,
        phone="+491234567890",
        text="Hello",
        reply_to_provider_message_id="wamid.TARGET",
    )

    assert err is None
    assert msg_id == "wamid.NEW"
    assert captured["reply_to_provider_message_id"] == "wamid.TARGET"


@pytest.mark.asyncio
async def test_safe_send_blank_reply_context_not_forwarded() -> None:
    """Whitespace-only reply context must NOT be forwarded as a kwarg."""
    captured: dict[str, Any] = {}

    class _CaptureProvider:
        async def send(
            self,
            sender_id: int,
            phone_e164: str,
            text: str,
            *,
            contact_name: str | None = None,
            reply_to_provider_message_id: str | None = None,
        ) -> str:
            captured["reply_to_provider_message_id"] = reply_to_provider_message_id
            return "wamid.NEW"

    msg_id, err = await safe_send(
        _CaptureProvider(),  # type: ignore[arg-type]
        sender_id=1,
        phone="+491234567890",
        text="Hello",
        reply_to_provider_message_id="   ",
    )

    assert err is None
    assert msg_id == "wamid.NEW"
    # Default None — the blank value was dropped by safe_send.
    assert captured["reply_to_provider_message_id"] is None


@pytest.mark.asyncio
async def test_safe_send_old_callers_still_work_without_reply_context() -> None:
    captured: dict[str, Any] = {}

    class _CaptureProvider:
        async def send(
            self,
            sender_id: int,
            phone_e164: str,
            text: str,
            *,
            contact_name: str | None = None,
        ) -> str:
            captured["sender_id"] = sender_id
            captured["phone_e164"] = phone_e164
            captured["text"] = text
            captured["contact_name"] = contact_name
            return "wamid.PLAIN"

    msg_id, err = await safe_send(
        _CaptureProvider(),  # type: ignore[arg-type]
        sender_id=1,
        phone="+491234567890",
        text="Hello",
    )

    assert err is None
    assert msg_id == "wamid.PLAIN"
    assert captured["text"] == "Hello"


# ---------------------------------------------------------------------------
# DummyProvider – accepts the optional kwarg
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dummy_provider_accepts_reply_context(caplog: pytest.LogCaptureFixture) -> None:
    """DummyProvider.send accepts reply context and records it in its debug log."""
    provider = DummyProvider()

    with caplog.at_level(logging.INFO, logger="altegio_bot.providers.dummy"):
        msg_id = await provider.send(
            1,
            "+491234567890",
            "Hello",
            reply_to_provider_message_id="wamid.TARGET",
        )

    assert msg_id.startswith("dummy-")
    assert "reply_context=True" in caplog.text


@pytest.mark.asyncio
async def test_dummy_provider_without_reply_context() -> None:
    """DummyProvider.send still works with no reply context."""
    provider = DummyProvider()
    msg_id = await provider.send(1, "+491234567890", "Hello")
    assert msg_id.startswith("dummy-")


# ---------------------------------------------------------------------------
# ChatwootHybridProvider – primary gets reply context, mirror does NOT
# ---------------------------------------------------------------------------


def _hybrid_with_fakes(primary_calls: list[dict[str, Any]], mirror_calls: list[dict[str, Any]]):
    from altegio_bot.providers.chatwoot_hybrid import ChatwootHybridProvider

    class _FakePrimary:
        async def send(
            self,
            sender_id: int,
            phone_e164: str,
            text: str,
            *,
            reply_to_provider_message_id: str | None = None,
        ) -> str:
            primary_calls.append(
                {
                    "phone_e164": phone_e164,
                    "text": text,
                    "reply_to_provider_message_id": reply_to_provider_message_id,
                }
            )
            return "wamid.PRIMARY"

    class _FakeChatwoot:
        async def mirror_outbound_as_note(
            self,
            phone_e164: str,
            content: str,
            *,
            contact_name: str | None = None,
        ) -> None:
            mirror_calls.append(
                {
                    "phone_e164": phone_e164,
                    "content": content,
                    "contact_name": contact_name,
                }
            )

        async def aclose(self) -> None:
            return None

    provider = ChatwootHybridProvider(
        primary=_FakePrimary(),  # type: ignore[arg-type]
        chatwoot=_FakeChatwoot(),  # type: ignore[arg-type]
    )
    return provider


@pytest.mark.asyncio
async def test_hybrid_forwards_reply_context_to_primary_only() -> None:
    """Reply context reaches the Meta primary, never the Chatwoot mirror path."""
    primary_calls: list[dict[str, Any]] = []
    mirror_calls: list[dict[str, Any]] = []
    provider = _hybrid_with_fakes(primary_calls, mirror_calls)

    msg_id = await provider.send(
        1,
        "+491234567890",
        "Reply text",
        reply_to_provider_message_id="wamid.TARGET",
    )
    # Flush the best-effort background mirror task.
    await provider.aclose()

    assert msg_id == "wamid.PRIMARY"
    assert len(primary_calls) == 1
    assert primary_calls[0]["reply_to_provider_message_id"] == "wamid.TARGET"
    assert primary_calls[0]["text"] == "Reply text"

    # Mirror ran, but its arguments never include reply context — the private
    # note is plain content only.
    assert len(mirror_calls) == 1
    assert "reply_to_provider_message_id" not in mirror_calls[0]
    assert mirror_calls[0]["content"] == "Reply text"


@pytest.mark.asyncio
async def test_hybrid_without_reply_context_unchanged() -> None:
    """No reply context → primary gets None, mirror behavior unchanged."""
    primary_calls: list[dict[str, Any]] = []
    mirror_calls: list[dict[str, Any]] = []
    provider = _hybrid_with_fakes(primary_calls, mirror_calls)

    msg_id = await provider.send(1, "+491234567890", "Plain")
    await provider.aclose()

    assert msg_id == "wamid.PRIMARY"
    assert primary_calls[0]["reply_to_provider_message_id"] is None
    assert len(mirror_calls) == 1
    assert mirror_calls[0]["content"] == "Plain"
