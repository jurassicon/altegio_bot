"""Unit tests for ChatwootClient."""

from __future__ import annotations

import json
import re
from urllib.parse import unquote

import httpx
import pytest
import respx

from altegio_bot.chatwoot_client import (
    WA_CLICK_TO_CHAT_MAX_URL_CHARS,
    ChatwootClient,
    append_wa_deeplink,
    build_wa_click_to_chat_url,
    outbound_mirror_content_attributes,
)

# ---------------------------------------------------------------------------
# append_wa_deeplink – unit tests
# ---------------------------------------------------------------------------


def test_append_wa_deeplink_standard_phone() -> None:
    result = append_wa_deeplink("Hello", "+4917630316130")
    assert result == "Hello\n\n---\n\U0001f4ac Написать в WhatsApp: https://wa.me/4917630316130"


def test_append_wa_deeplink_normalises_messy_phone() -> None:
    result = append_wa_deeplink("Hi", "+49 (176) 303-16130")
    assert "https://wa.me/4917630316130" in result


def test_append_wa_deeplink_idempotent() -> None:
    first = append_wa_deeplink("Hi", "+4917630316130")
    second = append_wa_deeplink(first, "+4917630316130")
    assert first == second


def test_append_wa_deeplink_none_phone() -> None:
    assert append_wa_deeplink("Hi", None) == "Hi"


def test_append_wa_deeplink_empty_phone() -> None:
    assert append_wa_deeplink("Hi", "") == "Hi"


def test_append_wa_deeplink_no_digits_in_phone() -> None:
    assert append_wa_deeplink("Hi", "+++---") == "Hi"


@pytest.fixture
def client() -> ChatwootClient:
    return ChatwootClient(
        base_url="https://chatwoot.example.com",
        api_token="test-token",
        account_id=1,
        inbox_id=2,
    )


@respx.mock
@pytest.mark.asyncio
async def test_get_or_create_contact_found(client: ChatwootClient) -> None:
    """Should return existing contact id when found by phone."""
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/search").mock(
        return_value=httpx.Response(
            200,
            json={
                "payload": [
                    {"id": 42, "phone_number": "+49123456789", "name": "Test"},
                ]
            },
        )
    )

    cid = await client.get_or_create_contact("+49123456789")
    assert cid == 42


@respx.mock
@pytest.mark.asyncio
async def test_get_or_create_contact_creates_when_not_found(client: ChatwootClient) -> None:
    """Should create a new contact when not found."""
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/search").mock(
        return_value=httpx.Response(200, json={"payload": []})
    )
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/contacts").mock(
        return_value=httpx.Response(200, json={"id": 99, "phone_number": "+49987654321"})
    )

    cid = await client.get_or_create_contact("+49987654321", name="Alice")
    assert cid == 99


@respx.mock
@pytest.mark.asyncio
async def test_get_or_create_conversation_returns_open(client: ChatwootClient) -> None:
    """Should reuse an existing open conversation on the correct inbox."""
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/42/conversations").mock(
        return_value=httpx.Response(
            200,
            json={
                "payload": [
                    {"id": 7, "inbox_id": 2, "status": "open"},
                    {"id": 6, "inbox_id": 2, "status": "resolved"},
                ]
            },
        )
    )

    conv_id = await client.get_or_create_conversation(42)
    assert conv_id == 7


@respx.mock
@pytest.mark.asyncio
async def test_get_or_create_conversation_creates_when_none(
    client: ChatwootClient,
) -> None:
    """Should create a conversation with status=open when none exist."""
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/42/conversations").mock(
        return_value=httpx.Response(200, json={"payload": []})
    )
    create_route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations").mock(
        return_value=httpx.Response(200, json={"id": 15, "status": "open"})
    )

    conv_id = await client.get_or_create_conversation(42)
    assert conv_id == 15

    body = json.loads(create_route.calls[0].request.content)
    assert body["status"] == "open"
    assert body["inbox_id"] == 2
    assert body["contact_id"] == 42


@respx.mock
@pytest.mark.asyncio
async def test_same_contact_uses_separate_conversations_for_different_inboxes() -> None:
    """A shared account-level contact must never reuse another branch's thread."""
    du = ChatwootClient(
        base_url="https://chatwoot.example.com",
        api_token="test-token",
        account_id=1,
        inbox_id=101,
    )
    ra = ChatwootClient(
        base_url="https://chatwoot.example.com",
        api_token="test-token",
        account_id=1,
        inbox_id=102,
    )
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/42/conversations").mock(
        return_value=httpx.Response(
            200,
            json={"payload": [{"id": 7001, "inbox_id": 101, "status": "open"}]},
        )
    )
    create_route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations").mock(
        return_value=httpx.Response(200, json={"id": 7002, "status": "open"})
    )

    try:
        assert await du.get_or_create_conversation(42) == 7001
        assert await ra.get_or_create_conversation(42) == 7002
    finally:
        await du.aclose()
        await ra.aclose()

    assert len(create_route.calls) == 1
    body = json.loads(create_route.calls[0].request.content)
    assert body == {"inbox_id": 102, "contact_id": 42, "status": "open"}


@respx.mock
@pytest.mark.asyncio
async def test_send_message(client: ChatwootClient) -> None:
    """Should post a message and return the message id."""
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(200, json={"id": 101, "content": "Hello"})
    )

    msg_id = await client.send_message(15, "Hello", message_type="outgoing")
    assert msg_id == 101


@respx.mock
@pytest.mark.asyncio
async def test_send_message_includes_content_attributes(client: ChatwootClient) -> None:
    """content_attributes must be forwarded verbatim when provided."""
    route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(200, json={"id": 102, "content": "Reply"})
    )

    attrs = {"in_reply_to": 7644, "in_reply_to_external_id": "wamid.X"}
    msg_id = await client.send_message(
        15,
        "Reply",
        message_type="incoming",
        content_attributes=attrs,
    )
    assert msg_id == 102

    body = json.loads(route.calls[0].request.content)
    assert body["content_attributes"] == attrs
    assert body["message_type"] == "incoming"
    # private must never be sent for incoming messages (Chatwoot 422).
    assert "private" not in body


@respx.mock
@pytest.mark.asyncio
async def test_send_message_without_content_attributes_omits_key(client: ChatwootClient) -> None:
    """The content_attributes key must be absent when not provided."""
    route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(200, json={"id": 103, "content": "Plain"})
    )

    await client.send_message(15, "Plain", message_type="incoming")

    body = json.loads(route.calls[0].request.content)
    assert "content_attributes" not in body
    assert "private" not in body


@respx.mock
@pytest.mark.asyncio
async def test_send_message_outgoing_keeps_private_with_content_attributes(client: ChatwootClient) -> None:
    """For outgoing messages private is still sent alongside content_attributes."""
    route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(200, json={"id": 104, "content": "Note"})
    )

    await client.send_message(
        15,
        "Note",
        message_type="outgoing",
        private=True,
        content_attributes={"in_reply_to": 1},
    )

    body = json.loads(route.calls[0].request.content)
    assert body["private"] is True
    assert body["content_attributes"] == {"in_reply_to": 1}


@respx.mock
@pytest.mark.asyncio
async def test_get_or_create_incoming_conversation_does_not_post_message(
    client: ChatwootClient,
) -> None:
    """Must resolve contact + conversation without posting any message."""
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/search").mock(
        return_value=httpx.Response(200, json={"payload": [{"id": 5, "phone_number": "+49111222333"}]})
    )
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/5/conversations").mock(
        return_value=httpx.Response(200, json={"payload": [{"id": 20, "inbox_id": 2, "status": "open"}]})
    )
    post_route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(200, json={"id": 999})
    )

    conv_id = await client.get_or_create_incoming_conversation("+49111222333")
    assert conv_id == 20
    assert not post_route.called


@respx.mock
@pytest.mark.asyncio
async def test_log_incoming_message_passes_content_attributes(client: ChatwootClient) -> None:
    """log_incoming_message must forward content_attributes to send_message."""
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/search").mock(
        return_value=httpx.Response(200, json={"payload": [{"id": 5, "phone_number": "+49111222333"}]})
    )
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/5/conversations").mock(
        return_value=httpx.Response(200, json={"payload": [{"id": 20, "inbox_id": 2, "status": "open"}]})
    )
    route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(200, json={"id": 201, "content": "Hi"})
    )

    attrs = {"in_reply_to": 7644, "in_reply_to_external_id": "wamid.X"}
    conv_id, msg_id = await client.log_incoming_message(
        "+49111222333",
        "Hi",
        content_attributes=attrs,
    )
    assert (conv_id, msg_id) == (20, 201)

    body = json.loads(route.calls[0].request.content)
    assert body["content_attributes"] == attrs


@respx.mock
@pytest.mark.asyncio
async def test_log_incoming_message(client: ChatwootClient) -> None:
    """log_incoming_message should create contact, conversation, and message."""
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/search").mock(
        return_value=httpx.Response(200, json={"payload": [{"id": 5, "phone_number": "+49111222333"}]})
    )
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/5/conversations").mock(
        return_value=httpx.Response(200, json={"payload": [{"id": 20, "inbox_id": 2, "status": "open"}]})
    )
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(200, json={"id": 200, "content": "Hi"})
    )

    conv_id, msg_id = await client.log_incoming_message("+49111222333", "Hi")
    assert conv_id == 20
    assert msg_id == 200


@respx.mock
@pytest.mark.asyncio
async def test_mirror_outbound_as_note(client: ChatwootClient) -> None:
    """mirror_outbound_as_note should post a private outgoing message."""
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/search").mock(
        return_value=httpx.Response(200, json={"payload": [{"id": 5, "phone_number": "+49111222333"}]})
    )
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/5/conversations").mock(
        return_value=httpx.Response(200, json={"payload": [{"id": 20, "inbox_id": 2, "status": "open"}]})
    )
    # No prior inbound from client.
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(200, json={"payload": []})
    )
    post_mock = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(200, json={"id": 300, "content": "Note"})
    )

    await client.mirror_outbound_as_note("+49111222333", "Note")

    assert post_mock.called
    sent_body = post_mock.calls[0].request.content
    body = json.loads(sent_body)
    assert body["private"] is True
    assert body["message_type"] == "outgoing"


@respx.mock
@pytest.mark.asyncio
async def test_mirror_outbound_as_note_with_contact_name(client: ChatwootClient) -> None:
    """mirror_outbound_as_note with contact_name should pass name to get_or_create_contact."""
    search_mock = respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/search").mock(
        return_value=httpx.Response(200, json={"payload": []})
    )
    create_mock = respx.post("https://chatwoot.example.com/api/v1/accounts/1/contacts").mock(
        return_value=httpx.Response(200, json={"id": 77, "phone_number": "+49111222333"})
    )
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/77/conversations").mock(
        return_value=httpx.Response(200, json={"payload": [{"id": 20, "inbox_id": 2, "status": "open"}]})
    )
    # No prior inbound from client.
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(200, json={"payload": []})
    )
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(200, json={"id": 301, "content": "Note"})
    )

    await client.mirror_outbound_as_note("+49111222333", "Note", contact_name="Alice Müller")

    assert search_mock.called
    assert create_mock.called
    create_body = json.loads(create_mock.calls[0].request.content)
    assert create_body["name"] == "Alice Müller"


@respx.mock
@pytest.mark.asyncio
async def test_log_incoming_message_logs_success(
    client: ChatwootClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """log_incoming_message success path is DEBUG-only: phone/ids logged, no INFO/WARNING."""
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/search").mock(
        return_value=httpx.Response(200, json={"payload": [{"id": 5, "phone_number": "+49111222333"}]})
    )
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/5/conversations").mock(
        return_value=httpx.Response(200, json={"payload": [{"id": 20, "inbox_id": 2, "status": "open"}]})
    )
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(200, json={"id": 200, "content": "Hi"})
    )

    import logging

    with caplog.at_level(logging.DEBUG, logger="altegio_bot.chatwoot_client"):
        conv_id, msg_id = await client.log_incoming_message("+49111222333", "Hi")

    assert conv_id == 20
    assert msg_id == 200
    # Normal per-message path must not add INFO/WARNING noise.
    assert [r for r in caplog.records if r.levelno >= logging.INFO] == []
    # The success line is still emitted, at DEBUG, with phone/ids.
    incoming_debug = [r for r in caplog.records if "incoming logged" in r.message]
    assert incoming_debug and all(r.levelno == logging.DEBUG for r in incoming_debug)
    assert "+49111222333" in caplog.text
    assert "20" in caplog.text
    assert "200" in caplog.text


@respx.mock
@pytest.mark.asyncio
async def test_mirror_outbound_as_note_logs_success(
    client: ChatwootClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """mirror_outbound_as_note success path is DEBUG-only: no INFO/WARNING."""
    _mock_contact_and_conv("+49111222333", 5, 20)
    # No prior inbound from client.
    _mock_messages(20, [])
    post_route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(200, json={"id": 300, "content": "Note"})
    )

    import logging

    with caplog.at_level(logging.DEBUG, logger="altegio_bot.chatwoot_client"):
        await client.mirror_outbound_as_note("+49111222333", "Note")

    assert post_route.called
    # Normal per-message mirroring must not add INFO/WARNING noise.
    assert [r for r in caplog.records if r.levelno >= logging.INFO] == []
    # The success line is still emitted, at DEBUG.
    mirror_debug = [r for r in caplog.records if "mirror note posted" in r.message]
    assert mirror_debug and all(r.levelno == logging.DEBUG for r in mirror_debug)


@respx.mock
@pytest.mark.asyncio
async def test_send_message_error_logs_response_body(
    client: ChatwootClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """On HTTP error, _log_and_raise should log status code and body."""
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(422, json={"error": "unprocessable entity"})
    )

    import logging

    with caplog.at_level(logging.WARNING, logger="altegio_bot.chatwoot_client"):
        with pytest.raises(httpx.HTTPStatusError):
            await client.send_message(15, "Hello", message_type="incoming")

    assert "422" in caplog.text
    assert "unprocessable entity" in caplog.text


@respx.mock
@pytest.mark.asyncio
async def test_get_or_create_conversation_reopens_resolved(
    client: ChatwootClient,
) -> None:
    """Should reopen most recent resolved conversation, not create new."""
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/42/conversations").mock(
        return_value=httpx.Response(
            200,
            json={
                "payload": [
                    {"id": 10, "inbox_id": 2, "status": "resolved", "created_at": 100},
                ]
            },
        )
    )
    toggle_route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/10/toggle_status").mock(
        return_value=httpx.Response(200, json={"id": 10, "status": "open"})
    )

    conv_id = await client.get_or_create_conversation(42)
    assert conv_id == 10
    assert toggle_route.called


@respx.mock
@pytest.mark.asyncio
async def test_get_or_create_contact_sends_name_on_create(client: ChatwootClient) -> None:
    """name= must be included in the POST body when creating a new contact."""
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/search").mock(
        return_value=httpx.Response(200, json={"payload": []})
    )
    create_route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/contacts").mock(
        return_value=httpx.Response(200, json={"id": 77, "phone_number": "+49111000111"})
    )

    cid = await client.get_or_create_contact("+49111000111", name="Bob Mustermann")
    assert cid == 77
    body = json.loads(create_route.calls[0].request.content)
    assert body.get("name") == "Bob Mustermann"


# ---------------------------------------------------------------------------
# Integration: deeplink injected into Chatwoot body
# ---------------------------------------------------------------------------


def _mock_contact_and_conv(phone: str, contact_id: int, conv_id: int) -> None:
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/search").mock(
        return_value=httpx.Response(
            200,
            json={"payload": [{"id": contact_id, "phone_number": phone}]},
        )
    )
    respx.get(f"https://chatwoot.example.com/api/v1/accounts/1/contacts/{contact_id}/conversations").mock(
        return_value=httpx.Response(
            200,
            json={"payload": [{"id": conv_id, "inbox_id": 2, "status": "open"}]},
        )
    )


def _mock_messages(conv_id: int, messages: list) -> None:
    """Mock GET /conversations/{conv_id}/messages."""
    respx.get(f"https://chatwoot.example.com/api/v1/accounts/1/conversations/{conv_id}/messages").mock(
        return_value=httpx.Response(200, json={"payload": messages})
    )


# ---------------------------------------------------------------------------
# Deeplink rules for log_incoming_message
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_log_incoming_message_body_no_deeplink(
    client: ChatwootClient,
) -> None:
    """Incoming customer message must NOT contain a wa.me deeplink."""
    _mock_contact_and_conv("+4917630316130", 5, 20)
    post_route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(200, json={"id": 200, "content": "x"})
    )

    await client.log_incoming_message("+4917630316130", "Привет")

    sent = json.loads(post_route.calls[0].request.content)
    assert "Привет" in sent["content"]
    assert "https://wa.me/" not in sent["content"]


# ---------------------------------------------------------------------------
# Deeplink rules for mirror_outbound_as_note
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_mirror_outbound_no_prior_inbound_contains_deeplink(
    client: ChatwootClient,
) -> None:
    """Bot mirror note BEFORE first client inbound must contain a wa.me deeplink."""
    _mock_contact_and_conv("+4917630316130", 5, 20)
    # Conversation has only outgoing/activity messages — no inbound from client.
    _mock_messages(20, [{"id": 1, "message_type": 1, "content": "prev outgoing"}])
    post_route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(200, json={"id": 300, "content": "x"})
    )

    await client.mirror_outbound_as_note("+4917630316130", "Запись подтверждена")

    sent = json.loads(post_route.calls[0].request.content)
    assert "Запись подтверждена" in sent["content"]
    assert "https://wa.me/4917630316130" in sent["content"]
    assert sent["private"] is True


@respx.mock
@pytest.mark.asyncio
async def test_mirror_outbound_after_inbound_no_deeplink(
    client: ChatwootClient,
) -> None:
    """Bot mirror note AFTER client wrote in must NOT contain a wa.me deeplink."""
    _mock_contact_and_conv("+4917630316130", 5, 20)
    # Conversation already has an incoming message from the client.
    _mock_messages(20, [{"id": 1, "message_type": 0, "content": "client wrote"}])
    post_route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(200, json={"id": 301, "content": "x"})
    )

    await client.mirror_outbound_as_note("+4917630316130", "Запись подтверждена")

    sent = json.loads(post_route.calls[0].request.content)
    assert "Запись подтверждена" in sent["content"]
    assert "https://wa.me/" not in sent["content"]
    assert sent["private"] is True


@respx.mock
@pytest.mark.asyncio
async def test_mirror_outbound_messages_api_error_keeps_deeplink(
    client: ChatwootClient,
) -> None:
    """When messages API fails, default to including deeplink (safe fallback)."""
    _mock_contact_and_conv("+4917630316130", 5, 20)
    # Simulate a 500 from the messages endpoint.
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(500)
    )
    post_route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(200, json={"id": 302, "content": "x"})
    )

    await client.mirror_outbound_as_note("+4917630316130", "Nachricht")

    sent = json.loads(post_route.calls[0].request.content)
    assert "https://wa.me/4917630316130" in sent["content"]


# ---------------------------------------------------------------------------
# X-Forwarded-Proto opt-in header (CHATWOOT_API_FORWARDED_PROTO)
# ---------------------------------------------------------------------------


def _client_with_proto(forwarded_proto: str | None) -> ChatwootClient:
    return ChatwootClient(
        base_url="https://chatwoot.example.com",
        api_token="test-token",
        account_id=1,
        inbox_id=2,
        forwarded_proto=forwarded_proto,
    )


def test_headers_no_forwarded_proto_by_default(client: ChatwootClient) -> None:
    """Default client (settings empty) must not send X-Forwarded-Proto."""
    headers = client._headers()
    assert "X-Forwarded-Proto" not in headers
    assert headers["api_access_token"] == "test-token"
    assert headers["Content-Type"] == "application/json"


def test_headers_forwarded_proto_https() -> None:
    headers = _client_with_proto("https")._headers()
    assert headers["X-Forwarded-Proto"] == "https"
    # Existing headers must be preserved, not overwritten.
    assert headers["api_access_token"] == "test-token"
    assert headers["Content-Type"] == "application/json"


def test_headers_forwarded_proto_http_allowed() -> None:
    headers = _client_with_proto("http")._headers()
    assert headers["X-Forwarded-Proto"] == "http"


def test_headers_forwarded_proto_trimmed_and_lowered() -> None:
    headers = _client_with_proto(" HTTPS ")._headers()
    assert headers["X-Forwarded-Proto"] == "https"


@pytest.mark.parametrize("blank", ["", "   "])
def test_headers_forwarded_proto_blank_means_no_header(blank: str) -> None:
    headers = _client_with_proto(blank)._headers()
    assert "X-Forwarded-Proto" not in headers


def test_headers_forwarded_proto_invalid_no_header_and_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING", logger="altegio_bot.chatwoot_headers"):
        headers = _client_with_proto("ftp")._headers()
    assert "X-Forwarded-Proto" not in headers
    assert "CHATWOOT_API_FORWARDED_PROTO" in caplog.text


def test_client_reads_forwarded_proto_from_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without an explicit kwarg the client falls back to settings."""
    from altegio_bot import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "chatwoot_api_forwarded_proto", "https")
    client = ChatwootClient(
        base_url="https://chatwoot.example.com",
        api_token="test-token",
        account_id=1,
        inbox_id=2,
    )
    assert client._headers()["X-Forwarded-Proto"] == "https"


@respx.mock
@pytest.mark.asyncio
async def test_contact_search_request_carries_forwarded_proto() -> None:
    """The actual outgoing HTTP request must carry the opt-in header."""
    route = respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/search").mock(
        return_value=httpx.Response(200, json={"payload": []})
    )
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/contacts").mock(
        return_value=httpx.Response(200, json={"payload": {"contact": {"id": 7}}})
    )

    await _client_with_proto("https").get_or_create_contact("+49123456789")

    request = route.calls[0].request
    assert request.headers["X-Forwarded-Proto"] == "https"
    assert request.headers["api_access_token"] == "test-token"


@respx.mock
@pytest.mark.asyncio
async def test_contact_search_request_has_no_forwarded_proto_by_default(
    client: ChatwootClient,
) -> None:
    """Default behaviour on the wire is unchanged: no X-Forwarded-Proto."""
    route = respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/search").mock(
        return_value=httpx.Response(
            200,
            json={"payload": [{"id": 42, "phone_number": "+49123456789", "name": "Test"}]},
        )
    )

    await client.get_or_create_contact("+49123456789")

    assert "X-Forwarded-Proto" not in route.calls[0].request.headers


# ---------------------------------------------------------------------------
# content_attributes: Python-side normalization for the Chatwoot REST API only.
#
# altegio_bot never connects to the Chatwoot database and never rewrites how
# Chatwoot stores content_attributes after create — the current Chatwoot version
# expects its own serialized storage format. These tests cover the API request
# body and the message-id return value only.
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_send_message_coerces_json_string_content_attributes_to_object(client: ChatwootClient) -> None:
    """A JSON-string object is sent to Chatwoot as a nested JSON object, never a string."""
    captured: dict[str, object] = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json={"id": 305, "content": "Hello"})

    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(side_effect=_capture)

    msg_id = await client.send_message(
        15,
        "Hello",
        message_type="incoming",
        content_attributes='{"in_reply_to": 123, "target_kind": "outbox_message"}',
    )

    assert msg_id == 305
    assert isinstance(captured["content_attributes"], dict)
    assert not isinstance(captured["content_attributes"], str)
    assert captured["content_attributes"] == {"in_reply_to": 123, "target_kind": "outbox_message"}


@respx.mock
@pytest.mark.asyncio
async def test_send_message_mapping_content_attributes_serialized_as_object(client: ChatwootClient) -> None:
    """A non-dict Mapping input is copied to a dict and sent as a JSON object."""
    from types import MappingProxyType

    captured: dict[str, object] = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json={"id": 306, "content": "Hello"})

    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(side_effect=_capture)

    attrs = MappingProxyType({"in_reply_to": 7})
    msg_id = await client.send_message(15, "Hello", message_type="incoming", content_attributes=attrs)

    assert msg_id == 306
    assert isinstance(captured["content_attributes"], dict)
    assert captured["content_attributes"] == {"in_reply_to": 7}


@respx.mock
@pytest.mark.asyncio
async def test_send_message_invalid_content_attributes_raises(client: ChatwootClient) -> None:
    """Invalid JSON or a non-object JSON string is rejected before any POST."""
    post_route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(200, json={"id": 1})
    )

    with pytest.raises(ValueError):
        await client.send_message(15, "Hi", message_type="incoming", content_attributes="{not valid json")

    with pytest.raises(ValueError):
        await client.send_message(15, "Hi", message_type="incoming", content_attributes='"just a string"')

    # Rejected before the message ever reaches the network.
    assert not post_route.called


@respx.mock
@pytest.mark.asyncio
async def test_send_message_returns_id_and_does_not_touch_chatwoot_db(client: ChatwootClient) -> None:
    """send_message returns the API message id and has no Chatwoot DB persistence hook.

    Regression for the removal of direct Chatwoot DB content_attributes
    normalization: nothing happens after a successful POST except returning the id.
    """
    import altegio_bot.chatwoot_client as cc

    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(200, json={"id": 913, "content": "Hi"})
    )

    msg_id = await client.send_message(
        15,
        "Hi",
        message_type="incoming",
        content_attributes={"in_reply_to": 7, "in_reply_to_external_id": "wamid.X"},
    )

    assert msg_id == 913
    # No direct Chatwoot DB normalization remains: no persistence hook on the
    # client, and no DB engine / SQL / sqlalchemy async factory in the module.
    module_names = list(vars(cc))
    assert not any("persist" in name for name in dir(client))
    assert not any("persist" in name for name in module_names)
    assert not any(name.startswith("_chatwoot_db") for name in module_names)
    assert not any("async_engine" in name for name in module_names)
    assert not any("NORMALIZE" in name for name in module_names)


# ---------------------------------------------------------------------------
# build_wa_click_to_chat_url – Click-to-Chat URL contract
# ---------------------------------------------------------------------------
#
# The URL only opens WhatsApp and prefills its composer. It sends nothing and
# does not bypass Meta's 24h customer service window.


def test_click_to_chat_normalises_messy_phone_to_digits() -> None:
    assert build_wa_click_to_chat_url("+49 (176) 303-16130") == "https://wa.me/4917630316130"


def test_click_to_chat_without_text_is_the_bare_base_url() -> None:
    assert build_wa_click_to_chat_url("+4917630316130") == "https://wa.me/4917630316130"
    assert build_wa_click_to_chat_url("+4917630316130", "") == "https://wa.me/4917630316130"


@pytest.mark.parametrize(
    "text",
    [
        "Guten Tag Frau Müller",
        "Zeile eins\nZeile zwei\r\nZeile drei",
        "Rabatt & Gutschein",
        "Passt 14:00?",
        "Termin #7",
        "100% zufrieden",
        'Sie sagten "morgen"',
        "Скидка 20% — приходите",
        "Bis morgen! 👍🏽💬🎉",
        "a+b=c/d?e&f#g%h;i,j:k@l$m!n'o(p)q~r_s-t.u",
        "   führende und nachgestellte Leerzeichen   ",
    ],
    ids=[
        "plain",
        "newlines",
        "ampersand",
        "question",
        "hash",
        "percent",
        "quotes",
        "cyrillic",
        "emoji",
        "reserved",
        "whitespace",
    ],
)
def test_click_to_chat_query_round_trips_exactly(text: str) -> None:
    """The prefilled text must decode back byte-for-byte."""
    url = build_wa_click_to_chat_url("+4917630316130", text)
    assert url is not None
    base, _, query = url.partition("?text=")
    assert base == "https://wa.me/4917630316130"
    assert unquote(query) == text
    # Fully percent-encoded: the finished URL is pure ASCII, and no raw
    # separator survives that could break the Markdown link or the query
    # string (``%`` itself is the escape prefix, so it is expected).
    assert url.isascii()
    assert not any(ch in query for ch in " \n\r&?#\"'()")


def _click_to_chat_prefix_len(phone_e164: str) -> int:
    return len(f"https://wa.me/{re.sub(r'[^0-9]', '', phone_e164)}?text=")


def test_click_to_chat_exactly_at_the_limit_keeps_the_text() -> None:
    phone = "+4917630316130"
    # ASCII letters encode 1:1, so the finished URL length is exactly 2000.
    text = "a" * (WA_CLICK_TO_CHAT_MAX_URL_CHARS - _click_to_chat_prefix_len(phone))
    url = build_wa_click_to_chat_url(phone, text)
    assert url is not None
    assert len(url) == 2000 == WA_CLICK_TO_CHAT_MAX_URL_CHARS
    assert unquote(url.partition("?text=")[2]) == text


def test_click_to_chat_one_char_over_the_limit_drops_the_query() -> None:
    phone = "+4917630316130"
    text = "a" * (WA_CLICK_TO_CHAT_MAX_URL_CHARS - _click_to_chat_prefix_len(phone) + 1)
    assert build_wa_click_to_chat_url(phone, text) == "https://wa.me/4917630316130"


def test_click_to_chat_measures_the_encoded_length_not_the_raw_text() -> None:
    """A short Unicode text can still exceed the limit once percent-encoded."""
    phone = "+4917630316130"
    # Each emoji is 4 UTF-8 bytes → 12 encoded characters.
    text = "🎉" * 200
    assert len(text) == 200
    assert build_wa_click_to_chat_url(phone, text) == "https://wa.me/4917630316130"


def test_click_to_chat_never_truncates_the_text() -> None:
    """Over the limit the prefill is dropped whole — never shortened."""
    phone = "+4917630316130"
    text = "Sehr wichtige Nachricht. " * 200
    url = build_wa_click_to_chat_url(phone, text)
    assert url == "https://wa.me/4917630316130"
    assert "?text=" not in url
    # And nothing below the limit loses a single character.
    short = "Sehr wichtige Nachricht."
    short_url = build_wa_click_to_chat_url(phone, short)
    assert short_url is not None
    assert unquote(short_url.partition("?text=")[2]) == short


@pytest.mark.parametrize("phone", [None, "", "   ", "+++---", "no digits here"])
def test_click_to_chat_unusable_phone_returns_none(phone: str | None) -> None:
    assert build_wa_click_to_chat_url(phone, "Hallo") is None
    assert build_wa_click_to_chat_url(phone) is None


# ---------------------------------------------------------------------------
# outbound_mirror_content_attributes – the native marker contract
# ---------------------------------------------------------------------------


def test_outbound_mirror_content_attributes_holds_only_marker_and_wamid() -> None:
    attrs = outbound_mirror_content_attributes("wamid.ABC")
    assert attrs == {
        "altegio_bot_message_kind": "whatsapp_outbound_mirror_v1",
        "whatsapp_provider_message_id": "wamid.ABC",
    }


@pytest.mark.parametrize("wamid", [None, "", "   "])
def test_outbound_mirror_content_attributes_without_wamid_is_none(wamid: str | None) -> None:
    """No wamid → a plain note that can never be proven native."""
    assert outbound_mirror_content_attributes(wamid) is None


@respx.mock
@pytest.mark.asyncio
async def test_mirror_outbound_as_note_sends_the_native_marker(client: ChatwootClient) -> None:
    _mock_contact_and_conv("+4917630316130", 5, 20)
    _mock_messages(20, [{"id": 1, "message_type": 0, "content": "client wrote"}])
    post_route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(200, json={"id": 310, "content": "x"})
    )

    await client.mirror_outbound_as_note(
        "+4917630316130",
        "Ihr Termin morgen um 10:00",
        provider_message_id="wamid.MIRROR",
    )

    sent = json.loads(post_route.calls[0].request.content)
    assert sent["private"] is True
    assert sent["message_type"] == "outgoing"
    assert sent["content_attributes"] == {
        "altegio_bot_message_kind": "whatsapp_outbound_mirror_v1",
        "whatsapp_provider_message_id": "wamid.MIRROR",
    }


@respx.mock
@pytest.mark.asyncio
async def test_mirror_outbound_as_note_without_wamid_omits_content_attributes(client: ChatwootClient) -> None:
    """Historical behaviour is unchanged when no wamid is passed."""
    _mock_contact_and_conv("+4917630316130", 5, 20)
    _mock_messages(20, [{"id": 1, "message_type": 0, "content": "client wrote"}])
    post_route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(200, json={"id": 311, "content": "x"})
    )

    await client.mirror_outbound_as_note("+4917630316130", "Ihr Termin morgen um 10:00")

    sent = json.loads(post_route.calls[0].request.content)
    assert "content_attributes" not in sent


# ---------------------------------------------------------------------------
# find_outbound_mirror_note – fail-closed proof of one native target
# ---------------------------------------------------------------------------

_MIRROR_WAMID = "wamid.PROVEN"


def _mirror_note(
    *,
    message_id: int = 4242,
    conversation_id: int | None = 30,
    message_type: object = "outgoing",
    private: object = True,
    kind: object = "whatsapp_outbound_mirror_v1",
    wamid: object = _MIRROR_WAMID,
    content_attributes: object = "__build__",
) -> dict:
    attrs: object
    if content_attributes == "__build__":
        attrs = {"altegio_bot_message_kind": kind, "whatsapp_provider_message_id": wamid}
    else:
        attrs = content_attributes
    message: dict = {
        "id": message_id,
        "message_type": message_type,
        "private": private,
        "content": "Ihr Termin morgen um 10:00",
        "content_attributes": attrs,
    }
    if conversation_id is not None:
        message["conversation_id"] = conversation_id
    return message


@respx.mock
@pytest.mark.asyncio
async def test_find_outbound_mirror_note_proves_single_match(client: ChatwootClient) -> None:
    _mock_messages(
        30,
        [
            {"id": 1, "message_type": 0, "private": False, "content": "client wrote"},
            _mirror_note(message_id=4242),
        ],
    )

    assert await client.find_outbound_mirror_note(30, _MIRROR_WAMID) == 4242


@respx.mock
@pytest.mark.asyncio
async def test_find_outbound_mirror_note_accepts_numeric_outgoing_enum(client: ChatwootClient) -> None:
    _mock_messages(30, [_mirror_note(message_id=4243, message_type=1)])

    assert await client.find_outbound_mirror_note(30, _MIRROR_WAMID) == 4243


@respx.mock
@pytest.mark.asyncio
async def test_find_outbound_mirror_note_accepts_serialized_content_attributes(client: ChatwootClient) -> None:
    """Some Chatwoot versions return content_attributes as a JSON string."""
    serialized = json.dumps(
        {
            "altegio_bot_message_kind": "whatsapp_outbound_mirror_v1",
            "whatsapp_provider_message_id": _MIRROR_WAMID,
        }
    )
    _mock_messages(30, [_mirror_note(message_id=4244, content_attributes=serialized)])

    assert await client.find_outbound_mirror_note(30, _MIRROR_WAMID) == 4244


@pytest.mark.parametrize(
    "message",
    [
        _mirror_note(kind="whatsapp_outbound_mirror_v0"),
        _mirror_note(kind=None),
        _mirror_note(wamid="wamid.SOMEONE_ELSE"),
        _mirror_note(wamid=None),
        _mirror_note(private=False),
        _mirror_note(private=None),
        _mirror_note(private="true"),
        _mirror_note(message_type="incoming"),
        _mirror_note(message_type=0),
        _mirror_note(message_type=True),
        _mirror_note(conversation_id=31),
        _mirror_note(content_attributes=None),
        _mirror_note(content_attributes="not json"),
        _mirror_note(content_attributes="[1, 2]"),
        _mirror_note(content_attributes=["not", "a", "mapping"]),
        _mirror_note(message_id=0),
        _mirror_note(message_id=-5),
        _mirror_note(message_id="4242"),
        _mirror_note(message_id=None),
        _mirror_note(message_id=True),
    ],
    ids=[
        "old_marker_version",
        "missing_marker",
        "foreign_wamid",
        "missing_wamid",
        "not_private",
        "private_none",
        "private_string",
        "incoming",
        "incoming_enum",
        "private_bool_message_type",
        "other_conversation",
        "no_content_attributes",
        "malformed_content_attributes",
        "content_attributes_not_object",
        "content_attributes_list",
        "zero_id",
        "negative_id",
        "string_id",
        "missing_id",
        "bool_id",
    ],
)
@respx.mock
@pytest.mark.asyncio
async def test_find_outbound_mirror_note_fails_closed_on_unproven_candidate(
    client: ChatwootClient,
    message: dict,
) -> None:
    _mock_messages(30, [message])

    assert await client.find_outbound_mirror_note(30, _MIRROR_WAMID) is None


@respx.mock
@pytest.mark.asyncio
async def test_find_outbound_mirror_note_fails_closed_on_duplicate_marker(client: ChatwootClient) -> None:
    """Two distinct proven notes are as unusable as none — never pick one."""
    _mock_messages(30, [_mirror_note(message_id=4242), _mirror_note(message_id=4343)])

    assert await client.find_outbound_mirror_note(30, _MIRROR_WAMID) is None


@respx.mock
@pytest.mark.asyncio
async def test_find_outbound_mirror_note_tolerates_repeated_identical_row(client: ChatwootClient) -> None:
    """The same message listed twice is one target, not an ambiguity."""
    _mock_messages(30, [_mirror_note(message_id=4242), _mirror_note(message_id=4242)])

    assert await client.find_outbound_mirror_note(30, _MIRROR_WAMID) == 4242


@respx.mock
@pytest.mark.asyncio
async def test_find_outbound_mirror_note_fails_closed_on_empty_conversation(client: ChatwootClient) -> None:
    _mock_messages(30, [])

    assert await client.find_outbound_mirror_note(30, _MIRROR_WAMID) is None


@respx.mock
@pytest.mark.asyncio
async def test_find_outbound_mirror_note_fails_closed_on_http_error(client: ChatwootClient) -> None:
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/conversations/30/messages").mock(
        return_value=httpx.Response(500)
    )

    assert await client.find_outbound_mirror_note(30, _MIRROR_WAMID) is None


@respx.mock
@pytest.mark.asyncio
async def test_find_outbound_mirror_note_fails_closed_on_malformed_payload(client: ChatwootClient) -> None:
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/conversations/30/messages").mock(
        return_value=httpx.Response(200, text="not json at all")
    )

    assert await client.find_outbound_mirror_note(30, _MIRROR_WAMID) is None


@respx.mock
@pytest.mark.asyncio
async def test_find_outbound_mirror_note_fails_closed_on_transport_error(client: ChatwootClient) -> None:
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/conversations/30/messages").mock(
        side_effect=httpx.ConnectError("boom")
    )

    assert await client.find_outbound_mirror_note(30, _MIRROR_WAMID) is None


@pytest.mark.parametrize(("conversation_id", "wamid"), [(0, _MIRROR_WAMID), (30, ""), (30, "   ")])
@respx.mock
@pytest.mark.asyncio
async def test_find_outbound_mirror_note_never_calls_the_api_without_inputs(
    client: ChatwootClient,
    conversation_id: int,
    wamid: str,
) -> None:
    route = respx.get(f"https://chatwoot.example.com/api/v1/accounts/1/conversations/{conversation_id}/messages").mock(
        return_value=httpx.Response(200, json={"payload": [_mirror_note()]})
    )

    assert await client.find_outbound_mirror_note(conversation_id, wamid) is None
    assert not route.called


@respx.mock
@pytest.mark.asyncio
async def test_find_outbound_mirror_note_is_read_only(client: ChatwootClient) -> None:
    """The lookup only reads the conversation; it posts nothing."""
    _mock_messages(30, [_mirror_note(message_id=4242)])
    post_route = respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/30/messages")

    await client.find_outbound_mirror_note(30, _MIRROR_WAMID)

    assert not post_route.called
