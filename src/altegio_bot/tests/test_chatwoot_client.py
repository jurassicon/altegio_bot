"""Unit tests for ChatwootClient."""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from altegio_bot.chatwoot_client import (
    ChatwootClient,
    append_wa_deeplink,
    forwarded_proto_header,
    normalize_forwarded_proto,
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
    """log_incoming_message should emit INFO with phone/ids on success."""
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

    with caplog.at_level(logging.INFO, logger="altegio_bot.chatwoot_client"):
        conv_id, msg_id = await client.log_incoming_message("+49111222333", "Hi")

    assert conv_id == 20
    assert msg_id == 200
    assert "+49111222333" in caplog.text
    assert "20" in caplog.text
    assert "200" in caplog.text


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
    with caplog.at_level("WARNING", logger="altegio_bot.chatwoot_client"):
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
# normalize_forwarded_proto / forwarded_proto_header – shared helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        ("   ", None),
        ("https", "https"),
        ("http", "http"),
        (" https ", "https"),
        ("HTTPS", "https"),
        ("ftp", None),
        ("https://chatwoot.example.com", None),
    ],
)
def test_normalize_forwarded_proto(value: str | None, expected: str | None) -> None:
    assert normalize_forwarded_proto(value) == expected


def test_forwarded_proto_header_from_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    from altegio_bot import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "chatwoot_api_forwarded_proto", "https")
    assert forwarded_proto_header() == {"X-Forwarded-Proto": "https"}

    monkeypatch.setattr(settings_module.settings, "chatwoot_api_forwarded_proto", "")
    assert forwarded_proto_header() == {}
