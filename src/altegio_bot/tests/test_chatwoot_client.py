"""Unit tests for ChatwootClient."""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from altegio_bot.chatwoot_client import ChatwootClient, append_wa_deeplink

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
# Post-create content_attributes persistence normalization
#
# Chatwoot's create-message endpoint stores content_attributes as a JSON *string*
# even when the HTTP body is a nested object, so native reply context
# (content_attributes ->> 'in_reply_to') is NULL. send_message therefore runs a
# best-effort, idempotent UPDATE against the Chatwoot DB for the single message it
# just created. These tests cover that wiring without touching a real database.
# ---------------------------------------------------------------------------


def _reset_chatwoot_db_engine_state(monkeypatch: pytest.MonkeyPatch) -> None:
    import altegio_bot.chatwoot_client as cc

    monkeypatch.setattr(cc, "_chatwoot_db_engine", None, raising=False)
    monkeypatch.setattr(cc, "_chatwoot_db_engine_url", None, raising=False)
    monkeypatch.setattr(cc, "_chatwoot_db_engine_error_url", None, raising=False)
    monkeypatch.setattr(cc, "_chatwoot_db_engine_error_type", None, raising=False)
    monkeypatch.setattr(cc, "_chatwoot_db_runtime_error_url", None, raising=False)
    monkeypatch.setattr(cc, "_chatwoot_db_runtime_error_until", 0.0, raising=False)
    monkeypatch.setattr(cc, "_chatwoot_db_runtime_error_type", None, raising=False)
    monkeypatch.setattr(cc, "_chatwoot_db_runtime_error_count", 0, raising=False)


class _FakeConn:
    """Records executed statements; optionally raises to simulate a DB failure."""

    def __init__(self, recorder: list[tuple[str, object]], *, fail: bool = False) -> None:
        self._recorder = recorder
        self._fail = fail

    async def execute(self, statement: object, params: object = None) -> None:
        if self._fail:
            raise RuntimeError("simulated chatwoot db failure")
        self._recorder.append((str(statement), params))


class _FakeBeginCtx:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakeEngine:
    def __init__(self, recorder: list[tuple[str, object]], *, fail: bool = False) -> None:
        self._recorder = recorder
        self._fail = fail

    def begin(self) -> _FakeBeginCtx:
        return _FakeBeginCtx(_FakeConn(self._recorder, fail=self._fail))


class _CountingFailEngine:
    def __init__(self) -> None:
        self.begin_calls = 0

    def begin(self) -> _FakeBeginCtx:
        self.begin_calls += 1
        return _FakeBeginCtx(_FakeConn([], fail=True))


class _SequencedEngine:
    def __init__(self, failures: list[bool]) -> None:
        self._failures = failures
        self.begin_calls = 0
        self.recorder: list[tuple[str, object]] = []

    def begin(self) -> _FakeBeginCtx:
        fail = self.begin_calls < len(self._failures) and self._failures[self.begin_calls]
        self.begin_calls += 1
        return _FakeBeginCtx(_FakeConn(self.recorder, fail=fail))


# ---------------------------------------------------------------------------
# Client payload + persistence wiring
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_send_message_sends_content_attributes_as_dict(
    client: ChatwootClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The HTTP JSON body carries content_attributes as a nested object, not a string."""
    import altegio_bot.chatwoot_client as cc

    _reset_chatwoot_db_engine_state(monkeypatch)
    monkeypatch.setattr(cc.settings, "chatwoot_db_url", "", raising=False)

    captured: dict[str, object] = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json={"id": 303, "content": "Hello"})

    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(side_effect=_capture)

    attrs = {"in_reply_to": 123, "in_reply_to_external_id": "wamid.X"}
    msg_id = await client.send_message(15, "Hello", message_type="incoming", content_attributes=attrs)

    assert msg_id == 303
    assert isinstance(captured["content_attributes"], dict)
    assert not isinstance(captured["content_attributes"], str)
    assert captured["content_attributes"] == attrs


@respx.mock
@pytest.mark.asyncio
async def test_send_message_coerces_json_string_content_attributes_to_object(
    client: ChatwootClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A JSON-string content_attributes is coerced to a dict in the body and to the hook."""
    import altegio_bot.chatwoot_client as cc

    _reset_chatwoot_db_engine_state(monkeypatch)
    monkeypatch.setattr(cc.settings, "chatwoot_db_url", "", raising=False)

    captured: dict[str, object] = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json={"id": 305, "content": "Hello"})

    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(side_effect=_capture)

    calls: list[tuple[int, int, dict[str, object]]] = []

    async def _spy(message_id: int, conversation_id: int, attributes: dict[str, object]) -> None:
        calls.append((message_id, conversation_id, attributes))

    client._persist_native_content_attributes = _spy  # type: ignore[method-assign]

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
    # The persistence hook receives the parsed dict, never the original string.
    assert calls == [(305, 15, {"in_reply_to": 123, "target_kind": "outbox_message"})]
    assert isinstance(calls[0][2], dict)


@respx.mock
@pytest.mark.asyncio
async def test_send_message_calls_persistence_after_success(client: ChatwootClient) -> None:
    """After a successful create, persistence is invoked with id/conv/normalized dict."""
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(200, json={"id": 913, "content": "Hi"})
    )

    calls: list[tuple[int, int, dict[str, object]]] = []

    async def _spy(message_id: int, conversation_id: int, attributes: dict[str, object]) -> None:
        calls.append((message_id, conversation_id, attributes))

    client._persist_native_content_attributes = _spy  # type: ignore[method-assign]

    msg_id = await client.send_message(
        15,
        "Hi",
        message_type="incoming",
        content_attributes={"in_reply_to": 123, "in_reply_to_external_id": "wamid.X"},
    )

    assert msg_id == 913
    assert calls == [(913, 15, {"in_reply_to": 123, "in_reply_to_external_id": "wamid.X"})]


@respx.mock
@pytest.mark.asyncio
async def test_send_message_skips_persistence_without_content_attributes(client: ChatwootClient) -> None:
    """No content_attributes → no DB normalization (fallback paths are untouched)."""
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(200, json={"id": 911, "content": "Hi"})
    )

    calls: list[object] = []

    async def _spy(*args: object, **kwargs: object) -> None:
        calls.append((args, kwargs))

    client._persist_native_content_attributes = _spy  # type: ignore[method-assign]

    msg_id = await client.send_message(15, "Hi", message_type="outgoing")

    assert msg_id == 911
    assert calls == []


@respx.mock
@pytest.mark.asyncio
async def test_send_message_returns_id_when_db_normalization_fails(
    client: ChatwootClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A normalization failure never turns a successful POST into a failure."""
    import altegio_bot.chatwoot_client as cc

    _reset_chatwoot_db_engine_state(monkeypatch)
    monkeypatch.setattr(
        cc.settings,
        "chatwoot_db_url",
        "postgresql+asyncpg://postgres:pw@cw-postgres:5432/chatwoot",
        raising=False,
    )
    monkeypatch.setattr(cc, "_get_chatwoot_db_engine", lambda: _FakeEngine([], fail=True))
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(200, json={"id": 912, "content": "Hi"})
    )

    msg_id = await client.send_message(15, "Hi", message_type="outgoing", content_attributes={"in_reply_to": 7})

    assert msg_id == 912


# ---------------------------------------------------------------------------
# DB normalization
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_persist_native_content_attributes_executes_idempotent_update(
    client: ChatwootClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When configured, it runs the guarded UPDATE for exactly the given message id."""
    import altegio_bot.chatwoot_client as cc

    _reset_chatwoot_db_engine_state(monkeypatch)
    monkeypatch.setattr(
        cc.settings,
        "chatwoot_db_url",
        "postgresql+asyncpg://postgres:pw@cw-postgres:5432/chatwoot",
        raising=False,
    )
    recorder: list[tuple[str, object]] = []
    monkeypatch.setattr(cc, "_get_chatwoot_db_engine", lambda: _FakeEngine(recorder))

    await client._persist_native_content_attributes(8200, 15, {"in_reply_to": 8179})

    assert len(recorder) == 1
    sql, params = recorder[0]
    assert params == {"message_id": 8200}
    # Idempotency + scope guards must be present in the statement; single-row only.
    assert "UPDATE messages" in sql
    assert "jsonb_typeof" in sql
    assert "#>> '{}'" in sql
    assert "'string'" in sql
    assert ":message_id" in sql
    assert "WHERE id = :message_id" in sql


@pytest.mark.asyncio
async def test_empty_chatwoot_db_url_is_noop(
    client: ChatwootClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Empty chatwoot_db_url → safe no-op, no engine created, no raise."""
    import altegio_bot.chatwoot_client as cc

    _reset_chatwoot_db_engine_state(monkeypatch)
    monkeypatch.setattr(cc.settings, "chatwoot_db_url", "", raising=False)

    created: list[object] = []
    monkeypatch.setattr(cc, "create_async_engine", lambda *a, **k: created.append((a, k)))

    await client._persist_native_content_attributes(8201, 15, {"in_reply_to": 8179})

    assert created == []
    assert cc._get_chatwoot_db_engine() is None


@respx.mock
@pytest.mark.asyncio
async def test_malformed_chatwoot_db_url_is_no_raise_and_safe(
    client: ChatwootClient,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A malformed chatwoot_db_url must not turn a successful POST into a failure."""
    import logging

    import altegio_bot.chatwoot_client as cc

    bad_url = "postgresql+notreal://postgres:super-secret@host/db"
    _reset_chatwoot_db_engine_state(monkeypatch)
    monkeypatch.setattr(cc.settings, "chatwoot_db_url", bad_url, raising=False)
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(200, json={"id": 913, "content": "Hi"})
    )

    with caplog.at_level(logging.WARNING, logger="altegio_bot.chatwoot_client"):
        msg_id = await client.send_message(15, "Hi", message_type="incoming", content_attributes={"in_reply_to": 7})

    assert msg_id == 913
    warning_text = "\n".join(record.message for record in caplog.records)
    assert "normalization disabled" in warning_text
    assert "normalization skipped" in warning_text
    # Log safety: never leak the DSN / password / host.
    assert bad_url not in warning_text
    assert "super-secret" not in warning_text
    assert "host/db" not in warning_text


def test_get_chatwoot_db_engine_lazy_and_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    """Engine is None when unset, built once and reused, and a URL change rebuilds it."""
    import altegio_bot.chatwoot_client as cc

    _reset_chatwoot_db_engine_state(monkeypatch)

    # Unconfigured → None, never touches the engine factory.
    monkeypatch.setattr(cc.settings, "chatwoot_db_url", "", raising=False)
    assert cc._get_chatwoot_db_engine() is None

    built: list[str] = []

    def _fake_create_async_engine(url: str, **kwargs: object) -> object:
        built.append(url)
        return object()

    monkeypatch.setattr(cc, "create_async_engine", _fake_create_async_engine)

    url_a = "postgresql+asyncpg://postgres:pw@cw-postgres:5432/chatwoot"
    monkeypatch.setattr(cc.settings, "chatwoot_db_url", url_a, raising=False)
    first = cc._get_chatwoot_db_engine()
    second = cc._get_chatwoot_db_engine()
    assert first is second
    assert built == [url_a]

    # URL change → a new engine is attempted.
    url_b = "postgresql+asyncpg://postgres:pw@other-host:5432/chatwoot"
    monkeypatch.setattr(cc.settings, "chatwoot_db_url", url_b, raising=False)
    third = cc._get_chatwoot_db_engine()
    assert third is not first
    assert built == [url_a, url_b]


def test_configured_engine_timeouts(monkeypatch: pytest.MonkeyPatch) -> None:
    """create_async_engine receives the configured connect/pool timeouts."""
    import altegio_bot.chatwoot_client as cc

    built: list[dict[str, object]] = []

    def _fake_create_async_engine(url: str, **kwargs: object) -> object:
        built.append(dict(kwargs))
        return object()

    _reset_chatwoot_db_engine_state(monkeypatch)
    monkeypatch.setattr(cc, "create_async_engine", _fake_create_async_engine)
    monkeypatch.setattr(
        cc.settings,
        "chatwoot_db_url",
        "postgresql+asyncpg://postgres:pw@cw-postgres:5432/chatwoot",
        raising=False,
    )
    monkeypatch.setattr(cc.settings, "chatwoot_db_connect_timeout_seconds", 2.5, raising=False)
    monkeypatch.setattr(cc.settings, "chatwoot_db_pool_timeout_seconds", 1.5, raising=False)

    assert cc._get_chatwoot_db_engine() is not None
    assert built == [
        {
            "pool_pre_ping": True,
            "pool_size": 2,
            "max_overflow": 2,
            "pool_timeout": 1.5,
            "connect_args": {"timeout": 2.5},
        }
    ]


# ---------------------------------------------------------------------------
# Runtime failure threshold / cooldown
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_runtime_db_failure_does_not_arm_cooldown_before_threshold(
    client: ChatwootClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import altegio_bot.chatwoot_client as cc

    db_url = "postgresql+asyncpg://postgres:super-secret@deadhost:5432/chatwoot"
    engine = _CountingFailEngine()
    _reset_chatwoot_db_engine_state(monkeypatch)
    monkeypatch.setattr(cc.settings, "chatwoot_db_url", db_url, raising=False)
    monkeypatch.setattr(cc.settings, "chatwoot_db_runtime_failure_threshold", 3, raising=False)
    monkeypatch.setattr(cc, "_get_chatwoot_db_engine", lambda: engine)
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(200, json={"id": 914, "content": "Hi"})
    )

    first = await client.send_message(15, "Hi", message_type="incoming", content_attributes={"in_reply_to": 7})
    second = await client.send_message(15, "Hi again", message_type="incoming", content_attributes={"in_reply_to": 8})

    assert (first, second) == (914, 914)
    assert engine.begin_calls == 2
    assert cc._chatwoot_db_runtime_error_count == 2
    assert not cc._chatwoot_db_runtime_failure_active(db_url)


@respx.mock
@pytest.mark.asyncio
async def test_runtime_db_failure_arms_cooldown_at_threshold(
    client: ChatwootClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import altegio_bot.chatwoot_client as cc

    db_url = "postgresql+asyncpg://postgres:super-secret@deadhost:5432/chatwoot"
    engine = _CountingFailEngine()
    _reset_chatwoot_db_engine_state(monkeypatch)
    monkeypatch.setattr(cc.settings, "chatwoot_db_url", db_url, raising=False)
    monkeypatch.setattr(cc.settings, "chatwoot_db_runtime_failure_threshold", 3, raising=False)
    monkeypatch.setattr(cc, "_get_chatwoot_db_engine", lambda: engine)
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(200, json={"id": 914, "content": "Hi"})
    )

    ids = [
        await client.send_message(15, "Hi 1", message_type="incoming", content_attributes={"in_reply_to": 7}),
        await client.send_message(15, "Hi 2", message_type="incoming", content_attributes={"in_reply_to": 8}),
        await client.send_message(15, "Hi 3", message_type="incoming", content_attributes={"in_reply_to": 9}),
        await client.send_message(15, "Hi 4", message_type="incoming", content_attributes={"in_reply_to": 10}),
    ]

    assert ids == [914, 914, 914, 914]
    # The 4th send is during cooldown → DB attempt is skipped.
    assert engine.begin_calls == 3
    assert cc._chatwoot_db_runtime_error_count == 3
    assert cc._chatwoot_db_runtime_failure_active(db_url)


@respx.mock
@pytest.mark.asyncio
async def test_runtime_db_success_clears_failure_state(
    client: ChatwootClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import altegio_bot.chatwoot_client as cc

    db_url = "postgresql+asyncpg://postgres:super-secret@cw-postgres:5432/chatwoot"
    engine = _SequencedEngine([True, False, True])
    _reset_chatwoot_db_engine_state(monkeypatch)
    monkeypatch.setattr(cc.settings, "chatwoot_db_url", db_url, raising=False)
    monkeypatch.setattr(cc.settings, "chatwoot_db_runtime_failure_threshold", 2, raising=False)
    monkeypatch.setattr(cc, "_get_chatwoot_db_engine", lambda: engine)
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(200, json={"id": 917, "content": "Hi"})
    )

    await client.send_message(15, "fails once", message_type="incoming", content_attributes={"in_reply_to": 7})
    assert cc._chatwoot_db_runtime_error_count == 1

    await client.send_message(15, "succeeds", message_type="incoming", content_attributes={"in_reply_to": 8})
    assert cc._chatwoot_db_runtime_error_count == 0
    assert not cc._chatwoot_db_runtime_failure_active(db_url)

    await client.send_message(15, "fails again", message_type="incoming", content_attributes={"in_reply_to": 9})
    assert cc._chatwoot_db_runtime_error_count == 1
    assert not cc._chatwoot_db_runtime_failure_active(db_url)
    assert engine.begin_calls == 3


@respx.mock
@pytest.mark.asyncio
async def test_url_change_retries_after_runtime_failure(
    client: ChatwootClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import altegio_bot.chatwoot_client as cc

    url_a = "postgresql+asyncpg://postgres:secret-a@deadhost:5432/chatwoot"
    url_b = "postgresql+asyncpg://postgres:secret-b@otherhost:5432/chatwoot"
    engine = _CountingFailEngine()
    _reset_chatwoot_db_engine_state(monkeypatch)
    monkeypatch.setattr(cc.settings, "chatwoot_db_url", url_a, raising=False)
    monkeypatch.setattr(cc.settings, "chatwoot_db_runtime_failure_threshold", 3, raising=False)
    monkeypatch.setattr(cc, "_get_chatwoot_db_engine", lambda: engine)
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(200, json={"id": 915, "content": "Hi"})
    )

    await client.send_message(15, "Hi 1", message_type="incoming", content_attributes={"in_reply_to": 7})
    await client.send_message(15, "Hi 2", message_type="incoming", content_attributes={"in_reply_to": 8})
    await client.send_message(15, "Hi 3", message_type="incoming", content_attributes={"in_reply_to": 9})
    assert cc._chatwoot_db_runtime_failure_active(url_a)

    # URL change → cooldown for url_a no longer applies; DB is attempted immediately.
    monkeypatch.setattr(cc.settings, "chatwoot_db_url", url_b, raising=False)
    await client.send_message(15, "Hi again", message_type="incoming", content_attributes={"in_reply_to": 10})

    assert engine.begin_calls == 4


@respx.mock
@pytest.mark.asyncio
async def test_cooldown_expiry_retries(
    client: ChatwootClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import altegio_bot.chatwoot_client as cc

    now = 100.0

    def _fake_monotonic() -> float:
        return now

    db_url = "postgresql+asyncpg://postgres:super-secret@deadhost:5432/chatwoot"
    engine = _CountingFailEngine()
    _reset_chatwoot_db_engine_state(monkeypatch)
    monkeypatch.setattr(cc.time, "monotonic", _fake_monotonic)
    monkeypatch.setattr(cc.settings, "chatwoot_db_url", db_url, raising=False)
    monkeypatch.setattr(cc.settings, "chatwoot_db_runtime_failure_threshold", 3, raising=False)
    monkeypatch.setattr(cc.settings, "chatwoot_db_runtime_failure_cooldown_seconds", 30.0, raising=False)
    monkeypatch.setattr(cc, "_get_chatwoot_db_engine", lambda: engine)
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(200, json={"id": 916, "content": "Hi"})
    )

    await client.send_message(15, "Hi 1", message_type="incoming", content_attributes={"in_reply_to": 7})
    now = 101.0
    await client.send_message(15, "Hi 2", message_type="incoming", content_attributes={"in_reply_to": 8})
    now = 102.0
    await client.send_message(15, "Hi 3", message_type="incoming", content_attributes={"in_reply_to": 9})
    # Cooldown armed at t=102 until t=132. A send before expiry is skipped.
    now = 120.0
    await client.send_message(15, "during cooldown", message_type="incoming", content_attributes={"in_reply_to": 10})
    assert engine.begin_calls == 3
    # After expiry, the DB is attempted again.
    now = 133.0
    await client.send_message(15, "after expiry", message_type="incoming", content_attributes={"in_reply_to": 11})
    assert engine.begin_calls == 4


@respx.mock
@pytest.mark.asyncio
async def test_cooldown_skip_does_not_log_warning_or_info_spam(
    client: ChatwootClient,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    import altegio_bot.chatwoot_client as cc

    db_url = "postgresql+asyncpg://postgres:super-secret@deadhost:5432/chatwoot"
    engine = _CountingFailEngine()
    _reset_chatwoot_db_engine_state(monkeypatch)
    monkeypatch.setattr(cc.settings, "chatwoot_db_url", db_url, raising=False)
    monkeypatch.setattr(cc.settings, "chatwoot_db_runtime_failure_threshold", 1, raising=False)
    monkeypatch.setattr(cc, "_get_chatwoot_db_engine", lambda: engine)
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/15/messages").mock(
        return_value=httpx.Response(200, json={"id": 918, "content": "Hi"})
    )

    # First send arms cooldown immediately (threshold=1).
    await client.send_message(15, "arms cooldown", message_type="incoming", content_attributes={"in_reply_to": 7})
    assert engine.begin_calls == 1

    caplog.clear()
    with caplog.at_level(logging.INFO):
        await client.send_message(15, "skip 1", message_type="incoming", content_attributes={"in_reply_to": 8})
        await client.send_message(15, "skip 2", message_type="incoming", content_attributes={"in_reply_to": 9})
        await client.send_message(15, "skip 3", message_type="incoming", content_attributes={"in_reply_to": 10})

    # No further DB attempts and no WARNING/INFO spam from the client (DEBUG only).
    assert engine.begin_calls == 1
    client_logs = [record for record in caplog.records if record.name == "altegio_bot.chatwoot_client"]
    assert client_logs == []
