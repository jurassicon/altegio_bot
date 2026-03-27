"""Unit tests for ChatwootClient."""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from altegio_bot.chatwoot_client import ChatwootClient


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
async def test_get_or_create_conversation_creates_when_none(client: ChatwootClient) -> None:
    """Should create a conversation when no open one exists."""
    respx.get("https://chatwoot.example.com/api/v1/accounts/1/contacts/42/conversations").mock(
        return_value=httpx.Response(200, json={"payload": []})
    )
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations").mock(
        return_value=httpx.Response(200, json={"id": 15, "status": "open"})
    )

    conv_id = await client.get_or_create_conversation(42)
    assert conv_id == 15


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
    respx.post("https://chatwoot.example.com/api/v1/accounts/1/conversations/20/messages").mock(
        return_value=httpx.Response(200, json={"id": 301, "content": "Note"})
    )

    await client.mirror_outbound_as_note("+49111222333", "Note", contact_name="Alice Müller")

    assert search_mock.called
    assert create_mock.called
    create_body = json.loads(create_mock.calls[0].request.content)
    assert create_body["name"] == "Alice Müller"
