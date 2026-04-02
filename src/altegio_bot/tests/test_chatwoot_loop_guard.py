"""Regression tests for the chatwoot-origin loop guard in whatsapp_inbox_worker.

Scenarios:
  1. A Chatwoot-origin event must NOT call ChatwootClient.log_incoming_message
     (to prevent the feedback loop).
  2. A real Meta-origin event MUST call ChatwootClient.log_incoming_message
     (existing behaviour is preserved).
  3. A Chatwoot-origin event with a stop/start command must still process the
     command (opt-out logic must not be skipped by the loop guard).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from altegio_bot.models.models import Client, WhatsAppEvent, WhatsAppSender
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.workers.whatsapp_inbox_worker import handle_event


class _NoOpProvider(WhatsAppProvider):
    async def send(self, sender_id: int, phone_e164: str, text: str, contact_name: str | None = None) -> str:
        return "msg-noop"


def _meta_payload(phone_number_id: str, from_phone: str, text: str) -> dict[str, Any]:
    """Minimal Meta-style webhook payload (no _chatwoot marker)."""
    return {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "metadata": {"phone_number_id": phone_number_id},
                            "messages": [
                                {
                                    "from": from_phone,
                                    "id": "wamid.TEST",
                                    "timestamp": "1700000000",
                                    "type": "text",
                                    "text": {"body": text},
                                }
                            ],
                        }
                    }
                ]
            }
        ]
    }


def _chatwoot_payload(phone_number_id: str, from_phone: str, text: str, conv_id: int = 42) -> dict[str, Any]:
    """Normalized payload as produced by webhooks/chatwoot.py (includes _chatwoot marker)."""
    return {
        **_meta_payload(phone_number_id, from_phone, text),
        "_chatwoot": {
            "conversation_id": conv_id,
            "message_id": 99,
            "account_id": 1,
        },
    }


def _mock_chatwoot_client() -> tuple[MagicMock, MagicMock]:
    """Return (mock_class, mock_instance) for ChatwootClient."""
    mock_instance = MagicMock()
    mock_instance.log_incoming_message = AsyncMock(return_value=None)
    mock_instance.aclose = AsyncMock(return_value=None)
    mock_class = MagicMock(return_value=mock_instance)
    return mock_class, mock_instance


@pytest.mark.asyncio
async def test_chatwoot_origin_does_not_call_log_incoming_message(session_maker) -> None:
    """An event with _chatwoot in payload must not be forwarded back to Chatwoot."""
    provider = _NoOpProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=5,
                    company_id=1,
                    sender_code="default",
                    phone_number_id="PNID_CW",
                    display_phone="+49",
                    is_active=True,
                )
            )

            evt = WhatsAppEvent(
                dedupe_key="chatwoot:42:99",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_chatwoot_payload("PNID_CW", "49123456789", "Hello from Chatwoot"),
                chatwoot_conversation_id=42,
            )
            session.add(evt)
            await session.flush()

            mock_class, mock_instance = _mock_chatwoot_client()
            with patch(
                "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                mock_class,
            ):
                await handle_event(session, evt, provider)

    mock_instance.log_incoming_message.assert_not_called()


@pytest.mark.asyncio
async def test_real_wa_event_calls_log_incoming_message(session_maker) -> None:
    """A real Meta-origin event must be forwarded to Chatwoot as before."""
    provider = _NoOpProvider()
    phone = "+49987654321"

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=6,
                    company_id=1,
                    sender_code="default",
                    phone_number_id="PNID_META",
                    display_phone="+49",
                    is_active=True,
                )
            )

            evt = WhatsAppEvent(
                dedupe_key="wa:meta-real-001",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_meta_payload("PNID_META", "49987654321", "Hello from Meta"),
                chatwoot_conversation_id=None,
            )
            session.add(evt)
            await session.flush()

            mock_class, mock_instance = _mock_chatwoot_client()
            with patch(
                "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                mock_class,
            ):
                await handle_event(session, evt, provider)

    mock_instance.log_incoming_message.assert_called_once()
    call_kwargs = mock_instance.log_incoming_message.call_args
    assert call_kwargs.args[0] == phone or call_kwargs.kwargs.get("phone") == phone


@pytest.mark.asyncio
async def test_chatwoot_origin_stop_command_still_opts_out(session_maker) -> None:
    """A chatwoot-origin STOP event must not call log_incoming_message, but must
    still set wa_opted_out=True on the matching client record."""
    provider = _NoOpProvider()
    phone = "+49555000111"

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=7,
                    company_id=1,
                    sender_code="default",
                    phone_number_id="PNID_STOP",
                    display_phone="+49",
                    is_active=True,
                )
            )
            session.add(
                Client(
                    company_id=1,
                    altegio_client_id=1001,
                    phone_e164=phone,
                    wa_opted_out=False,
                )
            )

            evt = WhatsAppEvent(
                dedupe_key="chatwoot:55:100",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_chatwoot_payload("PNID_STOP", "49555000111", "stop", conv_id=55),
                chatwoot_conversation_id=55,
            )
            session.add(evt)
            await session.flush()

            mock_class, mock_instance = _mock_chatwoot_client()
            with patch(
                "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                mock_class,
            ):
                await handle_event(session, evt, provider)

        async with session.begin():
            client = await session.scalar(select(Client).where(Client.phone_e164 == phone))

    mock_instance.log_incoming_message.assert_not_called()
    assert client is not None
    assert client.wa_opted_out is True
