"""Tests: WhatsApp inbound marketing/promo command handling.

Covers:
A. _parse_command normalisation for promo keywords (aktion/angebot/rabatt)
B. Promo command sends a free-form text reply (not a Meta template)
C. OutboxMessage audit row created with correct fields
D. STOP/START regression: commands still parse and process correctly
E. Chatwoot-origin safety: promo command must not trigger a bot reply
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import patch

import pytest
from sqlalchemy import select

from altegio_bot.models.models import (
    OutboxMessage,
    WhatsAppEvent,
    WhatsAppSender,
)
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.workers.whatsapp_inbox_worker import (
    _parse_command,
    handle_event,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PHONE_NUMBER_ID = "PNID_PROMO"
FROM_PHONE = "4917600000001"
PHONE_E164 = "+4917600000001"


class _CaptureProvider(WhatsAppProvider):
    wamid = "wamid.PROMO_TEST"

    def __init__(self) -> None:
        self.sent: list[tuple[int, str, str]] = []

    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
        contact_name: str | None = None,
    ) -> str:
        self.sent.append((sender_id, phone_e164, text))
        return self.wamid


class _FakeCW:
    async def log_incoming_message(
        self,
        phone: str,
        text: str,
        contact_name: str | None = None,
    ) -> None:
        pass

    async def aclose(self) -> None:
        pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _inbound_payload(phone_number_id: str, from_phone: str, text: str) -> dict[str, Any]:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {"phone_number_id": phone_number_id},
                            "messages": [
                                {
                                    "from": from_phone,
                                    "id": "wamid.INBOUND_PROMO",
                                    "timestamp": "1700000000",
                                    "type": "text",
                                    "text": {"body": text},
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


# ---------------------------------------------------------------------------
# A. Unit tests: _parse_command / _norm_text
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [
        # promo keywords — all casings and with punctuation
        ("aktion", "promo"),
        ("AKTION", "promo"),
        (" Aktion! ", "promo"),
        ("aktion!", "promo"),
        ("  Aktion  ", "promo"),
        ("angebot", "promo"),
        ("ANGEBOT", "promo"),
        ("rabatt", "promo"),
        ("RABATT", "promo"),
        # STOP/START must remain unaffected
        ("stop", "stop"),
        ("STOP", "stop"),
        ("start", "start"),
        ("START", "start"),
        # unknown text → None
        ("hello", None),
        ("", None),
        ("unknown command", None),
    ],
)
def test_parse_command(text: str, expected: str | None) -> None:
    assert _parse_command(text) == expected


# ---------------------------------------------------------------------------
# B. Inbound promo command sends free-form text (not a Meta template)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_promo_command_sends_free_form_reply(session_maker) -> None:
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=201,
                    company_id=1,
                    sender_code="default",
                    phone_number_id=PHONE_NUMBER_ID,
                    display_phone="+49",
                    is_active=True,
                )
            )

            evt = WhatsAppEvent(
                dedupe_key="wa:promo-b-1",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_inbound_payload(PHONE_NUMBER_ID, FROM_PHONE, "AKTION!"),
            )
            session.add(evt)
            await session.flush()

            with patch(
                "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                return_value=_FakeCW(),
            ):
                await handle_event(session, evt, provider)

    assert provider.sent, "Expected provider.send() to be called for promo command"
    _sid, sent_phone, sent_text = provider.sent[0]
    assert sent_phone == PHONE_E164
    assert "10 % Rabatt" in sent_text
    assert evt.error is None


# ---------------------------------------------------------------------------
# C. OutboxMessage audit row created after promo command
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_promo_command_creates_outbox_audit(session_maker) -> None:
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=202,
                    company_id=1,
                    sender_code="default",
                    phone_number_id=PHONE_NUMBER_ID,
                    display_phone="+49",
                    is_active=True,
                )
            )

            evt = WhatsAppEvent(
                dedupe_key="wa:promo-c-1",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_inbound_payload(PHONE_NUMBER_ID, FROM_PHONE, "aktion"),
            )
            session.add(evt)
            await session.flush()
            event_id = int(evt.id)

            with patch(
                "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                return_value=_FakeCW(),
            ):
                await handle_event(session, evt, provider)

    async with session_maker() as s2:
        result = await s2.execute(select(OutboxMessage).where(OutboxMessage.template_code == "wa_cmd_promo"))
        outbox = result.scalar_one_or_none()

    assert outbox is not None, "OutboxMessage for promo command must be created"
    assert outbox.template_code == "wa_cmd_promo"
    assert outbox.message_source == "bot"
    assert outbox.status == "sent"
    assert outbox.provider_message_id == _CaptureProvider.wamid
    assert outbox.phone_e164 == PHONE_E164

    meta = outbox.meta or {}
    assert meta.get("source") == "inbound_command"
    assert meta.get("command") == "promo"
    assert meta.get("whatsapp_event_id") == event_id


# ---------------------------------------------------------------------------
# D. STOP/START regression: parse_command still returns correct values
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text,expected_cmd",
    [
        ("stop", "stop"),
        ("STOP", "stop"),
        ("Stop", "stop"),
        ("start", "start"),
        ("START", "start"),
    ],
)
def test_stop_start_parse_command_regression(text: str, expected_cmd: str) -> None:
    assert _parse_command(text) == expected_cmd


# ---------------------------------------------------------------------------
# E. Chatwoot-origin safety: promo command must not send bot reply
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_promo_command_skipped_for_chatwoot_origin(session_maker) -> None:
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=203,
                    company_id=1,
                    sender_code="default",
                    phone_number_id=PHONE_NUMBER_ID,
                    display_phone="+49",
                    is_active=True,
                )
            )

            # Mark as Chatwoot-origin via dedupe_key prefix
            evt = WhatsAppEvent(
                dedupe_key="chatwoot:test-promo-e-1",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_inbound_payload(PHONE_NUMBER_ID, FROM_PHONE, "aktion"),
            )
            session.add(evt)
            await session.flush()

            await handle_event(session, evt, provider)

    assert not provider.sent, "Promo command must NOT send a bot reply for Chatwoot-origin events"
    assert evt.error is None
