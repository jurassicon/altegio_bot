"""The worker logs that runbook §4.1 describes must contain no PII and no injection.

Two paths carry customer/agent data through `whatsapp_inbox_worker`:
  * WhatsApp → Chatwoot (forwarding an inbound message);
  * Chatwoot → WhatsApp (operator relay).

Both used to log the customer phone, the client name and the agent name at INFO.
Beyond the PII leak, those strings are sender-controlled, so a newline or an ANSI
escape inside them could forge an extra log line. These tests pin both properties
against the real `handle_event` code path.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest

import altegio_bot.workers.whatsapp_inbox_worker as worker_module
from altegio_bot.models.models import WhatsAppEvent, WhatsAppSender
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.workers.whatsapp_inbox_worker import handle_event

PHONE = "+4915199999999"
CLIENT_NAME = "PRIVATE CUSTOMER"
MESSAGE_TEXT = "STRENG GEHEIMER KUNDENTEXT"
AGENT_NAME = "Agent\n2026-07-24 INFO forged"
HOSTILE_CONV_ID = "501\nforged"
HOSTILE_MSG_ID = "5001\x1b[31m"

LINE_SEP = chr(0x2028)
PARA_SEP = chr(0x2029)


class _CaptureProvider(WhatsAppProvider):
    """Minimal provider stand-in: records sends, never touches the network."""

    wamid = "wamid.LOGTEST"

    def __init__(self) -> None:
        self.sent: list[tuple] = []

    async def send(self, sender_id, phone_e164, text, contact_name=None) -> str:
        self.sent.append((sender_id, phone_e164, text))
        return self.wamid

    async def send_template(self, *args, **kwargs) -> str:
        self.sent.append((args, kwargs))
        return self.wamid


class _FakeChatwoot:
    """Stands in for ChatwootClient so the forward path runs without network."""

    def __init__(self, *args, **kwargs) -> None:
        pass

    async def get_or_create_incoming_conversation(self, *args, **kwargs):
        return 4242

    async def send_message(self, *args, **kwargs):
        return 777

    async def aclose(self):
        return None


def _assert_no_pii_and_no_injection(caplog) -> str:
    messages = [r.getMessage() for r in caplog.records]
    blob = "\n".join(messages)

    # PII must not appear at all.
    assert PHONE not in blob
    assert CLIENT_NAME not in blob
    assert MESSAGE_TEXT not in blob
    assert "Agent" not in blob

    # Sender-controlled strings must not forge a log line or emit control bytes.
    for record_message in messages:
        assert "\n" not in record_message
        assert "\r" not in record_message
        assert "\x1b" not in record_message
        assert LINE_SEP not in record_message
        assert PARA_SEP not in record_message

    # NOTE: the literal word "forged" may still appear *inside* an escaped
    # field — that is fine and is exactly what escaping is for. What must never
    # happen is a raw newline turning it into its own physical log line, which
    # the per-record checks above already guarantee.
    return blob


@pytest.mark.asyncio
async def test_operator_relay_logs_carry_no_pii_or_injection(session_maker, monkeypatch, caplog) -> None:
    """Chatwoot → WhatsApp relay: no phone, no agent name, no message body."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=911,
                    company_id=1,
                    sender_code="default",
                    phone_number_id="PNID_LOGTEST",
                    display_phone="+49",
                    is_active=True,
                )
            )
            # Open the 24h window so the direct-text branch is exercised.
            now = datetime.now(timezone.utc)
            session.add(
                WhatsAppEvent(
                    dedupe_key="wa:inbound:logtest",
                    received_at=now - timedelta(hours=1),
                    status="processed",
                    query={},
                    headers={},
                    payload={
                        "entry": [
                            {
                                "changes": [
                                    {
                                        "value": {
                                            "messages": [
                                                {
                                                    "from": PHONE.lstrip("+"),
                                                    "type": "text",
                                                    "text": {"body": "Hallo"},
                                                    "id": "wamid.win",
                                                }
                                            ],
                                            "metadata": {"phone_number_id": "PNID_LOGTEST"},
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                )
            )
            relay_evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:logtest",
                status="received",
                error=None,
                query={},
                headers={},
                payload={
                    "_chatwoot_operator_relay": {
                        "recipient_phone": PHONE,
                        "text": MESSAGE_TEXT,
                        "conversation_id": HOSTILE_CONV_ID,
                        "message_id": HOSTILE_MSG_ID,
                        "phone_number_id": "PNID_LOGTEST",
                        "agent_name": AGENT_NAME,
                        "agent_id": 1,
                        "contact_name": CLIENT_NAME,
                    }
                },
            )
            session.add(relay_evt)
            await session.flush()

            with caplog.at_level(logging.DEBUG):
                await handle_event(session, relay_evt, provider)

    blob = _assert_no_pii_and_no_injection(caplog)
    # Technical identifiers stay available for tracing.
    assert "conv_id=" in blob
    assert "operator_relay" in blob


@pytest.mark.asyncio
async def test_incoming_forward_logs_carry_no_pii_or_injection(session_maker, monkeypatch, caplog) -> None:
    """WhatsApp → Chatwoot forwarding: no phone, no client name, no body."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=912,
                    company_id=1,
                    sender_code="default",
                    phone_number_id="PNID_FWD",
                    display_phone="+49",
                    is_active=True,
                )
            )
            evt = WhatsAppEvent(
                dedupe_key="wa:inbound:fwd:logtest",
                status="received",
                error=None,
                query={},
                headers={},
                payload={
                    "entry": [
                        {
                            "changes": [
                                {
                                    "value": {
                                        "messages": [
                                            {
                                                "from": PHONE.lstrip("+"),
                                                "type": "text",
                                                "text": {"body": MESSAGE_TEXT},
                                                "id": "wamid.fwd",
                                            }
                                        ],
                                        "contacts": [{"profile": {"name": CLIENT_NAME}}],
                                        "metadata": {"phone_number_id": "PNID_FWD"},
                                    }
                                }
                            ]
                        }
                    ]
                },
            )
            session.add(evt)
            await session.flush()

            with caplog.at_level(logging.DEBUG):
                await handle_event(session, evt, provider)

    _assert_no_pii_and_no_injection(caplog)


def test_worker_log_calls_never_reference_pii_variables() -> None:
    """Static guard: no logger call may take a PII variable as an argument.

    Complements the runtime tests above, which only cover the branches they
    actually execute — this one covers every branch in the module.
    """
    import ast
    from pathlib import Path

    src = Path(worker_module.__file__).read_text(encoding="utf-8")
    pii = {"phone_e164", "client_name", "agent_name", "recipient_phone", "raw_phone", "text"}

    offenders = []
    for node in ast.walk(ast.parse(src)):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in {"info", "warning", "error", "exception", "debug"}:
            continue
        if not (isinstance(node.func.value, ast.Name) and node.func.value.id == "logger"):
            continue
        hit = {a.id for a in node.args if isinstance(a, ast.Name)} & pii
        if hit:
            offenders.append((node.lineno, sorted(hit)))

    assert offenders == [], f"PII passed to logger at {offenders}"
