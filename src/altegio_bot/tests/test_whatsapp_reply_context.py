"""Tests for native WhatsApp customer reply → Chatwoot native reply (PR1).

Covers:
- context.id / messages[0].id extraction and normalization;
- ReplyContextTarget lookup over operator-relay OutboxMessage rows;
- native-first forwarding with the same-conversation guard;
- visible fallback quote when no safe native mapping exists;
- fixed Chatwoot-origin detection (chatwoot_conversation_id is not a signal);
- new schema columns and the migration's revision chain.
"""

from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from altegio_bot.models.models import (
    Client,
    OutboxMessage,
    WhatsAppEvent,
    WhatsAppSender,
)
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.workers.whatsapp_inbox_worker import (
    ReplyContextTarget,
    WhatsAppReplyContextTarget,
    _event_origin_for_metrics,
    _extract_actions,
    _format_reply_context_prefix,
    _get_reply_context_target,
    _get_whatsapp_reply_context_target,
    _is_chatwoot_origin,
    _is_operator_relay,
    _normalize_reply_context_id,
    handle_event,
)

PHONE_NUMBER_ID = "PNID_REPLY"
FROM_PHONE = "49222333444"
PHONE_E164 = "+49222333444"


class _CaptureProvider(WhatsAppProvider):
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
        return "wamid.ACK"


def _meta_payload(
    text: str,
    *,
    from_phone: str = FROM_PHONE,
    wamid: str = "wamid.REPLY1",
    context_id: Any = None,
) -> dict[str, Any]:
    msg: dict[str, Any] = {
        "from": from_phone,
        "id": wamid,
        "timestamp": "1700000000",
        "type": "text",
        "text": {"body": text},
    }
    if context_id is not None:
        msg["context"] = {"id": context_id}
    return {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "metadata": {"phone_number_id": PHONE_NUMBER_ID},
                            "messages": [msg],
                        }
                    }
                ]
            }
        ]
    }


def _mock_chatwoot_client(
    conversation_id: int = 277,
    message_id: int = 9001,
) -> tuple[MagicMock, MagicMock]:
    inst = MagicMock()
    inst.get_or_create_incoming_conversation = AsyncMock(return_value=conversation_id)
    inst.send_message = AsyncMock(return_value=message_id)
    inst.aclose = AsyncMock(return_value=None)
    cls = MagicMock(return_value=inst)
    return cls, inst


def _operator_outbox(
    *,
    wamid: str = "wamid.OP1",
    phone_e164: str = PHONE_E164,
    chatwoot_message_id: int | None = 7644,
    chatwoot_conversation_id: int | None = 277,
    body: str = "На 9:00?",
    message_source: str = "operator",
    created_at: datetime | None = None,
) -> OutboxMessage:
    now = datetime.now(timezone.utc)
    ob = OutboxMessage(
        company_id=1,
        phone_e164=phone_e164,
        template_code="operator_relay",
        language="de",
        body=body,
        status="sent",
        provider_message_id=wamid,
        scheduled_at=now,
        sent_at=now,
        message_source=message_source,
        chatwoot_message_id=chatwoot_message_id,
        chatwoot_conversation_id=chatwoot_conversation_id,
        meta={},
    )
    if created_at is not None:
        ob.created_at = created_at
    return ob


def _make_event(payload: dict[str, Any], dedupe_key: str = "wa:reply-test") -> WhatsAppEvent:
    return WhatsAppEvent(
        dedupe_key=dedupe_key,
        status="received",
        error=None,
        query={},
        headers={},
        payload=payload,
    )


def _forwarded_whatsapp_event(
    *,
    dedupe_key: str = "wa:forwarded-reply-target",
    from_phone: str = FROM_PHONE,
    chatwoot_message_id: int | None = 4960,
    chatwoot_conversation_id: int | None = 230,
    whatsapp_message_id: str | None = "wamid.INBOUND",
) -> WhatsAppEvent:
    return WhatsAppEvent(
        dedupe_key=dedupe_key,
        status="processed",
        error=None,
        query={},
        headers={},
        payload=_meta_payload(
            "Message test",
            from_phone=from_phone,
            wamid=whatsapp_message_id or "wamid.MISSING_COLUMN",
        ),
        chatwoot_message_id=chatwoot_message_id,
        forwarded_chatwoot_conversation_id=chatwoot_conversation_id,
        whatsapp_message_id=whatsapp_message_id,
    )


# ---------------------------------------------------------------------------
# _normalize_reply_context_id
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        ("wamid.X", "wamid.X"),
        ("  wamid.X  ", "wamid.X"),
        ("", None),
        ("   ", None),
        (123, None),
        ({"id": "x"}, None),
        (None, None),
        (True, None),
    ],
)
def test_normalize_reply_context_id(value: Any, expected: str | None) -> None:
    assert _normalize_reply_context_id(value) == expected


# ---------------------------------------------------------------------------
# _extract_actions: context.id / whatsapp_message_id
# ---------------------------------------------------------------------------


def test_extract_actions_with_reply_context() -> None:
    payload = _meta_payload("Вот это время", wamid="wamid.IN1", context_id="wamid.OP1")
    actions = _extract_actions(payload)
    assert len(actions) == 1
    assert actions[0]["reply_to_provider_message_id"] == "wamid.OP1"
    assert actions[0]["whatsapp_message_id"] == "wamid.IN1"
    assert actions[0]["text"] == "Вот это время"
    assert actions[0]["cmd"] is None


def test_extract_actions_without_context() -> None:
    payload = _meta_payload("Привет", wamid="wamid.IN2")
    actions = _extract_actions(payload)
    assert len(actions) == 1
    assert actions[0]["reply_to_provider_message_id"] is None
    assert actions[0]["whatsapp_message_id"] == "wamid.IN2"


@pytest.mark.parametrize("bad_context_id", [42, {"x": 1}, "   ", ""])
def test_extract_actions_malformed_context_id(bad_context_id: Any) -> None:
    payload = _meta_payload("Hi", context_id=bad_context_id)
    actions = _extract_actions(payload)
    assert actions[0]["reply_to_provider_message_id"] is None


def test_extract_actions_non_dict_context() -> None:
    payload = _meta_payload("Hi")
    payload["entry"][0]["changes"][0]["value"]["messages"][0]["context"] = "not-a-dict"
    actions = _extract_actions(payload)
    assert actions[0]["reply_to_provider_message_id"] is None


def test_extract_actions_command_with_context_keeps_cmd() -> None:
    payload = _meta_payload("stop", context_id="wamid.OP1")
    actions = _extract_actions(payload)
    assert actions[0]["cmd"] == "stop"
    assert actions[0]["reply_to_provider_message_id"] == "wamid.OP1"


# ---------------------------------------------------------------------------
# _format_reply_context_prefix
# ---------------------------------------------------------------------------


def test_prefix_generic_when_no_body() -> None:
    assert _format_reply_context_prefix(None) == "↩️ Ответ на сообщение в WhatsApp"
    assert _format_reply_context_prefix("") == "↩️ Ответ на сообщение в WhatsApp"


def test_prefix_image_marker() -> None:
    assert _format_reply_context_prefix("[image]") == "↩️ Ответ на изображение"


def test_prefix_quotes_body() -> None:
    assert _format_reply_context_prefix("На 9:00?") == "↩️ Ответ на сообщение:\n«На 9:00?»"


def test_prefix_truncates_long_body() -> None:
    long_body = "x" * 500
    result = _format_reply_context_prefix(long_body)
    assert result.endswith("…»")
    assert len(result) < 350


# ---------------------------------------------------------------------------
# _get_reply_context_target (DB lookup)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reply_target_found(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(_operator_outbox())

        target = await _get_reply_context_target(session, "wamid.OP1", phone_e164=PHONE_E164)

    assert target == ReplyContextTarget(
        chatwoot_message_id=7644,
        chatwoot_conversation_id=277,
        body="На 9:00?",
    )


@pytest.mark.asyncio
async def test_reply_target_wrong_phone_returns_none(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(_operator_outbox())

        target = await _get_reply_context_target(session, "wamid.OP1", phone_e164="+49000000000")

    assert target is None


@pytest.mark.asyncio
async def test_reply_target_bot_source_not_matched(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(_operator_outbox(message_source="bot"))

        target = await _get_reply_context_target(session, "wamid.OP1", phone_e164=PHONE_E164)

    assert target is None


@pytest.mark.asyncio
async def test_reply_target_missing_args_returns_none(session_maker) -> None:
    async with session_maker() as session:
        assert await _get_reply_context_target(session, None, phone_e164=PHONE_E164) is None
        assert await _get_reply_context_target(session, "wamid.OP1", phone_e164=None) is None
        assert await _get_reply_context_target(session, "", phone_e164="") is None


@pytest.mark.asyncio
async def test_reply_target_old_row_without_chatwoot_ids(session_maker) -> None:
    """Pre-migration rows (no chatwoot ids) still expose body for the quote."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                _operator_outbox(
                    chatwoot_message_id=None,
                    chatwoot_conversation_id=None,
                    body="Старое сообщение",
                )
            )

        target = await _get_reply_context_target(session, "wamid.OP1", phone_e164=PHONE_E164)

    assert target is not None
    assert target.chatwoot_message_id is None
    assert target.body == "Старое сообщение"


@pytest.mark.asyncio
async def test_reply_target_newest_row_wins(session_maker) -> None:
    old = datetime.now(timezone.utc) - timedelta(days=2)
    new = datetime.now(timezone.utc) - timedelta(hours=1)
    async with session_maker() as session:
        async with session.begin():
            session.add(_operator_outbox(chatwoot_message_id=1, body="old", created_at=old))
            session.add(_operator_outbox(chatwoot_message_id=2, body="new", created_at=new))

        target = await _get_reply_context_target(session, "wamid.OP1", phone_e164=PHONE_E164)

    assert target is not None
    assert target.chatwoot_message_id == 2
    assert target.body == "new"


# ---------------------------------------------------------------------------
# _get_whatsapp_reply_context_target (Chatwoot Reply → WhatsApp wamid)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_whatsapp_reply_target_finds_inbound_event(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(_forwarded_whatsapp_event())

        target = await _get_whatsapp_reply_context_target(
            session,
            4960,
            chatwoot_conversation_id=230,
            phone_e164=PHONE_E164,
        )

    assert target == WhatsAppReplyContextTarget(
        provider_message_id="wamid.INBOUND",
        source="whatsapp_event",
    )


@pytest.mark.asyncio
async def test_whatsapp_reply_target_wrong_conversation_returns_none(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(_forwarded_whatsapp_event(chatwoot_conversation_id=999))

        target = await _get_whatsapp_reply_context_target(
            session,
            4960,
            chatwoot_conversation_id=230,
            phone_e164=PHONE_E164,
        )

    assert target is None


@pytest.mark.asyncio
async def test_whatsapp_reply_target_wrong_phone_returns_none(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(_forwarded_whatsapp_event(from_phone="49000000000"))

        target = await _get_whatsapp_reply_context_target(
            session,
            4960,
            chatwoot_conversation_id=230,
            phone_e164=PHONE_E164,
        )

    assert target is None


@pytest.mark.asyncio
async def test_whatsapp_reply_target_missing_whatsapp_message_id_returns_none(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(_forwarded_whatsapp_event(whatsapp_message_id=None))

        target = await _get_whatsapp_reply_context_target(
            session,
            4960,
            chatwoot_conversation_id=230,
            phone_e164=PHONE_E164,
        )

    assert target is None


@pytest.mark.asyncio
async def test_whatsapp_reply_target_chatwoot_origin_event_not_matched(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(
                _forwarded_whatsapp_event(
                    dedupe_key="chatwoot:230:4960",
                    whatsapp_message_id="wamid.CHATWOOT_MIRROR",
                )
            )

        target = await _get_whatsapp_reply_context_target(
            session,
            4960,
            chatwoot_conversation_id=230,
            phone_e164=PHONE_E164,
        )

    assert target is None


@pytest.mark.asyncio
async def test_whatsapp_reply_target_finds_previous_operator_outbox(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(
                _operator_outbox(
                    wamid="wamid.OPERATOR",
                    chatwoot_message_id=4964,
                    chatwoot_conversation_id=230,
                )
            )

        target = await _get_whatsapp_reply_context_target(
            session,
            4964,
            chatwoot_conversation_id=230,
            phone_e164=PHONE_E164,
        )

    assert target == WhatsAppReplyContextTarget(
        provider_message_id="wamid.OPERATOR",
        source="outbox_operator",
    )


@pytest.mark.asyncio
async def test_whatsapp_reply_target_bot_outbox_not_matched(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(
                _operator_outbox(
                    wamid="wamid.BOT",
                    chatwoot_message_id=4964,
                    chatwoot_conversation_id=230,
                    message_source="bot",
                )
            )

        target = await _get_whatsapp_reply_context_target(
            session,
            4964,
            chatwoot_conversation_id=230,
            phone_e164=PHONE_E164,
        )

    assert target is None


# ---------------------------------------------------------------------------
# Native-first forwarding through handle_event
# ---------------------------------------------------------------------------


async def _run_forward(
    session_maker,
    *,
    payload: dict[str, Any],
    outbox: OutboxMessage | None = None,
    destination_conversation_id: int = 277,
) -> tuple[WhatsAppEvent, MagicMock]:
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    company_id=1,
                    sender_code="default",
                    phone_number_id=PHONE_NUMBER_ID,
                    display_phone="+49",
                    is_active=True,
                )
            )
            if outbox is not None:
                session.add(outbox)

            evt = _make_event(payload)
            session.add(evt)
            await session.flush()

            mock_cls, mock_inst = _mock_chatwoot_client(
                conversation_id=destination_conversation_id,
            )
            with patch(
                "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                mock_cls,
            ):
                await handle_event(session, evt, provider)

    return evt, mock_inst


@pytest.mark.asyncio
async def test_native_reply_same_conversation(session_maker) -> None:
    """Reply target in the destination conversation → native in_reply_to."""
    evt, cw = await _run_forward(
        session_maker,
        payload=_meta_payload("Вот это время", wamid="wamid.REPLY1", context_id="wamid.OP1"),
        outbox=_operator_outbox(chatwoot_message_id=7644, chatwoot_conversation_id=277),
        destination_conversation_id=277,
    )

    cw.send_message.assert_called_once()
    call = cw.send_message.call_args
    assert call.args[0] == 277
    assert call.args[1] == "Вот это время"
    assert call.kwargs["message_type"] == "incoming"
    assert call.kwargs["content_attributes"] == {
        "in_reply_to": 7644,
        "in_reply_to_external_id": "wamid.OP1",
    }

    assert evt.forwarded_chatwoot_conversation_id == 277
    assert evt.chatwoot_message_id == 9001
    assert evt.chatwoot_conversation_id is None
    assert evt.whatsapp_message_id == "wamid.REPLY1"
    assert evt.error is None


@pytest.mark.asyncio
async def test_cross_conversation_falls_back_to_quote(session_maker) -> None:
    """Target in another conversation → no native ref, visible quote instead."""
    evt, cw = await _run_forward(
        session_maker,
        payload=_meta_payload("Вот это время", context_id="wamid.OP1"),
        outbox=_operator_outbox(chatwoot_message_id=7644, chatwoot_conversation_id=100),
        destination_conversation_id=277,
    )

    call = cw.send_message.call_args
    assert call.kwargs["content_attributes"] is None
    content = call.args[1]
    assert content.startswith("↩️ Ответ на сообщение:\n«На 9:00?»")
    assert content.endswith("Вот это время")
    assert evt.forwarded_chatwoot_conversation_id == 277


@pytest.mark.asyncio
async def test_missing_mapping_falls_back_to_generic_quote(session_maker) -> None:
    """context.id without any OutboxMessage → generic prefix, still delivered."""
    evt, cw = await _run_forward(
        session_maker,
        payload=_meta_payload("Вот это время", context_id="wamid.UNKNOWN"),
        outbox=None,
    )

    call = cw.send_message.call_args
    assert call.kwargs["content_attributes"] is None
    assert call.args[1] == "↩️ Ответ на сообщение в WhatsApp\n\nВот это время"
    assert evt.error is None
    assert evt.chatwoot_message_id == 9001


@pytest.mark.asyncio
async def test_old_row_without_native_id_quotes_body(session_maker) -> None:
    """Target without chatwoot_message_id → fallback quote with its body."""
    evt, cw = await _run_forward(
        session_maker,
        payload=_meta_payload("Вот это время", context_id="wamid.OP1"),
        outbox=_operator_outbox(
            chatwoot_message_id=None,
            chatwoot_conversation_id=None,
            body="Старое сообщение",
        ),
    )

    call = cw.send_message.call_args
    assert call.kwargs["content_attributes"] is None
    assert call.args[1].startswith("↩️ Ответ на сообщение:\n«Старое сообщение»")


@pytest.mark.asyncio
async def test_plain_text_without_context_forwarded_unchanged(session_maker) -> None:
    evt, cw = await _run_forward(
        session_maker,
        payload=_meta_payload("Привет"),
    )

    call = cw.send_message.call_args
    assert call.args[1] == "Привет"
    assert call.kwargs["content_attributes"] is None


@pytest.mark.asyncio
async def test_stop_command_with_context_not_forwarded(session_maker) -> None:
    """STOP with context.id keeps command behavior and is not forwarded."""
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    company_id=1,
                    sender_code="default",
                    phone_number_id=PHONE_NUMBER_ID,
                    display_phone="+49",
                    is_active=True,
                )
            )
            session.add(
                Client(
                    company_id=1,
                    altegio_client_id=2001,
                    phone_e164=PHONE_E164,
                    wa_opted_out=False,
                )
            )

            evt = _make_event(
                _meta_payload("stop", context_id="wamid.OP1"),
                dedupe_key="wa:reply-stop",
            )
            session.add(evt)
            await session.flush()

            mock_cls, mock_inst = _mock_chatwoot_client()
            with patch(
                "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                mock_cls,
            ):
                await handle_event(session, evt, provider)

        async with session.begin():
            client = await session.scalar(select(Client).where(Client.phone_e164 == PHONE_E164))

    # Command executed: opt-out set and ack sent through the provider.
    assert client is not None
    assert client.wa_opted_out is True
    assert provider.sent
    # Not forwarded to Chatwoot as a normal message.
    mock_inst.send_message.assert_not_called()
    mock_inst.get_or_create_incoming_conversation.assert_not_called()
    # Audit wamid is still recorded for the Meta-origin event.
    assert evt.whatsapp_message_id == "wamid.REPLY1"


@pytest.mark.asyncio
async def test_forward_failure_sets_safe_error(session_maker) -> None:
    """Chatwoot failure must store a class-name-only error and raise."""
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    company_id=1,
                    sender_code="default",
                    phone_number_id=PHONE_NUMBER_ID,
                    display_phone="+49",
                    is_active=True,
                )
            )
            evt = _make_event(_meta_payload("Привет"), dedupe_key="wa:reply-fail")
            session.add(evt)
            await session.flush()

            mock_cls, mock_inst = _mock_chatwoot_client()
            mock_inst.send_message = AsyncMock(side_effect=ValueError("secret-token http://internal"))
            with patch(
                "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                mock_cls,
            ):
                with pytest.raises(RuntimeError):
                    await handle_event(session, evt, provider)

    assert evt.error == "chatwoot forward failed: ValueError"
    assert "secret-token" not in (evt.error or "")
    assert evt.forwarded_chatwoot_conversation_id is None


# ---------------------------------------------------------------------------
# Origin detection (fixed)
# ---------------------------------------------------------------------------


def test_origin_chatwoot_payload_marker() -> None:
    evt = _make_event({"_chatwoot": {"conversation_id": 1}}, dedupe_key="wa:x")
    assert _is_chatwoot_origin(evt, evt.payload) is True


def test_origin_chatwoot_dedupe_prefix() -> None:
    evt = _make_event({}, dedupe_key="chatwoot:1:2")
    assert _is_chatwoot_origin(evt, evt.payload) is True


def test_origin_conversation_id_alone_is_not_a_signal() -> None:
    evt = _make_event(_meta_payload("Hi"), dedupe_key="wa:meta-1")
    evt.chatwoot_conversation_id = 42
    assert _is_chatwoot_origin(evt, evt.payload) is False


def test_origin_forwarded_conversation_id_is_not_a_signal() -> None:
    """A forwarded Meta-origin event must stay Meta-origin on reprocess."""
    evt = _make_event(_meta_payload("Hi"), dedupe_key="wa:meta-2")
    evt.forwarded_chatwoot_conversation_id = 277
    evt.chatwoot_message_id = 9001
    assert _is_chatwoot_origin(evt, evt.payload) is False


# ---------------------------------------------------------------------------
# Observability origin classification (separate from loop-prevention origin)
# ---------------------------------------------------------------------------


def _operator_relay_payload(text: str = "Wir erwarten Sie.") -> dict[str, Any]:
    return {
        "_chatwoot_operator_relay": {
            "recipient_phone": PHONE_E164,
            "text": text,
            "conversation_id": 30,
            "message_id": 40,
            "phone_number_id": PHONE_NUMBER_ID,
            "chatwoot_inbox_id": 1,
            "agent_name": "Boris",
        }
    }


def test_operator_relay_event_is_not_chatwoot_origin() -> None:
    """Loop-prevention semantics: operator relay must NOT be chatwoot-origin."""
    evt = _make_event(_operator_relay_payload(), dedupe_key="chatwoot_out:30:40")
    assert _is_operator_relay(evt.payload) is True
    assert _is_chatwoot_origin(evt, evt.payload) is False


def test_metrics_origin_operator_relay() -> None:
    """Operator relay gets its own metrics label, not the noisy 'meta'."""
    evt = _make_event(_operator_relay_payload(), dedupe_key="chatwoot_out:30:40")
    assert _event_origin_for_metrics(evt, evt.payload) == "chatwoot_operator_relay"


def test_metrics_origin_chatwoot() -> None:
    evt = _make_event({"_chatwoot": {"conversation_id": 1}}, dedupe_key="chatwoot:1:2")
    assert _event_origin_for_metrics(evt, evt.payload) == "chatwoot"


def test_metrics_origin_meta() -> None:
    evt = _make_event(_meta_payload("Hi"), dedupe_key="wa:meta-3")
    evt.chatwoot_conversation_id = 42  # source-only marker, not an origin signal
    assert _event_origin_for_metrics(evt, evt.payload) == "meta"


@pytest.mark.asyncio
async def test_operator_relay_runs_before_inbound_and_not_forwarded(session_maker) -> None:
    """Operator relay path runs first and is never forwarded back as inbound.

    Delivery: the operator text is sent to Meta (one provider.send) and an
    OutboxMessage(message_source='operator') is created.  It must NOT be
    forwarded into Chatwoot as inbound customer text.
    """
    from altegio_bot.settings import settings as _s

    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    company_id=1,
                    sender_code="default",
                    phone_number_id=PHONE_NUMBER_ID,
                    display_phone="+49",
                    is_active=True,
                )
            )
            # Recent customer inbound opens the 24h window so the relay sends
            # the operator text directly (instead of the closed-window note).
            # No msg timestamp → the window falls back to received_at (1h ago).
            session.add(
                WhatsAppEvent(
                    dedupe_key="wa:win-open-relay",
                    received_at=datetime.now(timezone.utc) - timedelta(hours=1),
                    status="processed",
                    query={},
                    headers={},
                    payload={
                        "entry": [
                            {
                                "changes": [
                                    {
                                        "value": {
                                            "metadata": {"phone_number_id": PHONE_NUMBER_ID},
                                            "messages": [
                                                {
                                                    "from": FROM_PHONE,
                                                    "type": "text",
                                                    "text": {"body": "Hallo"},
                                                    "id": "wamid.WINOPEN",
                                                }
                                            ],
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                )
            )
            evt = _make_event(_operator_relay_payload("Bis morgen"), dedupe_key="chatwoot_out:30:40")
            session.add(evt)
            await session.flush()

            mock_cls, mock_inst = _mock_chatwoot_client()
            original = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = True
            try:
                with patch(
                    "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                    mock_cls,
                ):
                    await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = original

        async with session.begin():
            outbox = await session.scalar(select(OutboxMessage).where(OutboxMessage.message_source == "operator"))

    # Sent to Meta exactly once as the operator's text.
    assert provider.sent
    assert provider.sent[0][2] == "Bis morgen"
    assert outbox is not None
    assert outbox.chatwoot_conversation_id == 30
    assert outbox.chatwoot_message_id == 40
    # Never forwarded back into Chatwoot as inbound customer text.
    mock_inst.get_or_create_incoming_conversation.assert_not_called()
    mock_inst.send_message.assert_not_called()


# ---------------------------------------------------------------------------
# Schema / migration
# ---------------------------------------------------------------------------


def test_models_have_reply_context_columns() -> None:
    we_cols = WhatsAppEvent.__table__.columns
    assert "chatwoot_message_id" in we_cols
    assert "forwarded_chatwoot_conversation_id" in we_cols
    assert "whatsapp_message_id" in we_cols
    assert we_cols["chatwoot_message_id"].nullable
    assert we_cols["forwarded_chatwoot_conversation_id"].nullable
    assert we_cols["whatsapp_message_id"].nullable

    ob_cols = OutboxMessage.__table__.columns
    assert "chatwoot_message_id" in ob_cols
    assert "chatwoot_conversation_id" in ob_cols
    assert ob_cols["chatwoot_message_id"].nullable
    assert ob_cols["chatwoot_conversation_id"].nullable


_VERSIONS_DIR = Path(__file__).resolve().parents[3] / "alembic" / "versions"


def _load_migration_module(filename: str, mod_name: str):
    path = _VERSIONS_DIR / filename
    spec = importlib.util.spec_from_file_location(mod_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, path


def _load_migration():
    return _load_migration_module(
        "c9d0e1f2a3b4_add_chatwoot_reply_context_columns.py",
        "reply_context_migration",
    )


def _load_index_migration():
    return _load_migration_module(
        "d0e1f2a3b4c5_add_reply_context_lookup_index.py",
        "reply_context_index_migration",
    )


def test_migration_revision_chain() -> None:
    module, _ = _load_migration()
    assert module.revision == "c9d0e1f2a3b4"
    assert module.down_revision == "b7c8d9e0f1a2"


def test_migration_is_idempotent_style() -> None:
    _, path = _load_migration()
    source = path.read_text()
    assert "ADD COLUMN IF NOT EXISTS" in source
    assert "CREATE INDEX IF NOT EXISTS" in source
    assert "DROP INDEX IF EXISTS" in source
    assert "DROP COLUMN IF EXISTS" in source
    # Backfill must stay PostgreSQL-only.
    assert 'bind.dialect.name != "postgresql"' in source


def test_columns_migration_does_not_own_composite_index() -> None:
    """The columns migration must NOT manage the composite lookup index.

    It moved to the dedicated follow-up migration so it is applied even in
    environments where c9d0e1f2a3b4 was already applied in-place.
    """
    _, path = _load_migration()
    source = path.read_text()
    assert "ix_outbox_messages_reply_context_lookup" not in source
    # Single-column indexes still belong here.
    assert "ix_outbox_messages_chatwoot_conversation_id" in source
    assert "ix_outbox_messages_chatwoot_message_id" in source


def test_index_migration_revision_chain() -> None:
    module, _ = _load_index_migration()
    assert module.revision == "d0e1f2a3b4c5"
    assert module.down_revision == "c9d0e1f2a3b4"
    assert module.branch_labels is None
    assert module.depends_on is None


def test_index_migration_declares_reply_context_composite_index() -> None:
    """The follow-up migration creates and drops only the composite index."""
    _, path = _load_index_migration()
    source = path.read_text()

    upgrade_src, downgrade_src = source.split("def downgrade")

    # Created idempotently in upgrade with the exact name and mandatory filters.
    assert "CREATE INDEX IF NOT EXISTS ix_outbox_messages_reply_context_lookup" in upgrade_src
    assert "provider_message_id, phone_e164, message_source, created_at DESC, id DESC" in upgrade_src
    assert "WHERE provider_message_id IS NOT NULL" in upgrade_src

    # Dropped idempotently in downgrade.
    assert "DROP INDEX IF EXISTS ix_outbox_messages_reply_context_lookup" in downgrade_src

    # The follow-up migration must not touch columns or single-column indexes.
    assert "DROP COLUMN" not in source
    assert "ADD COLUMN" not in source
    assert "ix_outbox_messages_chatwoot_conversation_id" not in source
    assert "ix_outbox_messages_chatwoot_message_id" not in source


def test_single_alembic_head() -> None:
    """The follow-up migration must leave exactly one Alembic head."""
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    cfg = Config(str(Path(__file__).resolve().parents[3] / "alembic.ini"))
    cfg.set_main_option("script_location", str(Path(__file__).resolve().parents[3] / "alembic"))
    script = ScriptDirectory.from_config(cfg)
    heads = list(script.get_heads())
    assert heads == ["d8f6e4c2b1a0"], f"Expected single head d8f6e4c2b1a0, got {heads}"
