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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from altegio_bot.chatwoot_outbox_route import CHATWOOT_ROUTE_META_KEY
from altegio_bot.models.models import (
    Client,
    OutboxMessage,
    PromoLead,
    WhatsAppEvent,
    WhatsAppSender,
)
from altegio_bot.providers.base import ChatwootRoute, WhatsAppProvider
from altegio_bot.providers.chatwoot_hybrid import ChatwootHybridProvider
from altegio_bot.settings import settings
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
    def __init__(self) -> None:
        self.notes: list[tuple[str, str]] = []

    async def mirror_outbound_as_note(
        self,
        phone: str,
        text: str,
        *,
        contact_name: str | None = None,
    ) -> None:
        self.notes.append((phone, text))

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


def _inbound_payload(
    phone_number_id: str,
    from_phone: str,
    text: str,
    *,
    wamid: str = "wamid.INBOUND_PROMO",
    context_id: str | None = None,
) -> dict[str, Any]:
    message: dict[str, Any] = {
        "from": from_phone,
        "id": wamid,
        "timestamp": "1700000000",
        "type": "text",
        "text": {"body": text},
    }
    if context_id is not None:
        message["context"] = {"id": context_id}
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
                            "messages": [message],
                        },
                    }
                ],
            }
        ],
    }


def _reaction_payload(
    phone_number_id: str,
    from_phone: str,
    *,
    wamid: str,
    target_wamid: str,
) -> dict[str, Any]:
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
                                    "id": wamid,
                                    "timestamp": "1700000001",
                                    "type": "reaction",
                                    "reaction": {"emoji": "👍", "message_id": target_wamid},
                                }
                            ],
                        }
                    }
                ]
            }
        ]
    }


def _mock_inbound_chatwoot(conversation_id: int, message_id: int) -> tuple[MagicMock, MagicMock]:
    client = MagicMock()
    client.get_or_create_incoming_conversation = AsyncMock(return_value=conversation_id)
    client.send_message = AsyncMock(return_value=message_id)
    client.aclose = AsyncMock(return_value=None)
    return MagicMock(return_value=client), client


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
        ("aktion bitte", "promo"),
        ("aktion\nbitte", "promo"),
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


def test_parse_command_uses_current_promo_secret_words() -> None:
    with patch.object(settings, "promo_secret_words", "neukunde"):
        assert _parse_command("neukunde bitte") == "promo"
        assert _parse_command("aktion") is None


# ---------------------------------------------------------------------------
# B. Inbound promo command sends free-form text (not a Meta template)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_promo_command_sends_free_form_reply(session_maker) -> None:
    # Superseded by full coverage in test_whatsapp_promo_leads.py.
    # Kept here as a smoke-test: promo command triggers a send (any text).
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
    # The promo lead handler sends German text; the exact copy lives in
    # promo_lead_handler.py and is tested in test_whatsapp_promo_leads.py.
    assert sent_text  # non-empty free-form reply
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

            with patch(
                "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                return_value=_FakeCW(),
            ):
                await handle_event(session, evt, provider)

    async with session_maker() as s2:
        # Funnel is disabled by default (promo_lead_funnel_enabled=False).
        # The informational handler produces template_code='wa_promo_info'.
        result = await s2.execute(select(OutboxMessage).where(OutboxMessage.template_code == "wa_promo_info"))
        outbox = result.scalar_one_or_none()

    assert outbox is not None, "OutboxMessage for promo command must be created"
    assert outbox.template_code == "wa_promo_info"
    assert outbox.message_source == "bot"
    assert outbox.status == "sent"
    assert outbox.provider_message_id == _CaptureProvider.wamid
    assert outbox.phone_e164 == PHONE_E164

    meta = outbox.meta or {}
    assert meta.get("source") == "promo_lead"
    assert meta.get("command") == "promo"


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


@pytest.mark.parametrize(
    ("text", "funnel_enabled", "expected_template", "followup_kind"),
    [
        ("STOP", False, "wa_cmd_stop", "reply"),
        ("STOP", False, "wa_cmd_stop", "reaction"),
        ("START", False, "wa_cmd_start", "reply"),
        ("START", False, "wa_cmd_start", "reaction"),
        ("AKTION", False, "wa_promo_info", "reply"),
        ("AKTION", False, "wa_promo_info", "reaction"),
        ("AKTION", True, "wa_promo_lead_issued", "reply"),
        ("AKTION", True, "wa_promo_lead_issued", "reaction"),
    ],
)
@pytest.mark.asyncio
async def test_identityless_direct_replies_use_only_explicit_general_with_shared_sender(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    text: str,
    funnel_enabled: bool,
    expected_template: str,
    followup_kind: str,
) -> None:
    """A shared phone_number_id must never turn _pick_sender() into tenant proof."""
    branch_map = (
        '{"101":{"provider":"altegio","company_id":71},'
        '"102":{"provider":"easyweek","company_id":72},'
        '"103":{"provider":"easyweek","company_id":73}}'
    )
    monkeypatch.setattr(settings, "chatwoot_inbox_company_map", branch_map)
    monkeypatch.setattr(settings, "chatwoot_inbox_id", 999)
    monkeypatch.setattr(settings, "promo_lead_funnel_enabled", funnel_enabled)
    monkeypatch.setattr(settings, "promo_check_new_client_in_altegio", False)
    monkeypatch.setattr(settings, "promo_async_eligibility_check_enabled", False)
    monkeypatch.setattr(settings, "promo_issue_loyalty_card_enabled", False)

    meta = _CaptureProvider()
    general = _FakeCW()
    branch_factory_calls: list[int] = []

    def _branch_factory(inbox_id: int) -> _FakeCW:
        branch_factory_calls.append(inbox_id)
        return _FakeCW()

    provider = ChatwootHybridProvider(
        primary=meta,  # type: ignore[arg-type]
        chatwoot=general,  # type: ignore[arg-type]
        chatwoot_factory=_branch_factory,  # type: ignore[arg-type]
    )
    opted_out_initially = text == "START"

    with caplog.at_level("INFO"):
        async with session_maker() as session:
            async with session.begin():
                session.add_all(
                    [
                        WhatsAppSender(
                            id=871,
                            provider="altegio",
                            company_id=71,
                            sender_code="shared_ka",
                            phone_number_id=PHONE_NUMBER_ID,
                            display_phone="+49",
                            is_active=True,
                        ),
                        WhatsAppSender(
                            id=872,
                            provider="easyweek",
                            company_id=72,
                            sender_code="shared_du",
                            phone_number_id=PHONE_NUMBER_ID,
                            display_phone="+49",
                            is_active=True,
                        ),
                        WhatsAppSender(
                            id=873,
                            provider="easyweek",
                            company_id=73,
                            sender_code="shared_ra",
                            phone_number_id=PHONE_NUMBER_ID,
                            display_phone="+49",
                            is_active=True,
                        ),
                        Client(
                            id=879,
                            company_id=71,
                            altegio_client_id=879,
                            display_name="General Route Customer",
                            phone_e164=PHONE_E164,
                            wa_opted_out=opted_out_initially,
                            wa_opt_out_reason="wa:stop" if opted_out_initially else None,
                            raw={},
                        ),
                    ]
                )
                event = WhatsAppEvent(
                    dedupe_key=f"wa:explicit-general:{text.lower()}:{int(funnel_enabled)}",
                    status="received",
                    error=None,
                    query={},
                    headers={},
                    payload=_inbound_payload(PHONE_NUMBER_ID, FROM_PHONE, text),
                )
                session.add(event)
                await session.flush()

                await handle_event(session, event, provider)
                await provider.aclose()

                outbox = (
                    await session.execute(select(OutboxMessage).where(OutboxMessage.template_code == expected_template))
                ).scalar_one_or_none()
                assert outbox is not None
                assert outbox.status == "sent"
                assert outbox.message_source == "bot"
                assert outbox.meta[CHATWOOT_ROUTE_META_KEY] == ChatwootRoute.GENERAL.value

                followup_wamid = f"wamid.GENERAL.{text}.{followup_kind}"
                followup_payload = (
                    _inbound_payload(
                        PHONE_NUMBER_ID,
                        FROM_PHONE,
                        "General follow-up",
                        wamid=followup_wamid,
                        context_id=outbox.provider_message_id,
                    )
                    if followup_kind == "reply"
                    else _reaction_payload(
                        PHONE_NUMBER_ID,
                        FROM_PHONE,
                        wamid=followup_wamid,
                        target_wamid=outbox.provider_message_id,
                    )
                )
                followup = WhatsAppEvent(
                    dedupe_key=f"wa:general-followup:{text.lower()}:{followup_kind}:{int(funnel_enabled)}",
                    status="received",
                    error=None,
                    query={},
                    headers={},
                    payload=followup_payload,
                )
                session.add(followup)
                await session.flush()

                general_cls, general_inbound = _mock_inbound_chatwoot(777, 8801)
                with patch("altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient", general_cls):
                    await handle_event(session, followup, meta)

                general_cls.assert_called_once_with(inbox_id=999)
                general_inbound.get_or_create_incoming_conversation.assert_called_once_with(
                    PHONE_E164,
                    contact_name=None,
                )
                assert followup.forwarded_chatwoot_conversation_id == 777
                assert followup.chatwoot_message_id == 8801
                assert followup.error is None

                chained = WhatsAppEvent(
                    dedupe_key=f"wa:general-chain:{text.lower()}:{followup_kind}:{int(funnel_enabled)}",
                    status="received",
                    error=None,
                    query={},
                    headers={},
                    payload=_inbound_payload(
                        PHONE_NUMBER_ID,
                        FROM_PHONE,
                        "Exact chained follow-up",
                        wamid=f"{followup_wamid}.chain",
                        context_id=followup_wamid,
                    ),
                )
                session.add(chained)
                await session.flush()

                exact_cls, exact_inbound = _mock_inbound_chatwoot(9999, 8802)
                with patch("altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient", exact_cls):
                    await handle_event(session, chained, meta)

                exact_cls.assert_called_once_with()
                exact_inbound.get_or_create_incoming_conversation.assert_not_called()
                assert exact_inbound.send_message.call_args.args[0] == 777
                assert chained.forwarded_chatwoot_conversation_id == 777
                assert chained.error is None

                customer = await session.get(Client, 879)
                assert customer is not None
                if text == "STOP":
                    assert customer.wa_opted_out is True
                    assert customer.wa_opt_out_reason == "wa:stop"
                elif text == "START":
                    assert customer.wa_opted_out is False
                    assert customer.wa_opt_out_reason is None
                elif funnel_enabled:
                    leads = (await session.execute(select(PromoLead))).scalars().all()
                    assert len(leads) == 1
                    assert leads[0].status == "issued"
                else:
                    assert (await session.execute(select(PromoLead))).scalars().all() == []

    assert len(meta.sent) == 1
    assert len(general.notes) == 1
    assert general.notes[0][0] == PHONE_E164
    assert branch_factory_calls == []
    assert PHONE_E164 not in caplog.text
    assert FROM_PHONE not in caplog.text
    assert branch_map not in caplog.text
    assert settings.promo_booking_url not in caplog.text
    assert "General Route Customer" not in caplog.text


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
