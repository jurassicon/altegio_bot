"""Worker must safely process an already-persisted WhatsAppEvent.

Webhook root/structure validation does not guarantee every nested field is
well-typed, and a stored event is replayed by `handle_event` regardless. These
tests cover the pure extractors directly (fast, exhaustive per branch) and the
full `handle_event` path end to end for the cases that must never crash or be
lost via a 200-then-failed acknowledgement.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import altegio_bot.workers.whatsapp_inbox_worker as worker_module
from altegio_bot.models.models import WhatsAppEvent, WhatsAppSender
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.webhooks.common import PG_BIGINT_MAX
from altegio_bot.workers.whatsapp_inbox_worker import (
    _extract_actions,
    _extract_message_text,
    _extract_status_updates,
    _is_operator_relay,
    handle_event,
)

# ---------------------------------------------------------------------------
# Pure extractors: malformed nested containers degrade instead of crashing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "msg",
    [
        {"type": "text", "text": []},
        {"type": "text", "text": "bad"},
        {"type": "text", "text": 123},
        {"type": "button", "button": "bad"},
        {"type": "button", "button": []},
        {"type": "interactive", "interactive": 123},
        {"type": "interactive", "interactive": "bad"},
        {"type": "interactive", "interactive": {"button_reply": []}},
        {"type": "interactive", "interactive": {"list_reply": "bad"}},
    ],
)
def test_extract_message_text_never_raises(msg: dict) -> None:
    assert _extract_message_text(msg) == ""


def test_extract_message_text_reads_valid_shapes() -> None:
    assert _extract_message_text({"type": "text", "text": {"body": "hi"}}) == "hi"
    assert _extract_message_text({"type": "button", "button": {"text": "Yes"}}) == "Yes"
    assert _extract_message_text({"type": "interactive", "interactive": {"button_reply": {"title": "OK"}}}) == "OK"


@pytest.mark.parametrize("bad_entry", [123, "abc", {"changes": 1}, 1.5, True])
def test_extract_actions_survives_malformed_entry(bad_entry: object) -> None:
    assert _extract_actions({"entry": bad_entry}) == []


def test_extract_actions_survives_malformed_inner_lists() -> None:
    payload = {"entry": [{"changes": "bad"}]}
    assert _extract_actions(payload) == []
    payload = {"entry": [{"changes": [{"value": {"messages": 123}}]}]}
    assert _extract_actions(payload) == []
    # A string where a list is expected must NOT be iterated character by char.
    payload = {"entry": [{"changes": [{"value": {"messages": "abc"}}]}]}
    assert _extract_actions(payload) == []


@pytest.mark.parametrize("bad", [123, "abc", 1.5, True, {"x": 1}])
def test_extract_status_updates_survives_malformed_containers(bad: object) -> None:
    assert _extract_status_updates({"entry": bad}) == []
    assert _extract_status_updates({"entry": [{"changes": bad}]}) == []
    assert _extract_status_updates({"entry": [{"changes": [{"value": {"statuses": bad}}]}]}) == []


# ---------------------------------------------------------------------------
# Operator relay marker must be a dict
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("marker", [[], "bad", 123, None, True, 1.5])
def test_is_operator_relay_requires_dict(marker: object) -> None:
    assert _is_operator_relay({"_chatwoot_operator_relay": marker}) is False


def test_is_operator_relay_accepts_dict() -> None:
    assert _is_operator_relay({"_chatwoot_operator_relay": {"recipient_phone": "+49"}}) is True


# ---------------------------------------------------------------------------
# End to end through handle_event
# ---------------------------------------------------------------------------


class _CaptureProvider(WhatsAppProvider):
    wamid = "wamid.HARDENING"

    def __init__(self) -> None:
        self.sent: list[tuple] = []
        self.send_calls: list[dict] = []

    async def send(self, sender_id, phone_e164, text, contact_name=None, **kwargs) -> str:
        # Accept and record the optional kwargs safe_send forwards
        # (company_id/staff_id/reply_to_provider_message_id) so tests can assert
        # a resolved reply context reached the provider.
        self.sent.append((sender_id, phone_e164, text))
        self.send_calls.append({"sender_id": sender_id, "phone_e164": phone_e164, "text": text, "kwargs": kwargs})
        return self.wamid

    async def send_template(self, *args, **kwargs) -> str:
        self.sent.append((args, kwargs))
        return self.wamid


class _FakeChatwoot:
    def __init__(self, *args, **kwargs) -> None:
        pass

    async def get_or_create_incoming_conversation(self, *args, **kwargs):
        return 4242

    async def send_message(self, *args, **kwargs):
        return 777

    async def aclose(self):
        return None


async def _add_open_window(session, phone: str, phone_number_id: str) -> None:
    """Store a fresh Meta inbound so the 24h operator window is open."""
    session.add(
        WhatsAppEvent(
            dedupe_key=f"wa:inbound:{phone_number_id}",
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
                                    "messages": [
                                        {"from": phone.lstrip("+"), "type": "text", "text": {"body": "hi"}, "id": "w1"}
                                    ],
                                    "metadata": {"phone_number_id": phone_number_id},
                                }
                            }
                        ]
                    }
                ]
            },
        )
    )


@pytest.mark.parametrize("hostile_id", ["9" * 5000, -1, "-42", 2**70, PG_BIGINT_MAX + 1])
@pytest.mark.asyncio
async def test_operator_relay_survives_hostile_ids(session_maker, monkeypatch, hostile_id) -> None:
    """An oversized/negative/out-of-range id must not crash background processing.

    The event must reach a terminal state (send attempted), not be lost through
    an unhandled exception.
    """
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    monkeypatch.setattr(worker_module, "SessionLocal", session_maker)
    provider = _CaptureProvider()
    phone = "+4915207156153"
    pnid = "PNID_HARDEN"

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=950,
                    company_id=1,
                    sender_code="default",
                    phone_number_id=pnid,
                    display_phone="+49",
                    is_active=True,
                )
            )
            await _add_open_window(session, phone, pnid)
            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:harden",
                status="received",
                error=None,
                query={},
                headers={},
                payload={
                    "_chatwoot_operator_relay": {
                        "recipient_phone": phone,
                        "text": "Hallo",
                        "conversation_id": hostile_id,
                        "message_id": hostile_id,
                        "phone_number_id": pnid,
                        "reply_to_chatwoot_message_id": hostile_id,
                        "agent_name": "A",
                    }
                },
            )
            session.add(evt)
            await session.flush()
            evt_id = evt.id

    # Must not raise, whatever the id.
    await process_one_event(evt_id, provider)

    # Window was open → free-form text sent. No unhandled exception, no loss.
    assert len(provider.sent) == 1
    async with session_maker() as session:
        reloaded = await session.get(WhatsAppEvent, evt_id)
    assert reloaded is not None


@pytest.mark.parametrize("marker", [[], "bad", 123])
@pytest.mark.asyncio
async def test_malformed_operator_marker_does_not_relay(session_maker, monkeypatch, marker) -> None:
    """A non-dict relay marker must not crash and must not send to Meta."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            evt = WhatsAppEvent(
                dedupe_key=f"wa:malformed:marker:{type(marker).__name__}",
                status="received",
                error=None,
                query={},
                headers={},
                payload={"_chatwoot_operator_relay": marker},
            )
            session.add(evt)
            await session.flush()

            await handle_event(session, evt, provider)

    assert provider.sent == []


@pytest.mark.asyncio
async def test_malformed_nested_message_does_not_crash_worker(session_maker, monkeypatch) -> None:
    """A stored inbound event with malformed nested containers must not crash."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            evt = WhatsAppEvent(
                dedupe_key="wa:malformed:nested",
                status="received",
                error=None,
                query={},
                headers={},
                payload={
                    "entry": [
                        {"changes": [{"value": {"messages": [{"type": "text", "text": [], "from": "4915207156153"}]}}]},
                        {"changes": "bad"},
                        "not-a-dict",
                    ]
                },
            )
            session.add(evt)
            await session.flush()

            # Must not raise despite the malformed containers.
            await handle_event(session, evt, provider)


# ---------------------------------------------------------------------------
# Full lifecycle via the production wrapper process_one_event
# ---------------------------------------------------------------------------

from sqlalchemy import select  # noqa: E402

from altegio_bot.models.models import OutboxMessage  # noqa: E402
from altegio_bot.workers.whatsapp_inbox_worker import process_one_event  # noqa: E402

PNID_LC = "PNID_LIFECYCLE"
PHONE_LC = "+4915207156153"


async def _seed_sender_and_window(session_maker, *, sender_id: int) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=sender_id,
                    company_id=1,
                    sender_code="default",
                    phone_number_id=PNID_LC,
                    display_phone="+49",
                    is_active=True,
                )
            )
            await _add_open_window(session, PHONE_LC, PNID_LC)


async def _insert_event(session_maker, payload: dict, *, dedupe_key: str) -> int:
    async with session_maker() as session:
        async with session.begin():
            evt = WhatsAppEvent(
                dedupe_key=dedupe_key,
                status="received",
                error=None,
                query={},
                headers={},
                payload=payload,
            )
            session.add(evt)
            await session.flush()
            return evt.id


async def _reload_event(session_maker, event_id: int) -> WhatsAppEvent:
    async with session_maker() as session:
        return await session.get(WhatsAppEvent, event_id)


async def _operator_outbox(session_maker):
    async with session_maker() as session:
        result = await session.execute(select(OutboxMessage).where(OutboxMessage.template_code == "operator_relay"))
        return result.scalars().first()


@pytest.mark.parametrize("hostile_id", ["9" * 5000, -1, "-42", 2**70, PG_BIGINT_MAX + 1])
@pytest.mark.asyncio
async def test_lifecycle_hostile_ids_null_projection(session_maker, monkeypatch, hostile_id) -> None:
    """process_one_event: hostile ids → NULL Outbox projections, event processed."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    monkeypatch.setattr(worker_module, "SessionLocal", session_maker)
    provider = _CaptureProvider()

    await _seed_sender_and_window(session_maker, sender_id=980)
    event_id = await _insert_event(
        session_maker,
        {
            "_chatwoot_operator_relay": {
                "recipient_phone": PHONE_LC,
                "text": "Hallo",
                "conversation_id": hostile_id,
                "message_id": hostile_id,
                "phone_number_id": PNID_LC,
                "reply_to_chatwoot_message_id": hostile_id,
            }
        },
        dedupe_key="chatwoot_out:lc:hostile",
    )

    await process_one_event(event_id, provider)

    event = await _reload_event(session_maker, event_id)
    assert event.status == "processed"
    assert event.processed_at is not None
    assert event.error is None
    assert len(provider.sent) == 1

    outbox = await _operator_outbox(session_maker)
    assert outbox is not None
    assert outbox.chatwoot_conversation_id is None
    assert outbox.chatwoot_message_id is None


@pytest.mark.asyncio
async def test_lifecycle_valid_ids_are_preserved(session_maker, monkeypatch) -> None:
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    monkeypatch.setattr(worker_module, "SessionLocal", session_maker)
    provider = _CaptureProvider()

    await _seed_sender_and_window(session_maker, sender_id=981)
    event_id = await _insert_event(
        session_maker,
        {
            "_chatwoot_operator_relay": {
                "recipient_phone": PHONE_LC,
                "text": "Hallo",
                "conversation_id": 6100,
                "message_id": 7100,
                "phone_number_id": PNID_LC,
            }
        },
        dedupe_key="chatwoot_out:lc:valid",
    )

    await process_one_event(event_id, provider)

    event = await _reload_event(session_maker, event_id)
    assert event.status == "processed"
    assert len(provider.sent) == 1
    outbox = await _operator_outbox(session_maker)
    assert outbox.chatwoot_conversation_id == 6100
    assert outbox.chatwoot_message_id == 7100


@pytest.mark.parametrize("hostile_from", [{}, [], 123, True, "abc"])
@pytest.mark.asyncio
async def test_lifecycle_malformed_from_is_safely_ignored(session_maker, monkeypatch, hostile_from) -> None:
    """messages[].from of any type → action skipped, event processed, no send."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    monkeypatch.setattr(worker_module, "SessionLocal", session_maker)
    provider = _CaptureProvider()

    event_id = await _insert_event(
        session_maker,
        {
            "entry": [
                {
                    "changes": [
                        {
                            "value": {
                                "messages": [{"from": hostile_from, "type": "text", "text": {"body": "hi"}}],
                                "metadata": {"phone_number_id": PNID_LC},
                            }
                        }
                    ]
                }
            ]
        },
        dedupe_key=f"wa:lc:from:{type(hostile_from).__name__}",
    )

    await process_one_event(event_id, provider)

    event = await _reload_event(session_maker, event_id)
    assert event.status == "processed"
    assert event.processed_at is not None
    assert provider.sent == []


@pytest.mark.parametrize("hostile_pnid", [{}, [], True, 123])
@pytest.mark.asyncio
async def test_lifecycle_malformed_phone_number_id_no_sql_error(session_maker, monkeypatch, hostile_pnid) -> None:
    """metadata.phone_number_id of any type → no SQL binding error, event processed."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    monkeypatch.setattr(worker_module, "SessionLocal", session_maker)
    provider = _CaptureProvider()

    event_id = await _insert_event(
        session_maker,
        {
            "entry": [
                {
                    "changes": [
                        {
                            "value": {
                                "messages": [{"from": PHONE_LC.lstrip("+"), "type": "text", "text": {"body": "hi"}}],
                                "metadata": {"phone_number_id": hostile_pnid},
                            }
                        }
                    ]
                }
            ]
        },
        dedupe_key=f"wa:lc:pnid:{type(hostile_pnid).__name__}",
    )

    await process_one_event(event_id, provider)

    event = await _reload_event(session_maker, event_id)
    assert event.status == "processed"
    assert provider.sent == []


@pytest.mark.parametrize("marker", [[], "bad", 123])
@pytest.mark.asyncio
async def test_lifecycle_malformed_operator_marker(session_maker, monkeypatch, marker) -> None:
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    monkeypatch.setattr(worker_module, "SessionLocal", session_maker)
    provider = _CaptureProvider()

    event_id = await _insert_event(
        session_maker,
        {"_chatwoot_operator_relay": marker},
        dedupe_key=f"wa:lc:marker:{type(marker).__name__}",
    )

    await process_one_event(event_id, provider)

    event = await _reload_event(session_maker, event_id)
    assert event.status == "processed"
    assert provider.sent == []


@pytest.mark.asyncio
async def test_lifecycle_mixed_malformed_reply_context_candidate(session_maker, monkeypatch) -> None:
    """A reply-context candidate whose payload has messages=123 before a valid
    entry must not crash the secondary scan; the valid entry is still reached."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    monkeypatch.setattr(worker_module, "SessionLocal", session_maker)
    provider = _CaptureProvider()

    conv_id, msg_id, reply_to = 6200, 7200, 8200

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=982,
                    company_id=1,
                    sender_code="default",
                    phone_number_id=PNID_LC,
                    display_phone="+49",
                    is_active=True,
                )
            )
            await _add_open_window(session, PHONE_LC, PNID_LC)
            # Historical candidate matching the reply-context SQL: mixed entries,
            # malformed messages=123 placed BEFORE the valid entry.
            session.add(
                WhatsAppEvent(
                    dedupe_key="wa:reply:candidate",
                    received_at=datetime.now(timezone.utc) - timedelta(minutes=30),
                    status="processed",
                    query={},
                    headers={},
                    chatwoot_message_id=reply_to,
                    forwarded_chatwoot_conversation_id=conv_id,
                    whatsapp_message_id="wamid.CANDIDATE",
                    payload={
                        "entry": [
                            {"changes": [{"value": {"messages": 123}}]},
                            {
                                "changes": [
                                    {
                                        "value": {
                                            "messages": [
                                                {
                                                    "from": PHONE_LC.lstrip("+"),
                                                    "type": "text",
                                                    "text": {"body": "orig"},
                                                    "id": "wamid.CANDIDATE",
                                                }
                                            ]
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                )
            )

    event_id = await _insert_event(
        session_maker,
        {
            "_chatwoot_operator_relay": {
                "recipient_phone": PHONE_LC,
                "text": "reply",
                "conversation_id": conv_id,
                "message_id": msg_id,
                "phone_number_id": PNID_LC,
                "reply_to_chatwoot_message_id": reply_to,
            }
        },
        dedupe_key="chatwoot_out:reply:mixed",
    )

    await process_one_event(event_id, provider)

    event = await _reload_event(session_maker, event_id)
    assert event.status == "processed"
    assert event.error is None
    assert len(provider.sent) == 1

    # The whole point: the valid entry AFTER the malformed one was reached, so
    # the native reply target resolved and was forwarded to the provider. A
    # helper that always returned False would still send — but without this.
    assert provider.send_calls[0]["kwargs"].get("reply_to_provider_message_id") == "wamid.CANDIDATE"

    outbox = await _operator_outbox(session_maker)
    assert outbox is not None
    assert outbox.meta["reply_context_native"] is True
    assert outbox.meta["reply_context_source"] == "whatsapp_event"
    assert outbox.meta["reply_to_provider_message_id"] == "wamid.CANDIDATE"


# ---------------------------------------------------------------------------
# Historical/replay defense for overlong phone and non-string content
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_phone",
    [
        "1" * 40,  # overlong
        "1" * 16,  # 16 digits
        "١٢٣٤٥",  # unicode-only digits
        "49١٢٣15",  # mixed unicode/ascii digits
        "+49 151 O23 4567",  # letter
        "+49🙂1511234567",  # emoji
        "49\n1511234567",  # control char
        "no phone here",  # letters
    ],
)
@pytest.mark.asyncio
async def test_lifecycle_overlong_historical_relay(session_maker, monkeypatch, bad_phone) -> None:
    """A stored relay with a grammar-invalid phone must not send or Outbox."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    monkeypatch.setattr(worker_module, "SessionLocal", session_maker)
    provider = _CaptureProvider()

    event_id = await _insert_event(
        session_maker,
        {"_chatwoot_operator_relay": {"recipient_phone": bad_phone, "text": "Hallo", "phone_number_id": PNID_LC}},
        dedupe_key=f"chatwoot_out:overlong:{len(bad_phone)}:{bad_phone[:2]}",
    )

    await process_one_event(event_id, provider)

    event = await _reload_event(session_maker, event_id)
    assert event.status == "processed"
    assert event.processed_at is not None
    assert event.error == "operator_relay: invalid recipient_phone"
    assert provider.sent == []
    assert await _operator_outbox(session_maker) is None


@pytest.mark.parametrize("bad_text", [{}, [], 123, True])
@pytest.mark.asyncio
async def test_lifecycle_non_string_relay_text(session_maker, monkeypatch, bad_text) -> None:
    """A stored relay whose text is non-string must fail closed (no send/Outbox)."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    monkeypatch.setattr(worker_module, "SessionLocal", session_maker)
    provider = _CaptureProvider()

    await _seed_sender_and_window(session_maker, sender_id=990)
    event_id = await _insert_event(
        session_maker,
        {"_chatwoot_operator_relay": {"recipient_phone": PHONE_LC, "text": bad_text, "phone_number_id": PNID_LC}},
        dedupe_key=f"chatwoot_out:badtext:{type(bad_text).__name__}",
    )

    await process_one_event(event_id, provider)

    event = await _reload_event(session_maker, event_id)
    assert event.status == "processed"
    assert event.error == "operator_relay: missing text"
    assert provider.sent == []
    assert await _operator_outbox(session_maker) is None


# ---------------------------------------------------------------------------
# Configured inbox-company map routing: no bypass, no provider before resolution
# ---------------------------------------------------------------------------

import logging  # noqa: E402

_MAP_ONE = '{"8":{"provider":"altegio","company_id":1}}'  # matches the seeded sender
_MISSING = object()  # sentinel: omit chatwoot_inbox_id entirely


async def _run_map_relay(session_maker, monkeypatch, *, inbox_id, map_json, sender_id, dedupe):
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(worker_module.settings, "chatwoot_inbox_company_map", map_json)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    monkeypatch.setattr(worker_module, "SessionLocal", session_maker)
    provider = _CaptureProvider()

    await _seed_sender_and_window(session_maker, sender_id=sender_id)
    relay = {"recipient_phone": PHONE_LC, "text": "Hallo", "phone_number_id": PNID_LC}
    if inbox_id is not _MISSING:
        relay["chatwoot_inbox_id"] = inbox_id
    event_id = await _insert_event(session_maker, {"_chatwoot_operator_relay": relay}, dedupe_key=dedupe)

    await process_one_event(event_id, provider)
    return provider, await _reload_event(session_maker, event_id)


@pytest.mark.parametrize(
    "inbox_id,expected_error",
    [
        (_MISSING, "operator_relay: missing_inbox_id"),
        (None, "operator_relay: missing_inbox_id"),
        (True, "operator_relay: invalid_inbox_id"),
        (1.9, "operator_relay: invalid_inbox_id"),
        ("8", "operator_relay: invalid_inbox_id"),
        (99, "operator_relay: inbox_mapping_missing"),
    ],
)
@pytest.mark.asyncio
async def test_configured_map_routing_blocks_before_provider(session_maker, monkeypatch, inbox_id, expected_error):
    provider, event = await _run_map_relay(
        session_maker,
        monkeypatch,
        inbox_id=inbox_id,
        map_json=_MAP_ONE,
        sender_id=940,
        dedupe=f"chatwoot_out:map:{expected_error[-6:]}:{inbox_id}",
    )

    assert provider.sent == []
    assert await _operator_outbox(session_maker) is None
    assert event.status == "processed"
    assert event.processed_at is not None
    assert event.error == expected_error


@pytest.mark.asyncio
async def test_configured_map_missing_inbox_does_not_send_even_with_one_sender(session_maker, monkeypatch):
    """Critical: a configured map + missing inbox ID must NOT fall back to the
    single active sender — that would route to the wrong tenant."""
    provider, event = await _run_map_relay(
        session_maker,
        monkeypatch,
        inbox_id=_MISSING,
        map_json=_MAP_ONE,
        sender_id=941,
        dedupe="chatwoot_out:map:critical",
    )

    assert provider.sent == []  # exactly one sender exists, still not used
    assert await _operator_outbox(session_maker) is None
    assert event.error == "operator_relay: missing_inbox_id"


@pytest.mark.asyncio
async def test_configured_map_valid_inbox_resolves_and_sends(session_maker, monkeypatch):
    provider, event = await _run_map_relay(
        session_maker,
        monkeypatch,
        inbox_id=8,
        map_json=_MAP_ONE,
        sender_id=942,
        dedupe="chatwoot_out:map:valid",
    )

    assert len(provider.sent) == 1
    assert event.error is None
    assert await _operator_outbox(session_maker) is not None


@pytest.mark.asyncio
async def test_invalid_map_does_not_reach_generic_exception(session_maker, monkeypatch):
    provider, event = await _run_map_relay(
        session_maker,
        monkeypatch,
        inbox_id=8,
        map_json="{not valid json",
        sender_id=943,
        dedupe="chatwoot_out:map:badjson",
    )

    assert provider.sent == []
    assert await _operator_outbox(session_maker) is None
    assert event.error == "operator_relay: invalid_inbox_company_map"


@pytest.mark.parametrize(
    "secret_map",
    [
        # Invalid JSON (literal newline) — rejected before any int() coercion.
        '{"42": "token=SECRETVAL\nforged"}',
        # VALID JSON with a non-numeric string value — this is the one that would
        # reach int() and leak the secret if company validation used int(value).
        '{"42": "token=SECRETVAL"}',
    ],
)
@pytest.mark.asyncio
async def test_secret_bearing_map_leaks_nothing(session_maker, monkeypatch, caplog, secret_map):
    """A secret embedded in the map config must not reach logs or event.error."""
    with caplog.at_level(logging.DEBUG):
        provider, event = await _run_map_relay(
            session_maker,
            monkeypatch,
            inbox_id=42,
            map_json=secret_map,
            sender_id=944,
            dedupe=f"chatwoot_out:map:secret:{len(secret_map)}",
        )

    assert provider.sent == []
    assert await _operator_outbox(session_maker) is None
    assert event.error == "operator_relay: invalid_inbox_company_map"

    worker_logs = "\n".join(r.getMessage() for r in caplog.records if r.name == "whatsapp_inbox_worker")
    for forbidden in ("SECRETVAL", "token=", "forged", chr(0x2028), chr(0x2029)):
        assert forbidden not in worker_logs
    assert "SECRETVAL" not in (event.error or "")


# ---------------------------------------------------------------------------
# §13 lifecycle: every shape of invalid inbox->company map fails closed with a
# single stable, non-sensitive marker (no send, no Outbox) via process_one_event.
# ---------------------------------------------------------------------------

_HUGE_KEY = '{"' + "1" * 5000 + '": 1}'  # must be rejected WITHOUT calling int()


@pytest.mark.parametrize(
    "label,map_json",
    [
        ("dup_raw_key", '{"8": 1, "8": 2}'),  # duplicate JSON key -> object_pairs_hook
        ("newline_collide_key", '{"8": 1, "8\\n": 2}'),  # match() would collide on int("8\n")
        ("out_of_range_key", '{"2147483648": 1}'),  # PG_INT_MAX + 1
        ("huge_key", _HUGE_KEY),  # 5000-digit key: rejected, totality, no int()
        ("company_not_int", '{"8": "1"}'),  # string company value
        ("company_bool", '{"8": true}'),  # bool company value
        ("company_float", '{"8": 1.0}'),  # float company value
        ("company_out_of_range", '{"8": 2147483648}'),  # PG_INT_MAX + 1
    ],
)
@pytest.mark.asyncio
async def test_lifecycle_invalid_map_variants_fail_closed(session_maker, monkeypatch, label, map_json):
    """Any invalid map (bad key OR bad company value) must yield the single stable
    marker, never send, never Outbox, and never raise (processed, not failed)."""
    provider, event = await _run_map_relay(
        session_maker,
        monkeypatch,
        inbox_id=8,
        map_json=map_json,
        sender_id=950,
        dedupe=f"chatwoot_out:map:invalidshape:{label}",
    )

    assert event.status == "processed"
    assert event.processed_at is not None
    assert event.error == "operator_relay: invalid_inbox_company_map"
    assert provider.sent == []
    assert await _operator_outbox(session_maker) is None
