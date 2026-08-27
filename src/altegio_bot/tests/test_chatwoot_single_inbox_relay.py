"""PR-7.4 hotfix: one Chatwoot inbox, one explicitly configured operator sender.

The production failure this pins down is narrow and reproducible. A brand-new
customer writes FIRST, so there is no booking, no client row and no prior
delivery to prove a branch with. ``resolve_tenant_affinity`` therefore answers
NO_EVIDENCE — correctly — and the operator's reply typed in General dies as
``operator_relay: general_affinity_no_evidence``.

Switching the branch map off does not fix it on its own: every branch sender
shares ONE Meta ``phone_number_id``, so the legacy phone-number fallback sees
several provider-scoped rows and ends in ``operator_relay: ambiguous_sender``.

``CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID`` answers the question the data
cannot: not "pick a sender", but "use the ONE sender the operator configured,
and prove it". These tests hold that line from both sides — the reply must go
out for the new contact, and it must NOT go out from a branch inbox, from an
unproved sender, or in any topology the rollback does not support.

Production ids are deliberately absent: the real General inbox and the real
sender id live only in ``.env``. The values below are synthetic.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    OutboxMessage,
    WhatsAppEvent,
    WhatsAppSender,
)
from altegio_bot.providers.base import ChatwootRoute, WhatsAppProvider
from altegio_bot.providers.chatwoot_hybrid import ChatwootHybridProvider
from altegio_bot.settings import Settings
from altegio_bot.workers import whatsapp_inbox_worker as wiw

# Synthetic topology. Shaped like production — one General inbox, three branch
# inboxes, one native Chatwoot WhatsApp inbox, one shared Meta number — but
# never carrying a production id.
GENERAL_INBOX = 4801
BRANCH_INBOXES = (9, 10, 11)
NATIVE_WHATSAPP_INBOX = 7

KARLSRUHE = (PROVIDER_ALTEGIO, 999701)
RASTATT = (PROVIDER_ALTEGIO, 999702)
DURLACH = (PROVIDER_EASYWEEK, 999703)

# The configured sender is deliberately the HIGHEST id and not the only
# `default` row: if anything here ever fell back to LIMIT 1 / min(id) / row
# order, these tests would pick a different sender and fail.
LOW_SENDER_ID = 4901
MID_SENDER_ID = 4902
SINGLE_SENDER_ID = 4903

SHARED_PNID = "PNID_SHARED_META_LINE"
FOREIGN_PNID = "PNID_SOME_OTHER_LINE"

PHONE = "+491700000042"
TEXT = "Guten Tag, morgen haben wir noch Termine frei."

BRANCH_MAP = (
    '{"9":{"provider":"altegio","company_id":999701},'
    '"10":{"provider":"altegio","company_id":999702},'
    '"11":{"provider":"easyweek","company_id":999703}}'
)

API_TOKEN = "cw_token_MUST_NEVER_BE_LOGGED"


# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------


class _CountingProvider(WhatsAppProvider):
    """Records every provider call so "never reached Meta" is provable."""

    def __init__(self, wamid: str = "wamid.SINGLE_INBOX_001") -> None:
        self.wamid = wamid
        self.sent: list[dict[str, Any]] = []
        self.templates_sent: list[dict[str, Any]] = []

    async def send(self, sender_id: int, phone_e164: str, text: str, **kwargs: Any) -> str:
        self.sent.append({"sender_id": sender_id, "phone_e164": phone_e164, "text": text})
        return self.wamid

    async def send_template(self, *args: Any, **kwargs: Any) -> str:
        self.templates_sent.append({"args": args, "kwargs": kwargs})
        return self.wamid


class _ChatwootSpy:
    """Stands in for every ChatwootClient the worker builds."""

    def __init__(self, conversation_id: int = 7100) -> None:
        self.conversation_id = conversation_id
        self.built_inbox_ids: list[object] = []
        self.notes: list[dict[str, Any]] = []
        self.messages: list[dict[str, Any]] = []
        self.created_conversations = 0

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        spy = self

        class _FakeChatwoot:
            def __init__(self, *args: Any, inbox_id: object = None, **kwargs: Any) -> None:
                spy.built_inbox_ids.append(inbox_id)

            async def get_or_create_incoming_conversation(
                self,
                phone_e164: str,
                *,
                contact_name: str | None = None,
            ) -> int:
                spy.created_conversations += 1
                return spy.conversation_id

            async def send_message(
                self,
                conversation_id: int,
                text: str,
                *,
                message_type: str = "outgoing",
                private: bool = False,
                content_attributes: dict[str, Any] | None = None,
            ) -> int:
                record = {
                    "conversation_id": conversation_id,
                    "text": text,
                    "private": private,
                    "message_type": message_type,
                }
                spy.messages.append(record)
                if private:
                    spy.notes.append(record)
                return 991

            async def aclose(self) -> None:
                return None

        monkeypatch.setattr(wiw, "ChatwootClient", _FakeChatwoot)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _single_inbox_settings(
    monkeypatch: pytest.MonkeyPatch,
    *,
    sender_id: int = SINGLE_SENDER_ID,
    mode: str = "general",
    branch_map: str = "{}",
    general_inbox: object = GENERAL_INBOX,
    meta_pnid: str = SHARED_PNID,
) -> None:
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(wiw.settings, "chatwoot_inbox_company_map", branch_map)
    monkeypatch.setattr(wiw.settings, "chatwoot_inbox_id", general_inbox)
    monkeypatch.setattr(wiw.settings, "chatwoot_inbound_routing_mode", mode)
    monkeypatch.setattr(wiw.settings, "chatwoot_single_inbox_operator_sender_id", sender_id)
    monkeypatch.setattr(wiw.settings, "meta_wa_phone_number_id", meta_pnid)
    monkeypatch.setattr(wiw.settings, "chatwoot_api_token", API_TOKEN)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_reopen_private_note_enabled", True)


def _sender(
    sender_id: int,
    tenant: tuple[str, int],
    *,
    phone_number_id: str = SHARED_PNID,
    is_active: bool = True,
    sender_code: str = "default",
) -> WhatsAppSender:
    return WhatsAppSender(
        id=sender_id,
        provider=tenant[0],
        company_id=tenant[1],
        sender_code=sender_code,
        phone_number_id=phone_number_id,
        display_phone="+49000000000",
        is_active=is_active,
    )


def _open_window_event(phone: str = PHONE, *, dedupe_key: str = "wa:inbound:single-inbox") -> WhatsAppEvent:
    """The customer's own first message — this is what opens the 24h window."""
    return WhatsAppEvent(
        dedupe_key=dedupe_key,
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
                                "metadata": {"phone_number_id": SHARED_PNID},
                                "messages": [
                                    {
                                        "from": phone,
                                        "type": "text",
                                        "text": {"body": "Hallo!"},
                                        "id": f"wamid.{dedupe_key}",
                                    }
                                ],
                            }
                        }
                    ]
                }
            ]
        },
    )


async def _seed_shared_line(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    configured: WhatsAppSender | None = None,
    open_window: bool = True,
) -> None:
    """Three provider-scoped senders behind ONE Meta phone_number_id.

    This is the production shape that makes the legacy fallback ambiguous, and
    the reason the rollback needs an explicit id rather than a better query.
    """
    async with session_maker() as session:
        async with session.begin():
            session.add(_sender(LOW_SENDER_ID, KARLSRUHE))
            session.add(_sender(MID_SENDER_ID, RASTATT))
            session.add(configured if configured is not None else _sender(SINGLE_SENDER_ID, DURLACH))
            if open_window:
                session.add(_open_window_event())


async def _run_relay(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    *,
    inbox_id: object = GENERAL_INBOX,
    relay_pnid: str = SHARED_PNID,
    conversation_id: int = 610,
    message_id: int = 9610,
) -> tuple[_CountingProvider, _ChatwootSpy, int]:
    """Drive one real operator-relay event end to end."""
    monkeypatch.setattr(wiw, "SessionLocal", session_maker)
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)

    provider = _CountingProvider()
    spy = _ChatwootSpy()
    spy.install(monkeypatch)

    async with session_maker() as session:
        async with session.begin():
            event = WhatsAppEvent(
                dedupe_key=f"chatwoot_out:{conversation_id}:{message_id}",
                status="received",
                query={},
                headers={},
                payload={
                    "_chatwoot_operator_relay": {
                        "recipient_phone": PHONE,
                        "text": TEXT,
                        "conversation_id": conversation_id,
                        "message_id": message_id,
                        "phone_number_id": relay_pnid,
                        "chatwoot_inbox_id": inbox_id,
                        "agent_name": "Anna",
                    },
                },
                chatwoot_conversation_id=conversation_id,
            )
            session.add(event)
            await session.flush()
            event_id = int(event.id)

    await wiw.process_one_event(event_id, provider)
    return provider, spy, event_id


async def _event_and_outbox(
    session_maker: async_sessionmaker[AsyncSession],
    event_id: int,
) -> tuple[WhatsAppEvent, list[OutboxMessage]]:
    async with session_maker() as session:
        event = await session.get(WhatsAppEvent, event_id)
        rows = list(
            (await session.execute(select(OutboxMessage).where(OutboxMessage.source_whatsapp_event_id == event_id)))
            .scalars()
            .all()
        )
    return event, rows


# ---------------------------------------------------------------------------
# 1. The default is off, and off means byte-for-byte today's behaviour
# ---------------------------------------------------------------------------


def test_the_setting_defaults_to_off() -> None:
    assert Settings().chatwoot_single_inbox_operator_sender_id == 0


def test_a_negative_sender_id_fails_startup_instead_of_reading_as_off() -> None:
    """A typo must not look like a deliberate "feature disabled"."""
    with pytest.raises(ValueError):
        Settings(chatwoot_single_inbox_operator_sender_id=-1)


async def test_sender_id_zero_keeps_the_fail_closed_ambiguity(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bug, unfixed: three senders on one Meta number stay ambiguous."""
    _single_inbox_settings(monkeypatch, sender_id=0)
    await _seed_shared_line(session_maker)

    provider, spy, event_id = await _run_relay(session_maker, monkeypatch)
    event, rows = await _event_and_outbox(session_maker, event_id)

    assert provider.sent == []
    assert event.status == "processed"
    assert event.error == "operator_relay: ambiguous_sender"
    assert rows == []


async def test_sender_id_zero_leaves_multi_inbox_behaviour_untouched(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With the branch map configured and the feature off, the map still rules."""
    _single_inbox_settings(monkeypatch, sender_id=0, mode="affinity", branch_map=BRANCH_MAP)
    await _seed_shared_line(session_maker)

    provider, spy, event_id = await _run_relay(session_maker, monkeypatch, inbox_id=BRANCH_INBOXES[0])
    event, rows = await _event_and_outbox(session_maker, event_id)

    # Inbox 9 maps to Karlsruhe, so the Karlsruhe sender carries it — chosen by
    # the map, not by this hotfix.
    assert event.error is None
    assert len(rows) == 1
    assert rows[0].sender_id == LOW_SENDER_ID
    assert rows[0].company_id == KARLSRUHE[1]
    assert [call["sender_id"] for call in provider.sent] == [LOW_SENDER_ID]


# ---------------------------------------------------------------------------
# 2. The customer who wrote first now gets an answer
# ---------------------------------------------------------------------------


async def test_a_brand_new_contact_is_answered_from_general(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No client, no booking, no prior delivery — and the reply still leaves."""
    _single_inbox_settings(monkeypatch)
    await _seed_shared_line(session_maker)

    provider, spy, event_id = await _run_relay(session_maker, monkeypatch)
    event, rows = await _event_and_outbox(session_maker, event_id)

    assert event.error is None
    assert len(provider.sent) == 1
    assert provider.sent[0]["phone_e164"] == PHONE
    assert provider.sent[0]["text"] == TEXT
    assert len(rows) == 1
    assert rows[0].sender_id == SINGLE_SENDER_ID
    assert rows[0].company_id == DURLACH[1]
    assert rows[0].status == "sent"
    assert spy.notes == [], "nothing was blocked, so the operator is owed no warning"


async def test_one_meta_number_behind_many_senders_is_not_ambiguous_here(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The configured id decides — not the row count, and not the row order.

    The configured sender is the highest id of three `default` rows sharing the
    number, so LIMIT 1, min(id) and "first row" would all answer differently.
    """
    _single_inbox_settings(monkeypatch)
    await _seed_shared_line(session_maker)

    provider, spy, event_id = await _run_relay(session_maker, monkeypatch)
    event, rows = await _event_and_outbox(session_maker, event_id)

    assert event.error is None
    assert [call["sender_id"] for call in provider.sent] == [SINGLE_SENDER_ID]
    assert rows[0].sender_id not in (LOW_SENDER_ID, MID_SENDER_ID)


async def test_the_24h_window_is_not_relaxed_by_the_rollback(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A closed customer-service window still blocks the free-form send."""
    _single_inbox_settings(monkeypatch)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_closed_window_mode", "private_note_only")
    await _seed_shared_line(session_maker, open_window=False)

    provider, spy, event_id = await _run_relay(session_maker, monkeypatch)
    event, rows = await _event_and_outbox(session_maker, event_id)

    assert provider.sent == [], "no inbound in 24h — Meta must not be called"
    assert provider.templates_sent == []
    assert len(rows) == 1
    assert rows[0].status == "canceled"
    # The sender was still resolved the strict way, for the audit row.
    assert rows[0].sender_id == SINGLE_SENDER_ID


# ---------------------------------------------------------------------------
# 3. Only the General inbox may use the sender
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("branch_inbox", BRANCH_INBOXES)
async def test_a_branch_inbox_never_borrows_the_single_inbox_sender(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    branch_inbox: int,
) -> None:
    """Replies still sitting in the old branch inboxes stay blocked."""
    _single_inbox_settings(monkeypatch)
    await _seed_shared_line(session_maker)

    provider, spy, event_id = await _run_relay(session_maker, monkeypatch, inbox_id=branch_inbox)
    event, rows = await _event_and_outbox(session_maker, event_id)

    assert provider.sent == []
    assert event.error == "operator_relay: single_inbox_not_general"
    assert rows == []


async def test_the_native_whatsapp_inbox_is_not_general_either(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Channel::Whatsapp inbox is a different inbox, not a second General."""
    _single_inbox_settings(monkeypatch)
    await _seed_shared_line(session_maker)

    provider, spy, event_id = await _run_relay(session_maker, monkeypatch, inbox_id=NATIVE_WHATSAPP_INBOX)
    event, rows = await _event_and_outbox(session_maker, event_id)

    assert provider.sent == []
    assert event.error == "operator_relay: single_inbox_not_general"
    assert rows == []


@pytest.mark.parametrize("inbox_id", [None, 0, -1, str(GENERAL_INBOX), {"id": GENERAL_INBOX}, True])
async def test_an_unusable_inbox_id_never_reaches_the_sender(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    inbox_id: object,
) -> None:
    """The inbox id is Chatwoot-controlled: only an exact positive int passes."""
    _single_inbox_settings(monkeypatch)
    await _seed_shared_line(session_maker)

    provider, spy, event_id = await _run_relay(session_maker, monkeypatch, inbox_id=inbox_id)
    event, rows = await _event_and_outbox(session_maker, event_id)

    assert provider.sent == []
    assert event.error == "operator_relay: single_inbox_not_general"
    assert rows == []


# ---------------------------------------------------------------------------
# 4. The configured sender must be proved, not trusted
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spec, relay_pnid, meta_pnid, expected",
    [
        pytest.param(None, SHARED_PNID, SHARED_PNID, "single_inbox_sender_not_found", id="missing_row"),
        pytest.param(
            {"tenant": DURLACH, "is_active": False},
            SHARED_PNID,
            SHARED_PNID,
            "single_inbox_sender_inactive",
            id="inactive_row",
        ),
        pytest.param(
            {"tenant": DURLACH, "phone_number_id": FOREIGN_PNID},
            SHARED_PNID,
            SHARED_PNID,
            "single_inbox_sender_phone_mismatch",
            id="sender_on_another_line",
        ),
        pytest.param(
            {"tenant": DURLACH},
            FOREIGN_PNID,
            SHARED_PNID,
            "single_inbox_sender_phone_mismatch",
            id="relay_from_another_line",
        ),
        pytest.param(
            {"tenant": DURLACH},
            SHARED_PNID,
            FOREIGN_PNID,
            "single_inbox_sender_phone_mismatch",
            id="deployment_owns_another_line",
        ),
        pytest.param(
            {"tenant": DURLACH},
            SHARED_PNID,
            "",
            "single_inbox_sender_phone_mismatch",
            id="deployment_number_unset",
        ),
        pytest.param(
            {"tenant": ("unknown_crm", 999703)},
            SHARED_PNID,
            SHARED_PNID,
            "single_inbox_sender_identity_invalid",
            id="unknown_provider",
        ),
        pytest.param(
            {"tenant": (PROVIDER_EASYWEEK, 0)},
            SHARED_PNID,
            SHARED_PNID,
            "single_inbox_sender_identity_invalid",
            id="unusable_company_id",
        ),
    ],
)
async def test_an_unproved_sender_is_blocked_before_meta(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    spec: dict[str, Any] | None,
    relay_pnid: str,
    meta_pnid: str,
    expected: str,
) -> None:
    _single_inbox_settings(monkeypatch, meta_pnid=meta_pnid)

    if spec is None:
        # Only the two OTHER senders exist: the configured id has no row at all.
        async with session_maker() as session:
            async with session.begin():
                session.add(_sender(LOW_SENDER_ID, KARLSRUHE))
                session.add(_sender(MID_SENDER_ID, RASTATT))
                session.add(_open_window_event())
    else:
        await _seed_shared_line(session_maker, configured=_sender(SINGLE_SENDER_ID, **spec))

    provider, spy, event_id = await _run_relay(session_maker, monkeypatch, relay_pnid=relay_pnid)
    event, rows = await _event_and_outbox(session_maker, event_id)

    assert provider.sent == [], "a sender that was not proved must never reach Meta"
    assert provider.templates_sent == []
    assert event.status == "processed"
    assert event.error == f"operator_relay: {expected}"
    assert rows == [], "a blocked relay must not leave a 'sent' Outbox behind"


async def test_the_other_active_senders_are_left_alone(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The rollback selects a sender; it never deactivates the rest."""
    _single_inbox_settings(monkeypatch)
    await _seed_shared_line(session_maker)

    await _run_relay(session_maker, monkeypatch)

    async with session_maker() as session:
        senders = list((await session.execute(select(WhatsAppSender))).scalars().all())

    assert {int(s.id) for s in senders} == {LOW_SENDER_ID, MID_SENDER_ID, SINGLE_SENDER_ID}
    assert all(s.is_active for s in senders)


# ---------------------------------------------------------------------------
# 5. An unsupported configuration is a fault, not a fallback
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs, label",
    [
        ({"branch_map": BRANCH_MAP}, "branch map still configured"),
        ({"mode": "affinity"}, "affinity mode"),
        ({"mode": "context"}, "context mode"),
        ({"general_inbox": 0}, "no General inbox"),
        ({"general_inbox": "4801"}, "General inbox not an int"),
    ],
)
async def test_a_positive_sender_id_outside_its_topology_fails_closed(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict[str, object],
    label: str,
) -> None:
    _single_inbox_settings(monkeypatch, **kwargs)
    await _seed_shared_line(session_maker)

    provider, spy, event_id = await _run_relay(session_maker, monkeypatch)
    event, rows = await _event_and_outbox(session_maker, event_id)

    assert provider.sent == [], label
    assert event.error == "operator_relay: single_inbox_config_invalid", label
    assert rows == [], label


# ---------------------------------------------------------------------------
# 6. The rest of the one-inbox rollback: display and mirrors
# ---------------------------------------------------------------------------


async def test_general_mode_inbound_never_reuses_a_branch_conversation(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reply whose context points into a branch conversation still lands in General."""
    _single_inbox_settings(monkeypatch)
    spy = _ChatwootSpy(conversation_id=7100)
    spy.install(monkeypatch)

    branch_conversation_id = 5150
    async with session_maker() as session:
        async with session.begin():
            prior = _open_window_event(dedupe_key="wa:inbound:prior-branch")
            prior.whatsapp_message_id = "wamid.PRIOR_BRANCH"
            prior.chatwoot_message_id = 4444
            prior.forwarded_chatwoot_conversation_id = branch_conversation_id
            prior.payload["entry"][0]["changes"][0]["value"]["messages"][0]["id"] = "wamid.PRIOR_BRANCH"
            session.add(prior)

            event = WhatsAppEvent(
                dedupe_key="wa:inbound:general-mode",
                status="received",
                query={},
                headers={},
                payload={},
            )
            session.add(event)
            await session.flush()
            event_id = int(event.id)

    async with session_maker() as session:
        async with session.begin():
            event = await session.get(WhatsAppEvent, event_id)
            await wiw._forward_text_to_chatwoot(
                session,
                event,
                phone_e164=PHONE,
                text=TEXT,
                reply_to_provider_message_id="wamid.PRIOR_BRANCH",
            )

    async with session_maker() as session:
        stored = await session.get(WhatsAppEvent, event_id)

    assert spy.created_conversations == 1, "general mode must open/reuse its own General conversation"
    assert stored.forwarded_chatwoot_conversation_id == 7100
    assert stored.forwarded_chatwoot_conversation_id != branch_conversation_id
    assert all(msg["conversation_id"] != branch_conversation_id for msg in spy.messages)


@pytest.mark.parametrize("tenant", [KARLSRUHE, RASTATT, DURLACH])
async def test_outbound_mirrors_go_to_general_when_the_branch_map_is_empty(
    monkeypatch: pytest.MonkeyPatch,
    tenant: tuple[str, int],
) -> None:
    """Altegio and EasyWeek automation mirrors both land in the one inbox."""
    _single_inbox_settings(monkeypatch)

    legacy = type("_Legacy", (), {"_inbox_id": GENERAL_INBOX})()

    def _never(inbox_id: int) -> Any:  # pragma: no cover - must not be reached
        raise AssertionError(f"no per-branch client may be built, got inbox_id={inbox_id}")

    hybrid = ChatwootHybridProvider(
        primary=_CountingProvider(),
        chatwoot=legacy,
        chatwoot_factory=_never,
    )

    client, inbox_id, routing_error = hybrid._chatwoot_for_route(ChatwootRoute.TENANT, tenant[0], tenant[1])

    assert routing_error is None
    assert client is legacy
    assert inbox_id == GENERAL_INBOX


# ---------------------------------------------------------------------------
# 7. Nothing sensitive escapes into the logs or into event.error
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "inbox_id, relay_pnid",
    [
        pytest.param(GENERAL_INBOX, SHARED_PNID, id="accepted"),
        pytest.param(BRANCH_INBOXES[0], SHARED_PNID, id="wrong_inbox"),
        pytest.param(GENERAL_INBOX, FOREIGN_PNID, id="wrong_line"),
    ],
)
async def test_no_phone_text_token_or_raw_env_reaches_logs_or_event_error(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    inbox_id: object,
    relay_pnid: str,
) -> None:
    _single_inbox_settings(monkeypatch)
    await _seed_shared_line(session_maker)

    with caplog.at_level(logging.DEBUG):
        provider, spy, event_id = await _run_relay(
            session_maker,
            monkeypatch,
            inbox_id=inbox_id,
            relay_pnid=relay_pnid,
        )

    event, _rows = await _event_and_outbox(session_maker, event_id)
    persisted = event.error or ""
    worker_log = "\n".join(r.getMessage() for r in caplog.records if r.name == "whatsapp_inbox_worker")
    every_log = caplog.text

    # The persisted reason is a stable code and nothing else.
    for secret in (PHONE, TEXT, API_TOKEN, SHARED_PNID, FOREIGN_PNID, BRANCH_MAP, "Anna"):
        assert secret not in persisted, f"event.error leaked {secret!r}"

    # Customer/agent data must not reach the worker's own log lines...
    for secret in (PHONE, TEXT, "Anna"):
        assert secret not in worker_log, f"worker log leaked {secret!r}"

    # ...and no logger anywhere may echo a credential or the raw configuration.
    for secret in (API_TOKEN, BRANCH_MAP):
        assert secret not in every_log, f"logs leaked {secret!r}"
