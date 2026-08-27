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

from altegio_bot.chatwoot_affinity import AffinityOutcome, resolve_tenant_affinity
from altegio_bot.chatwoot_outbox_route import (
    CHATWOOT_ROUTE_META_KEY,
    SINGLE_INBOX_RELAY_META_KEY,
    resolve_operator_outbox_route,
)
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
RASTATT = (PROVIDER_EASYWEEK, 999702)
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

# The map exactly as production restores it: inbox 9 Karlsruhe (Altegio),
# inbox 10 Rastatt, inbox 11 Durlach.
BRANCH_MAP = (
    '{"9":{"provider":"altegio","company_id":999701},'
    '"10":{"provider":"easyweek","company_id":999702},'
    '"11":{"provider":"easyweek","company_id":999703}}'
)
KARLSRUHE_INBOX = 9

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


# ---------------------------------------------------------------------------
# 8. The message sent during the rollback keeps its General route afterwards
# ---------------------------------------------------------------------------
#
# This is the trap the provenance exists for. The rollback sends through a
# TRANSPORT sender — in production the Karlsruhe row, because it is the one
# active line on the shared Meta number. That sender says nothing about which
# branch the customer belongs to; during the rollback nothing does, which is why
# the setting exists at all.
#
# `_get_outbox_context_target()` normally reads an operator Outbox's sender as
# authoritative tenant evidence. So without provenance, the moment the branch
# map and `affinity` come back, a customer replying to a message sent during the
# rollback would be pulled into Karlsruhe — a branch nobody proved.


async def _seed_transport_line(session_maker: async_sessionmaker[AsyncSession]) -> None:
    """Production's shape: the technical line is Karlsruhe, the customer is not."""
    async with session_maker() as session:
        async with session.begin():
            session.add(_sender(LOW_SENDER_ID, DURLACH))
            session.add(_sender(MID_SENDER_ID, RASTATT))
            session.add(_sender(SINGLE_SENDER_ID, KARLSRUHE))
            session.add(_open_window_event())


def _restore_affinity(monkeypatch: pytest.MonkeyPatch) -> None:
    """The rollback is over: provider-scoped map back, `affinity` back, feature off."""
    monkeypatch.setattr(wiw.settings, "chatwoot_inbox_company_map", BRANCH_MAP)
    monkeypatch.setattr(wiw.settings, "chatwoot_inbound_routing_mode", "affinity")
    monkeypatch.setattr(wiw.settings, "chatwoot_single_inbox_operator_sender_id", 0)


async def _inbound_reply_event(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    dedupe_key: str,
) -> int:
    async with session_maker() as session:
        async with session.begin():
            event = WhatsAppEvent(dedupe_key=dedupe_key, status="received", query={}, headers={}, payload={})
            session.add(event)
            await session.flush()
            return int(event.id)


async def test_a_reply_to_a_hotfix_message_stays_in_general_after_affinity_returns(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _single_inbox_settings(monkeypatch)
    await _seed_transport_line(session_maker)

    provider, _spy, event_id = await _run_relay(session_maker, monkeypatch)
    assert [call["sender_id"] for call in provider.sent] == [SINGLE_SENDER_ID], "sent via the Karlsruhe transport line"

    _restore_affinity(monkeypatch)
    spy = _ChatwootSpy(conversation_id=7200)
    spy.install(monkeypatch)

    reply_event_id = await _inbound_reply_event(session_maker, dedupe_key="wa:inbound:after-restore")
    async with session_maker() as session:
        async with session.begin():
            event = await session.get(WhatsAppEvent, reply_event_id)
            await wiw._forward_text_to_chatwoot(
                session,
                event,
                phone_e164=PHONE,
                text="Ja, 14 Uhr passt.",
                reply_to_provider_message_id=provider.wamid,
            )

    async with session_maker() as session:
        stored = await session.get(WhatsAppEvent, reply_event_id)

    assert stored.error is None
    assert spy.built_inbox_ids == [GENERAL_INBOX], "the reply must land in General, not in the transport branch"
    assert KARLSRUHE_INBOX not in spy.built_inbox_ids


async def test_a_reaction_on_a_hotfix_message_stays_in_general_after_affinity_returns(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _single_inbox_settings(monkeypatch)
    await _seed_transport_line(session_maker)

    provider, _spy, _event_id = await _run_relay(session_maker, monkeypatch)
    _restore_affinity(monkeypatch)

    async with session_maker() as session:
        target = await wiw._resolve_reaction_target(session, provider.wamid, phone_e164=PHONE)
        inbox_id, routing_error = wiw._inbound_target_inbox(
            chatwoot_route=target.chatwoot_route,
            tenant_provider=target.tenant_provider,
            company_id=target.company_id,
            tenant_error=target.tenant_error,
        )

    assert target.kind == "chatwoot_agent_message"
    assert target.tenant_error is None
    assert target.chatwoot_route is ChatwootRoute.GENERAL
    assert target.tenant_provider is None and target.company_id is None
    assert routing_error is None
    assert inbox_id == GENERAL_INBOX


async def test_the_transport_sender_never_becomes_tenant_evidence(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Not even once the message is delivered — the affinity resolver skips it."""
    _single_inbox_settings(monkeypatch)
    await _seed_transport_line(session_maker)

    provider, _spy, event_id = await _run_relay(session_maker, monkeypatch)
    _restore_affinity(monkeypatch)

    async with session_maker() as session:
        async with session.begin():
            row = (
                await session.execute(select(OutboxMessage).where(OutboxMessage.source_whatsapp_event_id == event_id))
            ).scalar_one()
            row.status = "delivered"
            outbox_meta = dict(row.meta or {})

        result = await resolve_tenant_affinity(session, [PHONE])
        target = await wiw._get_reply_context_target(session, provider.wamid, phone_e164=PHONE)

    # The provenance is written as one coherent pair, and it is not a secret.
    assert outbox_meta[CHATWOOT_ROUTE_META_KEY] == ChatwootRoute.GENERAL.value
    assert outbox_meta[SINGLE_INBOX_RELAY_META_KEY] == {"route": "general", "sender_scope": "transport_only"}

    # A delivered operator message would normally PROVE its sender's branch.
    assert result.outcome is AffinityOutcome.NO_EVIDENCE, "the transport line must not prove Karlsruhe"
    assert result.identity is None

    assert target.chatwoot_route is ChatwootRoute.GENERAL
    assert target.tenant_provider is None and target.company_id is None
    assert target.tenant_error is None


async def test_an_ordinary_operator_message_still_routes_by_its_sender_tenant(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The control: rows written outside the rollback keep today's behaviour."""
    _single_inbox_settings(monkeypatch, sender_id=0, mode="affinity", branch_map=BRANCH_MAP)
    await _seed_transport_line(session_maker)

    async with session_maker() as session:
        async with session.begin():
            session.add(
                OutboxMessage(
                    company_id=KARLSRUHE[1],
                    sender_id=SINGLE_SENDER_ID,
                    phone_e164=PHONE,
                    template_code="operator_relay",
                    language="de",
                    body="Bis morgen!",
                    status="sent",
                    scheduled_at=datetime.now(timezone.utc),
                    sent_at=datetime.now(timezone.utc),
                    provider_message_id="wamid.PLAIN_OPERATOR",
                    message_source="operator",
                    chatwoot_conversation_id=615,
                    chatwoot_message_id=9615,
                    meta={"send_type": "text"},
                )
            )

    spy = _ChatwootSpy(conversation_id=7300)
    spy.install(monkeypatch)
    reply_event_id = await _inbound_reply_event(session_maker, dedupe_key="wa:inbound:plain-operator")
    async with session_maker() as session:
        async with session.begin():
            event = await session.get(WhatsAppEvent, reply_event_id)
            await wiw._forward_text_to_chatwoot(
                session,
                event,
                phone_e164=PHONE,
                text="Danke!",
                reply_to_provider_message_id="wamid.PLAIN_OPERATOR",
            )

    assert spy.built_inbox_ids == [KARLSRUHE_INBOX], "no provenance means the sender still proves the branch"


# ---------------------------------------------------------------------------
# 9. The provenance is a pair, and half a pair proves nothing
# ---------------------------------------------------------------------------

_VALID_PROVENANCE = {"route": "general", "sender_scope": "transport_only"}


def _operator_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "message_source": "operator",
        "job_id": None,
        "template_code": "operator_relay",
        "sender_id": SINGLE_SENDER_ID,
        "meta": {CHATWOOT_ROUTE_META_KEY: "general", SINGLE_INBOX_RELAY_META_KEY: dict(_VALID_PROVENANCE)},
    }
    row.update(overrides)
    return row


def test_a_complete_provenance_pair_proves_general() -> None:
    route, error = resolve_operator_outbox_route(**_operator_row())
    assert error is None
    assert route is ChatwootRoute.GENERAL


@pytest.mark.parametrize(
    "meta",
    [
        pytest.param({}, id="no_provenance_at_all"),
        pytest.param({"send_type": "text"}, id="ordinary_relay_meta"),
        pytest.param(None, id="meta_is_null"),
        pytest.param(["general"], id="meta_is_not_a_mapping"),
    ],
)
def test_a_row_without_provenance_keeps_its_sender_tenant(meta: object) -> None:
    route, error = resolve_operator_outbox_route(**_operator_row(meta=meta))
    assert error is None
    assert route is ChatwootRoute.TENANT


@pytest.mark.parametrize(
    "meta, expected",
    [
        pytest.param(
            {CHATWOOT_ROUTE_META_KEY: "general"},
            "operator_route_marker_conflict",
            id="marker_without_provenance",
        ),
        pytest.param(
            {SINGLE_INBOX_RELAY_META_KEY: dict(_VALID_PROVENANCE)},
            "operator_route_marker_conflict",
            id="provenance_without_marker",
        ),
        pytest.param(
            {CHATWOOT_ROUTE_META_KEY: "tenant", SINGLE_INBOX_RELAY_META_KEY: dict(_VALID_PROVENANCE)},
            "invalid_outbox_route_marker",
            id="marker_says_tenant",
        ),
        pytest.param(
            {CHATWOOT_ROUTE_META_KEY: "unknown", SINGLE_INBOX_RELAY_META_KEY: dict(_VALID_PROVENANCE)},
            "invalid_outbox_route_marker",
            id="unknown_marker",
        ),
        pytest.param(
            {CHATWOOT_ROUTE_META_KEY: "general", SINGLE_INBOX_RELAY_META_KEY: {"route": "general"}},
            "operator_route_marker_conflict",
            id="partial_provenance",
        ),
        pytest.param(
            {
                CHATWOOT_ROUTE_META_KEY: "general",
                SINGLE_INBOX_RELAY_META_KEY: {**_VALID_PROVENANCE, "sender_scope": "tenant"},
            },
            "operator_route_marker_conflict",
            id="sender_claims_tenant_scope",
        ),
        pytest.param(
            {
                CHATWOOT_ROUTE_META_KEY: "general",
                SINGLE_INBOX_RELAY_META_KEY: {**_VALID_PROVENANCE, "extra": 1},
            },
            "operator_route_marker_conflict",
            id="unexpected_provenance_key",
        ),
        pytest.param(
            {CHATWOOT_ROUTE_META_KEY: "general", SINGLE_INBOX_RELAY_META_KEY: "general"},
            "operator_route_marker_conflict",
            id="provenance_is_not_a_mapping",
        ),
    ],
)
def test_a_broken_provenance_fails_closed(meta: dict[str, Any], expected: str) -> None:
    route, error = resolve_operator_outbox_route(**_operator_row(meta=meta))
    assert route is None
    assert error == expected


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"message_source": "bot"}, id="not_an_operator_row"),
        pytest.param({"job_id": 42}, id="carries_a_message_job"),
        pytest.param({"template_code": "record_created"}, id="foreign_template"),
        pytest.param({"template_code": None}, id="no_template"),
        pytest.param({"sender_id": None}, id="no_sender"),
    ],
)
def test_a_valid_marker_on_the_wrong_row_fails_closed(overrides: dict[str, Any]) -> None:
    """The row has to BE what the provenance claims — a marker is not enough."""
    route, error = resolve_operator_outbox_route(**_operator_row(**overrides))
    assert route is None
    assert error == "operator_route_marker_conflict"


async def test_a_forged_provenance_blocks_the_reply_before_any_chatwoot_call(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End to end: a half-written row never silently picks either route."""
    _single_inbox_settings(monkeypatch, sender_id=0, mode="affinity", branch_map=BRANCH_MAP)
    await _seed_transport_line(session_maker)

    async with session_maker() as session:
        async with session.begin():
            session.add(
                OutboxMessage(
                    company_id=KARLSRUHE[1],
                    sender_id=SINGLE_SENDER_ID,
                    phone_e164=PHONE,
                    template_code="operator_relay",
                    language="de",
                    body="Bis morgen!",
                    status="sent",
                    scheduled_at=datetime.now(timezone.utc),
                    sent_at=datetime.now(timezone.utc),
                    provider_message_id="wamid.FORGED",
                    message_source="operator",
                    chatwoot_conversation_id=616,
                    chatwoot_message_id=9616,
                    meta={SINGLE_INBOX_RELAY_META_KEY: dict(_VALID_PROVENANCE)},
                )
            )

    spy = _ChatwootSpy()
    spy.install(monkeypatch)
    reply_event_id = await _inbound_reply_event(session_maker, dedupe_key="wa:inbound:forged")
    with pytest.raises(RuntimeError) as blocked:
        async with session_maker() as session:
            async with session.begin():
                event = await session.get(WhatsAppEvent, reply_event_id)
                await wiw._forward_text_to_chatwoot(
                    session,
                    event,
                    phone_e164=PHONE,
                    text="Danke!",
                    reply_to_provider_message_id="wamid.FORGED",
                )

    # Neither route was chosen: the row is refused, not resolved by preferring
    # whichever half of the provenance happens to be present.
    assert str(blocked.value) == "chatwoot tenant routing failed: operator_route_marker_conflict"
    assert spy.built_inbox_ids == [], "fail-closed means no Chatwoot client is built at all"


# ---------------------------------------------------------------------------
# 10. A refused reply must never look delivered
# ---------------------------------------------------------------------------
#
# Terminating the event silently is worse than the original bug: the reply sits
# in Chatwoot looking sent, so the operator believes the customer got it. PR-7.2
# already owed that warning for an unprovable General reply; a refused
# single-inbox relay owes exactly the same one, through the same durable
# post-commit machinery — but with the opposite advice, because during the
# rollback the branch inboxes are precisely where a reply must NOT be written.


class _FailingChatwootSpy(_ChatwootSpy):
    """Chatwoot rejects the note until :attr:`failures` is exhausted."""

    def __init__(self, failures: int = 1) -> None:
        super().__init__()
        self.failures = failures
        self.attempts = 0

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        spy = self

        class _FlakyChatwoot:
            def __init__(self, *args: Any, inbox_id: object = None, **kwargs: Any) -> None:
                spy.built_inbox_ids.append(inbox_id)

            async def send_message(self, conversation_id: int, text: str, **kwargs: Any) -> int:
                spy.attempts += 1
                if spy.attempts <= spy.failures:
                    raise RuntimeError("chatwoot unavailable")
                record = {"conversation_id": conversation_id, "text": text, **kwargs}
                spy.messages.append(record)
                if kwargs.get("private"):
                    spy.notes.append(record)
                return 991

            async def aclose(self) -> None:
                return None

        monkeypatch.setattr(wiw, "ChatwootClient", _FlakyChatwoot)


def _assert_single_inbox_note(spy: _ChatwootSpy, *, conversation_id: int = 610) -> dict[str, Any]:
    assert len(spy.notes) == 1, "the operator must be warned exactly once"
    note = spy.notes[0]
    assert note["private"] is True
    assert note["conversation_id"] == conversation_id
    assert "NICHT" in note["text"]
    assert "General-Inbox" in note["text"], "the rollback advice must point at General"
    # Nothing about the customer, the sender, the configuration or the failure.
    for secret in (PHONE, TEXT, API_TOKEN, SHARED_PNID, BRANCH_MAP, "Anna", str(SINGLE_SENDER_ID)):
        assert secret not in note["text"], f"the note leaked {secret!r}"
    for code in ("single_inbox_", "operator_relay:"):
        assert code not in note["text"], "reason codes belong in the event, not in the operator's note"
    return note


@pytest.mark.parametrize("branch_inbox", BRANCH_INBOXES)
async def test_a_branch_inbox_refusal_warns_the_operator(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    branch_inbox: int,
) -> None:
    _single_inbox_settings(monkeypatch)
    await _seed_shared_line(session_maker)

    provider, spy, event_id = await _run_relay(session_maker, monkeypatch, inbox_id=branch_inbox)
    event, rows = await _event_and_outbox(session_maker, event_id)

    assert provider.sent == [] and provider.templates_sent == []
    assert rows == []
    assert event.error == "operator_relay: single_inbox_not_general", "the stable reason code is unchanged"
    _assert_single_inbox_note(spy)


async def test_a_native_whatsapp_inbox_refusal_warns_the_operator(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _single_inbox_settings(monkeypatch)
    await _seed_shared_line(session_maker)

    provider, spy, event_id = await _run_relay(session_maker, monkeypatch, inbox_id=NATIVE_WHATSAPP_INBOX)
    event, rows = await _event_and_outbox(session_maker, event_id)

    assert provider.sent == []
    assert rows == []
    assert event.error == "operator_relay: single_inbox_not_general"
    _assert_single_inbox_note(spy)


@pytest.mark.parametrize(
    "spec, meta_pnid, expected",
    [
        pytest.param(None, SHARED_PNID, "single_inbox_sender_not_found", id="missing"),
        pytest.param(
            {"tenant": DURLACH, "is_active": False},
            SHARED_PNID,
            "single_inbox_sender_inactive",
            id="inactive",
        ),
        pytest.param(
            {"tenant": DURLACH, "phone_number_id": FOREIGN_PNID},
            SHARED_PNID,
            "single_inbox_sender_phone_mismatch",
            id="mismatched",
        ),
        pytest.param(
            {"tenant": ("unknown_crm", 999703)},
            SHARED_PNID,
            "single_inbox_sender_identity_invalid",
            id="invalid_identity",
        ),
    ],
)
async def test_an_unproved_sender_refusal_warns_the_operator(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    spec: dict[str, Any] | None,
    meta_pnid: str,
    expected: str,
) -> None:
    _single_inbox_settings(monkeypatch, meta_pnid=meta_pnid)

    if spec is None:
        async with session_maker() as session:
            async with session.begin():
                session.add(_sender(LOW_SENDER_ID, KARLSRUHE))
                session.add(_open_window_event())
    else:
        await _seed_shared_line(session_maker, configured=_sender(SINGLE_SENDER_ID, **spec))

    provider, spy, event_id = await _run_relay(session_maker, monkeypatch)
    event, rows = await _event_and_outbox(session_maker, event_id)

    assert provider.sent == [] and provider.templates_sent == []
    assert rows == [], "no Meta call means no Outbox at all — never a false 'sent' row"
    assert event.error == f"operator_relay: {expected}"
    _assert_single_inbox_note(spy)


async def test_a_config_fault_warns_the_operator_too(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The operator cannot see the env — silence would look like delivery."""
    _single_inbox_settings(monkeypatch, mode="context")
    await _seed_shared_line(session_maker)

    provider, spy, event_id = await _run_relay(session_maker, monkeypatch)
    event, rows = await _event_and_outbox(session_maker, event_id)

    assert provider.sent == []
    assert rows == []
    assert event.error == "operator_relay: single_inbox_config_invalid"
    _assert_single_inbox_note(spy)


async def test_a_note_that_chatwoot_rejects_is_retried_and_never_duplicated(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Durable, commit-safe, at-least-once — and idempotent on re-dispatch."""
    _single_inbox_settings(monkeypatch)
    await _seed_shared_line(session_maker)

    monkeypatch.setattr(wiw, "SessionLocal", session_maker)
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    flaky = _FailingChatwootSpy(failures=1)
    flaky.install(monkeypatch)

    provider = _CountingProvider()
    async with session_maker() as session:
        async with session.begin():
            event = WhatsAppEvent(
                dedupe_key="chatwoot_out:620:9620",
                status="received",
                query={},
                headers={},
                payload={
                    "_chatwoot_operator_relay": {
                        "recipient_phone": PHONE,
                        "text": TEXT,
                        "conversation_id": 620,
                        "message_id": 9620,
                        "phone_number_id": SHARED_PNID,
                        "chatwoot_inbox_id": BRANCH_INBOXES[0],
                        "agent_name": "Anna",
                    },
                },
                chatwoot_conversation_id=620,
            )
            session.add(event)
            await session.flush()
            event_id = int(event.id)

    await wiw.process_one_event(event_id, provider)

    # First attempt failed: nothing delivered, but the obligation survived the
    # commit rather than dying with the process.
    assert flaky.notes == []
    async with session_maker() as session:
        stored = await session.get(WhatsAppEvent, event_id)
        marker = (stored.payload or {})[wiw._GENERAL_NOTE_KEY]
    assert stored.error == "operator_relay: single_inbox_not_general"
    assert marker["status"] == wiw._NOTE_PENDING
    assert marker["reason"] == "operator_relay: single_inbox_not_general"

    dispatched = await wiw.dispatch_pending_general_affinity_notes()
    assert dispatched == 1
    _assert_single_inbox_note(flaky, conversation_id=620)

    # Re-running recovery must not warn the operator twice.
    assert await wiw.dispatch_pending_general_affinity_notes() == 0
    assert len(flaky.notes) == 1
    assert provider.sent == [], "recovery only finishes the note; it never replays the relay"


async def test_an_unprovable_general_reply_keeps_its_own_branch_advice(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-7.2's note is unchanged: with the map configured, branches are right."""
    _single_inbox_settings(monkeypatch, sender_id=0, mode="affinity", branch_map=BRANCH_MAP)
    await _seed_shared_line(session_maker)

    provider, spy, event_id = await _run_relay(session_maker, monkeypatch)
    event, _rows = await _event_and_outbox(session_maker, event_id)

    assert provider.sent == []
    assert event.error == "operator_relay: general_affinity_no_evidence"
    assert len(spy.notes) == 1
    assert "Filial-Inbox" in spy.notes[0]["text"]
    assert "General-Inbox" not in spy.notes[0]["text"]


async def test_the_hotfix_being_off_adds_no_note_where_there_was_none(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`ambiguous_sender` was silent before this hotfix and stays silent."""
    _single_inbox_settings(monkeypatch, sender_id=0)
    await _seed_shared_line(session_maker)

    provider, spy, event_id = await _run_relay(session_maker, monkeypatch)
    event, rows = await _event_and_outbox(session_maker, event_id)

    assert provider.sent == []
    assert rows == []
    assert event.error == "operator_relay: ambiguous_sender"
    assert spy.notes == []


# ---------------------------------------------------------------------------
# 11. One trust model for an operator row, in BOTH resolvers
# ---------------------------------------------------------------------------
#
# `_get_outbox_context_target` (reply/reaction) demands the complete provenance
# pair, while `chatwoot_affinity._identity_of_outbox` used to accept a bare
# `chatwoot_route=general` and answer "proves nothing". The same row could
# therefore be a deliberate General row to one resolver and a broken audit row
# to the other — and a half-written marker quietly became NO_EVIDENCE instead of
# blocking. Both now ask `resolve_operator_outbox_route`.


async def _delivered_operator_outbox(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    meta: dict[str, Any] | None,
    sender_id: int = SINGLE_SENDER_ID,
    company_id: int = KARLSRUHE[1],
    template_code: str = "operator_relay",
    wamid: str = "wamid.AFFINITY_PROBE",
) -> None:
    """One delivered operator message — the strongest tenant evidence there is."""
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                OutboxMessage(
                    company_id=company_id,
                    sender_id=sender_id,
                    phone_e164=PHONE,
                    template_code=template_code,
                    language="de",
                    body="Bis morgen!",
                    status="delivered",
                    scheduled_at=now,
                    sent_at=now,
                    provider_message_id=wamid,
                    message_source="operator",
                    chatwoot_conversation_id=630,
                    chatwoot_message_id=9630,
                    meta=meta,
                )
            )


async def test_an_operator_row_without_provenance_still_proves_its_branch(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The control: nothing about ordinary operator evidence changed."""
    _single_inbox_settings(monkeypatch, sender_id=0, mode="affinity", branch_map=BRANCH_MAP)
    await _seed_transport_line(session_maker)
    await _delivered_operator_outbox(session_maker, meta={"send_type": "text"})

    async with session_maker() as session:
        result = await resolve_tenant_affinity(session, [PHONE])

    assert result.outcome is AffinityOutcome.PROVEN
    assert (result.identity.provider, result.identity.company_id) == KARLSRUHE


async def test_a_full_hotfix_provenance_proves_no_branch_at_all(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _single_inbox_settings(monkeypatch, sender_id=0, mode="affinity", branch_map=BRANCH_MAP)
    await _seed_transport_line(session_maker)
    await _delivered_operator_outbox(
        session_maker,
        meta={
            "send_type": "text",
            CHATWOOT_ROUTE_META_KEY: "general",
            SINGLE_INBOX_RELAY_META_KEY: dict(_VALID_PROVENANCE),
        },
    )

    async with session_maker() as session:
        result = await resolve_tenant_affinity(session, [PHONE])

    assert result.outcome is AffinityOutcome.NO_EVIDENCE, "the transport line is not a branch"
    assert result.identity is None


@pytest.mark.parametrize(
    "meta, expected_reason",
    [
        pytest.param(
            {CHATWOOT_ROUTE_META_KEY: "general"},
            "operator_route_marker_conflict",
            id="marker_only",
        ),
        pytest.param(
            {SINGLE_INBOX_RELAY_META_KEY: dict(_VALID_PROVENANCE)},
            "operator_route_marker_conflict",
            id="provenance_only",
        ),
        pytest.param(
            {CHATWOOT_ROUTE_META_KEY: "general", SINGLE_INBOX_RELAY_META_KEY: {"route": "general"}},
            "operator_route_marker_conflict",
            id="malformed_provenance",
        ),
        pytest.param(
            {
                CHATWOOT_ROUTE_META_KEY: "general",
                SINGLE_INBOX_RELAY_META_KEY: {**_VALID_PROVENANCE, "sender_scope": "tenant"},
            },
            "operator_route_marker_conflict",
            id="provenance_claims_tenant_scope",
        ),
        pytest.param(
            {CHATWOOT_ROUTE_META_KEY: "tenant", SINGLE_INBOX_RELAY_META_KEY: dict(_VALID_PROVENANCE)},
            "invalid_outbox_route_marker",
            id="conflicting_route_and_provenance",
        ),
        pytest.param(
            {CHATWOOT_ROUTE_META_KEY: "unknown", SINGLE_INBOX_RELAY_META_KEY: dict(_VALID_PROVENANCE)},
            "invalid_outbox_route_marker",
            id="unknown_marker",
        ),
    ],
)
async def test_a_broken_operator_provenance_blocks_affinity_instead_of_proving_nothing(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    meta: dict[str, Any],
    expected_reason: str,
) -> None:
    """INVALID, never NO_EVIDENCE: a contradictory row must not fall to General."""
    _single_inbox_settings(monkeypatch, sender_id=0, mode="affinity", branch_map=BRANCH_MAP)
    await _seed_transport_line(session_maker)
    await _delivered_operator_outbox(session_maker, meta=meta)

    async with session_maker() as session:
        result = await resolve_tenant_affinity(session, [PHONE])

    assert result.outcome is AffinityOutcome.INVALID
    assert result.identity is None
    assert result.reason == expected_reason
    assert result.source == "communication"
    # The safe dict is what reaches logs: ids and codes only.
    assert PHONE not in str(result.as_safe_dict())


async def test_a_marker_on_the_wrong_operator_row_blocks_affinity(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A complete pair on a foreign template is still not a relay row."""
    _single_inbox_settings(monkeypatch, sender_id=0, mode="affinity", branch_map=BRANCH_MAP)
    await _seed_transport_line(session_maker)
    await _delivered_operator_outbox(
        session_maker,
        template_code="record_created",
        meta={
            CHATWOOT_ROUTE_META_KEY: "general",
            SINGLE_INBOX_RELAY_META_KEY: dict(_VALID_PROVENANCE),
        },
    )

    async with session_maker() as session:
        result = await resolve_tenant_affinity(session, [PHONE])

    assert result.outcome is AffinityOutcome.INVALID
    assert result.reason == "operator_route_marker_conflict"


async def test_the_jobless_bot_general_contract_is_untouched(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A STOP ACK still proves nothing through its own, unchanged contract."""
    _single_inbox_settings(monkeypatch, sender_id=0, mode="affinity", branch_map=BRANCH_MAP)
    await _seed_transport_line(session_maker)

    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                OutboxMessage(
                    company_id=KARLSRUHE[1],
                    phone_e164=PHONE,
                    template_code="wa_cmd_stop",
                    language="de",
                    body="",
                    status="delivered",
                    scheduled_at=now,
                    sent_at=now,
                    provider_message_id="wamid.STOP_ACK",
                    message_source="bot",
                    meta={
                        CHATWOOT_ROUTE_META_KEY: "general",
                        "source": "inbound_command",
                        "command": "stop",
                    },
                )
            )

    async with session_maker() as session:
        result = await resolve_tenant_affinity(session, [PHONE])

    assert result.outcome is AffinityOutcome.NO_EVIDENCE
    assert result.identity is None
