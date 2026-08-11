"""Tests for inbound WhatsApp reactions → Chatwoot forwarding.

A reaction is a first-class inbound action (``messages[].type == "reaction"``):
it is mirrored into Chatwoot as an incoming message and must never trigger
commands, opt-out, promo, or any send back to WhatsApp.  Native reply
(``content_attributes.in_reply_to``) is used only when the reacted-to message
has a real Chatwoot message id in the same conversation; automatic outbox
targets and unknown targets use a visible fallback line.
"""

from __future__ import annotations

import inspect
from datetime import datetime, timezone
from typing import Any, Callable
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from altegio_bot.chatwoot_outbox_route import CHATWOOT_ROUTE_META_KEY
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    MessageJob,
    OutboxMessage,
    Record,
    WhatsAppEvent,
    WhatsAppSender,
)
from altegio_bot.providers.base import ChatwootRoute, WhatsAppProvider
from altegio_bot.workers.whatsapp_inbox_worker import (
    ReactionTarget,
    _extract_actions,
    _resolve_reaction_target,
    handle_event,
)

PHONE_NUMBER_ID = "PNID_REACT"
FROM_PHONE = "381638400431"
PHONE_E164 = "+381638400431"
TARGET_WAMID = "wamid.TARGET"
REACTION_WAMID = "wamid.REACTION"
DEST_CONVERSATION_ID = 555
MESSAGE_ID = 9100
BRANCH_MAP = (
    '{"101":{"provider":"easyweek","company_id":900001},'
    '"102":{"provider":"easyweek","company_id":900002},'
    '"103":{"provider":"altegio","company_id":900003}}'
)

GENERAL_REACTION_PROVENANCE_CASES = [
    ("wa_cmd_stop", {"source": "inbound_command", "command": "stop"}),
    ("wa_cmd_start", {"source": "inbound_command", "command": "start"}),
    ("wa_promo_info", {"source": "promo_lead", "command": "promo"}),
    ("wa_promo_lead_issued", {"source": "promo_lead", "command": "promo"}),
]


def _general_reaction_meta(provenance: dict[str, Any], *, marked: bool) -> dict[str, Any]:
    meta = dict(provenance)
    if marked:
        meta[CHATWOOT_ROUTE_META_KEY] = ChatwootRoute.GENERAL.value
    return meta


class _CaptureProvider(WhatsAppProvider):
    """Records any send so reaction tests can assert nothing went to WhatsApp."""

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
        return "wamid.SHOULD_NOT_SEND"


def _reaction_payload(
    *,
    emoji: Any = "👍",
    target_wamid: Any = TARGET_WAMID,
    reaction_wamid: str = REACTION_WAMID,
    from_phone: str = FROM_PHONE,
    extra_statuses: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    reaction: dict[str, Any] = {}
    if emoji is not None:
        reaction["emoji"] = emoji
    if target_wamid is not None:
        reaction["message_id"] = target_wamid
    msg: dict[str, Any] = {
        "from": from_phone,
        "id": reaction_wamid,
        "timestamp": "1700000000",
        "type": "reaction",
        "reaction": reaction,
    }
    value: dict[str, Any] = {
        "metadata": {"phone_number_id": PHONE_NUMBER_ID},
        "messages": [msg],
    }
    if extra_statuses is not None:
        value["statuses"] = extra_statuses
    return {"entry": [{"changes": [{"value": value}]}]}


def _mock_chatwoot_client(
    conversation_id: int = DEST_CONVERSATION_ID,
    message_id: int = MESSAGE_ID,
) -> tuple[MagicMock, MagicMock]:
    inst = MagicMock()
    inst.get_or_create_incoming_conversation = AsyncMock(return_value=conversation_id)
    inst.send_message = AsyncMock(return_value=message_id)
    inst.aclose = AsyncMock(return_value=None)
    cls = MagicMock(return_value=inst)
    return cls, inst


def _outbox(
    *,
    provider_message_id: str = TARGET_WAMID,
    phone_e164: str = PHONE_E164,
    template_code: str = "reminder_24h",
    record_id: int | None = None,
    message_source: str = "bot",
    chatwoot_message_id: int | None = None,
    chatwoot_conversation_id: int | None = None,
    body: str = "Ваша запись завтра в 10:00",
    company_id: int = 1,
    job_id: int | None = None,
    sender_id: int | None = None,
    created_at: datetime | None = None,
    meta: dict[str, Any] | None = None,
) -> OutboxMessage:
    now = datetime.now(timezone.utc)
    ob = OutboxMessage(
        company_id=company_id,
        phone_e164=phone_e164,
        template_code=template_code,
        language="de",
        body=body,
        status="sent",
        provider_message_id=provider_message_id,
        scheduled_at=now,
        sent_at=now,
        message_source=message_source,
        job_id=job_id,
        sender_id=sender_id,
        record_id=record_id,
        chatwoot_message_id=chatwoot_message_id,
        chatwoot_conversation_id=chatwoot_conversation_id,
        meta=meta or {},
    )
    if created_at is not None:
        ob.created_at = created_at
    return ob


def _prior_inbound_event(
    *,
    whatsapp_message_id: str = TARGET_WAMID,
    chatwoot_message_id: int | None = 456,
    forwarded_chatwoot_conversation_id: int | None = DEST_CONVERSATION_ID,
    from_phone: str = FROM_PHONE,
    dedupe_key: str | None = None,
) -> WhatsAppEvent:
    return WhatsAppEvent(
        dedupe_key=dedupe_key or f"wa:prior-inbound:{whatsapp_message_id}",
        status="processed",
        error=None,
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
                                        "from": from_phone,
                                        "id": whatsapp_message_id,
                                        "type": "text",
                                        "text": {"body": "Здравствуйте"},
                                    }
                                ],
                            }
                        }
                    ]
                }
            ]
        },
        chatwoot_message_id=chatwoot_message_id,
        forwarded_chatwoot_conversation_id=forwarded_chatwoot_conversation_id,
        whatsapp_message_id=whatsapp_message_id,
    )


def _make_event(payload: dict[str, Any], dedupe_key: str = "wa:reaction-test") -> WhatsAppEvent:
    return WhatsAppEvent(
        dedupe_key=dedupe_key,
        status="received",
        error=None,
        query={},
        headers={},
        payload=payload,
    )


async def _run_reaction(
    session_maker,
    *,
    payload: dict[str, Any],
    seeds: Callable[[Any], None] | None = None,
    destination_conversation_id: int = DEST_CONVERSATION_ID,
    message_id: int = MESSAGE_ID,
    dedupe_key: str = "wa:reaction-test",
    provider: WhatsAppProvider | None = None,
) -> tuple[WhatsAppEvent, MagicMock]:
    provider = provider or _CaptureProvider()
    async with session_maker() as session:
        async with session.begin():
            if seeds is not None:
                maybe = seeds(session)
                if inspect.isawaitable(maybe):
                    await maybe

            evt = _make_event(payload, dedupe_key=dedupe_key)
            session.add(evt)
            await session.flush()

            mock_cls, mock_inst = _mock_chatwoot_client(
                conversation_id=destination_conversation_id,
                message_id=message_id,
            )
            with patch("altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient", mock_cls):
                await handle_event(session, evt, provider)

    return evt, mock_inst


async def _seed_proven_reaction_target(
    session: Any,
    *,
    provider: str,
    company_id: int,
    source: str,
    wamid: str,
    phone_e164: str,
    suffix: str,
) -> None:
    now = datetime.now(timezone.utc)
    if source == "operator":
        sender = WhatsAppSender(
            provider=provider,
            company_id=company_id,
            sender_code=f"react-{company_id}-{suffix[-8:]}",
            phone_number_id=PHONE_NUMBER_ID,
            display_phone="+49",
            is_active=True,
        )
        session.add(sender)
        await session.flush()
        session.add(
            _outbox(
                provider_message_id=wamid,
                phone_e164=phone_e164,
                company_id=company_id,
                message_source="operator",
                template_code="operator_relay",
                sender_id=sender.id,
                chatwoot_message_id=123,
                chatwoot_conversation_id=DEST_CONVERSATION_ID,
            )
        )
        return

    job = MessageJob(
        provider=provider,
        company_id=company_id,
        job_type="reminder_24h",
        run_at=now,
        status="done",
        dedupe_key=f"reaction-route:{suffix}:{wamid}",
        payload={},
    )
    session.add(job)
    await session.flush()
    session.add(
        _outbox(
            provider_message_id=wamid,
            phone_e164=phone_e164,
            company_id=company_id,
            job_id=job.id,
        )
    )


async def _run_tenant_reaction(
    session_maker: Any,
    monkeypatch: pytest.MonkeyPatch,
    *,
    raw_map: str,
    targets: list[tuple[str, int, str]],
    target_wamid: str | None = TARGET_WAMID,
    target_phone: str = PHONE_E164,
    from_phone: str = FROM_PHONE,
    dedupe_key: str = "wa:tenant-reaction",
    destination_conversation_id: int = DEST_CONVERSATION_ID,
    expected_error: str | None = None,
    prior_inbound: bool = False,
    prior_inbound_events: list[WhatsAppEvent] | None = None,
    general_inbox_id: int = 999,
    outboxes: list[OutboxMessage] | None = None,
) -> tuple[WhatsAppEvent, MagicMock, MagicMock]:
    import altegio_bot.workers.whatsapp_inbox_worker as wiw

    monkeypatch.setattr(wiw.settings, "chatwoot_inbox_company_map", raw_map)
    monkeypatch.setattr(wiw.settings, "chatwoot_inbox_id", general_inbox_id)
    provider = _CaptureProvider()
    mock_cls, mock_inst = _mock_chatwoot_client(conversation_id=destination_conversation_id)

    async with session_maker() as session:
        async with session.begin():
            for index, (target_provider, company_id, source) in enumerate(targets):
                assert target_wamid is not None
                await _seed_proven_reaction_target(
                    session,
                    provider=target_provider,
                    company_id=company_id,
                    source=source,
                    wamid=target_wamid,
                    phone_e164=target_phone,
                    suffix=f"{dedupe_key}-{index}",
                )
            if prior_inbound:
                assert target_wamid is not None
                session.add(_prior_inbound_event(whatsapp_message_id=target_wamid))
            session.add_all(prior_inbound_events or [])
            session.add_all(outboxes or [])

            evt = _make_event(
                _reaction_payload(target_wamid=target_wamid, from_phone=from_phone),
                dedupe_key=dedupe_key,
            )
            session.add(evt)
            await session.flush()

            with patch("altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient", mock_cls):
                if expected_error is None:
                    await handle_event(session, evt, provider)
                else:
                    with pytest.raises(RuntimeError, match=expected_error):
                        await handle_event(session, evt, provider)

    return evt, mock_cls, mock_inst


# ---------------------------------------------------------------------------
# 1. extraction
# ---------------------------------------------------------------------------


def test_extract_actions_reaction() -> None:
    actions = _extract_actions(_reaction_payload())
    assert len(actions) == 1
    a = actions[0]
    assert a["kind"] == "reaction"
    assert a["reaction_emoji"] == "👍"
    assert a["reaction_target_provider_message_id"] == TARGET_WAMID
    assert a["whatsapp_message_id"] == REACTION_WAMID
    assert a["phone_e164"] == PHONE_E164
    assert a["cmd"] is None


@pytest.mark.parametrize(
    ("tenant_provider", "company_id", "inbox_id"),
    [
        (PROVIDER_EASYWEEK, 900001, 101),
        (PROVIDER_EASYWEEK, 900002, 102),
        (PROVIDER_ALTEGIO, 900003, 103),
    ],
    ids=["durlach", "rastatt", "karlsruhe"],
)
@pytest.mark.asyncio
async def test_reaction_to_lifecycle_notification_uses_its_branch_inbox(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    tenant_provider: str,
    company_id: int,
    inbox_id: int,
) -> None:
    evt, mock_cls, cw = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=BRANCH_MAP,
        targets=[(tenant_provider, company_id, "bot")],
        dedupe_key=f"wa:tenant-reaction:{inbox_id}",
    )

    mock_cls.assert_called_once_with(inbox_id=inbox_id)
    cw.send_message.assert_called_once()
    assert evt.forwarded_chatwoot_conversation_id == DEST_CONVERSATION_ID
    assert evt.error is None


@pytest.mark.asyncio
async def test_reaction_to_operator_message_uses_sender_tenant_branch(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evt, mock_cls, cw = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=BRANCH_MAP,
        targets=[(PROVIDER_EASYWEEK, 900001, "operator")],
        dedupe_key="wa:tenant-reaction:operator",
    )

    mock_cls.assert_called_once_with(inbox_id=101)
    assert cw.send_message.call_args.kwargs["content_attributes"]["in_reply_to"] == 123
    assert evt.error is None


@pytest.mark.asyncio
async def test_same_phone_reactions_keep_separate_branch_conversations(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    du_event, du_cls, _du = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=BRANCH_MAP,
        targets=[(PROVIDER_EASYWEEK, 900001, "bot")],
        target_wamid="wamid.DU.REACT",
        dedupe_key="wa:tenant-reaction:du",
        destination_conversation_id=2101,
    )
    ra_event, ra_cls, _ra = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=BRANCH_MAP,
        targets=[(PROVIDER_EASYWEEK, 900002, "bot")],
        target_wamid="wamid.RA.REACT",
        dedupe_key="wa:tenant-reaction:ra",
        destination_conversation_id=2102,
    )

    du_cls.assert_called_once_with(inbox_id=101)
    ra_cls.assert_called_once_with(inbox_id=102)
    assert du_event.forwarded_chatwoot_conversation_id == 2101
    assert ra_event.forwarded_chatwoot_conversation_id == 2102


@pytest.mark.parametrize(
    ("target_wamid", "target_phone", "targets", "dedupe_key"),
    [
        (None, PHONE_E164, [], "wa:tenant-reaction:no-target"),
        ("wamid.UNKNOWN", PHONE_E164, [], "wa:tenant-reaction:unknown"),
        (
            "wamid.WRONG.PHONE",
            "+10000000001",
            [(PROVIDER_EASYWEEK, 900001, "bot")],
            "wa:tenant-reaction:wrong-phone",
        ),
    ],
    ids=["no-target", "unknown-target", "wrong-phone"],
)
@pytest.mark.asyncio
async def test_reaction_without_proven_target_uses_general_inbox(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    target_wamid: str | None,
    target_phone: str,
    targets: list[tuple[str, int, str]],
    dedupe_key: str,
) -> None:
    evt, mock_cls, cw = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=BRANCH_MAP,
        targets=targets,
        target_wamid=target_wamid,
        target_phone=target_phone,
        dedupe_key=dedupe_key,
    )

    mock_cls.assert_called_once_with(inbox_id=999)
    cw.send_message.assert_called_once()
    assert evt.error is None


@pytest.mark.parametrize(
    ("raw_map", "expected_reason"),
    [
        (
            '{"102":{"provider":"easyweek","company_id":900002}}',
            "tenant_mapping_missing",
        ),
        (
            '{"101":{"provider":"easyweek","company_id":900001},"102":{"provider":"easyweek","company_id":900001}}',
            "invalid_inbox_company_map",
        ),
        ('{"101":900001}', "provider_scope_missing"),
    ],
    ids=["missing-route", "invalid-map", "legacy-unscoped-map"],
)
@pytest.mark.asyncio
async def test_reaction_with_found_target_and_unusable_route_fails_before_chatwoot_client(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    raw_map: str,
    expected_reason: str,
) -> None:
    evt, mock_cls, cw = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=raw_map,
        targets=[(PROVIDER_EASYWEEK, 900001, "bot")],
        dedupe_key=f"wa:tenant-reaction:blocked:{expected_reason}",
        expected_error=expected_reason,
    )

    mock_cls.assert_not_called()
    cw.send_message.assert_not_called()
    assert evt.error == f"chatwoot tenant routing failed: {expected_reason}"


@pytest.mark.asyncio
async def test_reaction_provider_collision_fails_closed_before_chatwoot_client(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collision_map = (
        '{"101":{"provider":"easyweek","company_id":900001},"201":{"provider":"altegio","company_id":900001}}'
    )
    evt, mock_cls, cw = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=collision_map,
        targets=[
            (PROVIDER_EASYWEEK, 900001, "bot"),
            (PROVIDER_ALTEGIO, 900001, "bot"),
        ],
        dedupe_key="wa:tenant-reaction:provider-collision",
        expected_error="ambiguous_outbox_tenant_identity",
        # A duplicate prior inbound event must not mask an authoritative but
        # ambiguous Outbox target and downgrade it to General.
        prior_inbound=True,
    )

    mock_cls.assert_not_called()
    cw.send_message.assert_not_called()
    assert evt.error == "chatwoot tenant routing failed: ambiguous_outbox_tenant_identity"


@pytest.mark.asyncio
async def test_configured_map_reuses_exact_prior_inbound_conversation(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evt, mock_cls, cw = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=BRANCH_MAP,
        targets=[],
        dedupe_key="wa:tenant-reaction:prior-inbound-unproven",
        prior_inbound=True,
    )

    mock_cls.assert_called_once_with()
    cw.get_or_create_incoming_conversation.assert_not_called()
    assert cw.send_message.call_args.args[0] == DEST_CONVERSATION_ID
    assert cw.send_message.call_args.kwargs["content_attributes"]["in_reply_to"] == 456
    cw.send_message.assert_called_once()
    assert evt.error is None


@pytest.mark.parametrize(
    ("branch", "target_wamid", "chatwoot_message_id", "conversation_id"),
    [
        ("durlach", "wamid.DU.PRIOR.REACT", 801, 2101),
        ("rastatt", "wamid.RA.PRIOR.REACT", 802, 2102),
    ],
)
@pytest.mark.asyncio
async def test_reaction_to_prior_inbound_reuses_exact_branch_conversation_without_lookup(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    branch: str,
    target_wamid: str,
    chatwoot_message_id: int,
    conversation_id: int,
) -> None:
    evt, mock_cls, cw = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=BRANCH_MAP,
        targets=[],
        target_wamid=target_wamid,
        dedupe_key=f"wa:chained-reaction:{branch}",
        destination_conversation_id=9999,
        prior_inbound_events=[
            _prior_inbound_event(
                whatsapp_message_id=target_wamid,
                chatwoot_message_id=chatwoot_message_id,
                forwarded_chatwoot_conversation_id=conversation_id,
                dedupe_key=f"wa:prior-reaction:{branch}",
            )
        ],
    )

    mock_cls.assert_called_once_with()
    cw.get_or_create_incoming_conversation.assert_not_called()
    call = cw.send_message.call_args
    assert call.args[0] == conversation_id
    assert call.args[1] == "👍"
    assert call.kwargs["content_attributes"]["in_reply_to"] == chatwoot_message_id
    assert evt.forwarded_chatwoot_conversation_id == conversation_id
    assert evt.error is None


@pytest.mark.asyncio
async def test_same_phone_prior_inbound_reaction_wamids_do_not_mix_du_and_ra_conversations(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    du_event, _du_cls, du_cw = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=BRANCH_MAP,
        targets=[],
        target_wamid="wamid.DU.REACT.CHAIN",
        dedupe_key="wa:du-reaction-chain",
        prior_inbound_events=[
            _prior_inbound_event(
                whatsapp_message_id="wamid.DU.REACT.CHAIN",
                chatwoot_message_id=811,
                forwarded_chatwoot_conversation_id=2201,
                dedupe_key="wa:du-reaction-target",
            )
        ],
    )
    ra_event, _ra_cls, ra_cw = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=BRANCH_MAP,
        targets=[],
        target_wamid="wamid.RA.REACT.CHAIN",
        dedupe_key="wa:ra-reaction-chain",
        prior_inbound_events=[
            _prior_inbound_event(
                whatsapp_message_id="wamid.RA.REACT.CHAIN",
                chatwoot_message_id=812,
                forwarded_chatwoot_conversation_id=2202,
                dedupe_key="wa:ra-reaction-target",
            )
        ],
    )

    assert du_cw.send_message.call_args.args[0] == 2201
    assert ra_cw.send_message.call_args.args[0] == 2202
    assert du_event.forwarded_chatwoot_conversation_id == 2201
    assert ra_event.forwarded_chatwoot_conversation_id == 2202


@pytest.mark.parametrize(
    ("raw_map", "expected_reason"),
    [
        ("{not json", "invalid_inbox_company_map"),
        ('{"101":900001}', "provider_scope_missing"),
    ],
)
@pytest.mark.asyncio
async def test_unknown_reaction_with_unusable_map_fails_before_chatwoot_client(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    raw_map: str,
    expected_reason: str,
) -> None:
    evt, mock_cls, cw = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=raw_map,
        targets=[],
        target_wamid="wamid.UNKNOWN",
        dedupe_key=f"wa:unknown-reaction:{expected_reason}",
        expected_error=expected_reason,
    )

    mock_cls.assert_not_called()
    cw.send_message.assert_not_called()
    assert evt.error == f"chatwoot tenant routing failed: {expected_reason}"


@pytest.mark.asyncio
async def test_unknown_reaction_general_overlap_fails_before_chatwoot_client(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evt, mock_cls, cw = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=BRANCH_MAP,
        targets=[],
        target_wamid="wamid.UNKNOWN",
        dedupe_key="wa:unknown-reaction:general-overlap",
        general_inbox_id=101,
        expected_error="general_inbox_overlaps_branch",
    )

    mock_cls.assert_not_called()
    cw.send_message.assert_not_called()
    assert evt.error == "chatwoot tenant routing failed: general_inbox_overlaps_branch"


@pytest.mark.parametrize(("template_code", "provenance"), GENERAL_REACTION_PROVENANCE_CASES)
@pytest.mark.asyncio
async def test_markerless_historical_direct_reaction_returns_to_validated_general(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    template_code: str,
    provenance: dict[str, Any],
) -> None:
    wamid = f"wamid.HISTORICAL.REACTION.{template_code}"
    evt, mock_cls, cw = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=BRANCH_MAP,
        targets=[],
        target_wamid=wamid,
        dedupe_key=f"wa:historical-reaction:{template_code}",
        outboxes=[
            _outbox(
                provider_message_id=wamid,
                template_code=template_code,
                meta=_general_reaction_meta(provenance, marked=False),
            )
        ],
    )

    mock_cls.assert_called_once_with(inbox_id=999)
    cw.send_message.assert_called_once()
    assert evt.error is None


@pytest.mark.parametrize(
    ("raw_map", "general_inbox_id", "expected_reason"),
    [
        (BRANCH_MAP, 101, "general_inbox_overlaps_branch"),
        (BRANCH_MAP, 0, "invalid_general_inbox_id"),
        ("{not json", 999, "invalid_inbox_company_map"),
        ('{"101":900001}', 999, "provider_scope_missing"),
    ],
)
@pytest.mark.asyncio
async def test_general_outbox_reaction_invalid_route_fails_before_chatwoot_client(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    raw_map: str,
    general_inbox_id: int,
    expected_reason: str,
) -> None:
    wamid = f"wamid.GENERAL.REACTION.BLOCKED.{expected_reason}"
    evt, mock_cls, cw = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=raw_map,
        targets=[],
        target_wamid=wamid,
        dedupe_key=f"wa:general-reaction-blocked:{expected_reason}",
        general_inbox_id=general_inbox_id,
        expected_error=expected_reason,
        outboxes=[
            _outbox(
                provider_message_id=wamid,
                template_code="wa_promo_info",
                meta=_general_reaction_meta({"source": "promo_lead", "command": "promo"}, marked=True),
            )
        ],
    )

    mock_cls.assert_not_called()
    cw.send_message.assert_not_called()
    assert evt.error == f"chatwoot tenant routing failed: {expected_reason}"


@pytest.mark.parametrize(
    ("template_code", "meta", "expected_reason"),
    [
        ("wa_unproven", {}, "bot_job_identity_missing"),
        (
            "wa_cmd_stop",
            {
                "source": "inbound_command",
                "command": "stop",
                CHATWOOT_ROUTE_META_KEY: "unknown",
            },
            "invalid_outbox_route_marker",
        ),
    ],
)
@pytest.mark.asyncio
async def test_unproven_general_like_reaction_stays_fail_closed(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    template_code: str,
    meta: dict[str, Any],
    expected_reason: str,
) -> None:
    wamid = f"wamid.UNPROVEN.REACTION.{expected_reason}"
    evt, mock_cls, cw = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=BRANCH_MAP,
        targets=[],
        target_wamid=wamid,
        dedupe_key=f"wa:unproven-reaction:{expected_reason}",
        expected_error=expected_reason,
        outboxes=[_outbox(provider_message_id=wamid, template_code=template_code, meta=meta)],
    )

    mock_cls.assert_not_called()
    cw.send_message.assert_not_called()
    assert evt.error == f"chatwoot tenant routing failed: {expected_reason}"


@pytest.mark.asyncio
async def test_duplicate_general_reaction_rows_prove_one_general_route(session_maker) -> None:
    wamid = "wamid.DUPLICATE.GENERAL.REACTION"
    meta = _general_reaction_meta({"source": "promo_lead", "command": "promo"}, marked=True)
    async with session_maker() as session:
        async with session.begin():
            session.add_all(
                [
                    _outbox(provider_message_id=wamid, template_code="wa_promo_info", meta=meta),
                    _outbox(provider_message_id=wamid, template_code="wa_promo_info", meta=meta),
                ]
            )
            await session.flush()
            target = await _resolve_reaction_target(session, wamid, phone_e164=PHONE_E164)

    assert target.kind == "outbox_message"
    assert target.chatwoot_route is ChatwootRoute.GENERAL
    assert target.tenant_provider is None
    assert target.company_id is None
    assert target.tenant_error is None


@pytest.mark.asyncio
async def test_general_and_tenant_reaction_rows_are_ambiguous(session_maker) -> None:
    wamid = "wamid.GENERAL.TENANT.REACTION.COLLISION"
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            job = MessageJob(
                provider=PROVIDER_EASYWEEK,
                company_id=900001,
                job_type="record_created",
                run_at=now,
                status="done",
                dedupe_key="general-tenant-reaction-collision",
                payload={},
            )
            session.add(job)
            await session.flush()
            session.add_all(
                [
                    _outbox(
                        provider_message_id=wamid,
                        template_code="wa_cmd_start",
                        meta=_general_reaction_meta(
                            {"source": "inbound_command", "command": "start"},
                            marked=True,
                        ),
                    ),
                    _outbox(provider_message_id=wamid, company_id=900001, job_id=job.id),
                ]
            )
            await session.flush()
            target = await _resolve_reaction_target(session, wamid, phone_e164=PHONE_E164)

    assert target.kind == "outbox_message"
    assert target.tenant_error == "ambiguous_outbox_chatwoot_route"


@pytest.mark.parametrize(
    "candidate",
    [
        _prior_inbound_event(dedupe_key="chatwoot:555:456"),
        _prior_inbound_event(dedupe_key="wa:wrong-phone-prior", from_phone="10000000001"),
        _prior_inbound_event(
            dedupe_key="wa:incomplete-prior",
            chatwoot_message_id=None,
        ),
    ],
    ids=["chatwoot-origin", "wrong-phone", "incomplete"],
)
@pytest.mark.asyncio
async def test_unproven_prior_reaction_candidate_uses_separate_general(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    candidate: WhatsAppEvent,
) -> None:
    evt, mock_cls, cw = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=BRANCH_MAP,
        targets=[],
        dedupe_key=f"wa:unproven-prior-reaction:{candidate.dedupe_key}",
        prior_inbound_events=[candidate],
    )

    mock_cls.assert_called_once_with(inbox_id=999)
    cw.get_or_create_incoming_conversation.assert_called_once()
    assert cw.send_message.call_args.args[0] == DEST_CONVERSATION_ID
    assert cw.send_message.call_args.kwargs["content_attributes"]["whatsapp_reaction_target_kind"] == "unknown"
    assert evt.error is None


@pytest.mark.asyncio
async def test_conflicting_prior_inbound_reaction_fails_before_chatwoot_client(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_wamid = "wamid.CONFLICTING.PRIOR.REACT"
    evt, mock_cls, cw = await _run_tenant_reaction(
        session_maker,
        monkeypatch,
        raw_map=BRANCH_MAP,
        targets=[],
        target_wamid=target_wamid,
        dedupe_key="wa:conflicting-prior-reaction",
        prior_inbound_events=[
            _prior_inbound_event(
                whatsapp_message_id=target_wamid,
                chatwoot_message_id=901,
                forwarded_chatwoot_conversation_id=2301,
                dedupe_key="wa:conflicting-reaction:1",
            ),
            _prior_inbound_event(
                whatsapp_message_id=target_wamid,
                chatwoot_message_id=902,
                forwarded_chatwoot_conversation_id=2302,
                dedupe_key="wa:conflicting-reaction:2",
            ),
        ],
        expected_error="ambiguous_prior_inbound_conversation",
    )

    mock_cls.assert_not_called()
    cw.send_message.assert_not_called()
    assert evt.error == "chatwoot tenant routing failed: ambiguous_prior_inbound_conversation"


# ---------------------------------------------------------------------------
# 2. native reply → agent (operator) message
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaction_forwarded_to_chatwoot_with_native_reply_to_agent_message(session_maker) -> None:
    evt, cw = await _run_reaction(
        session_maker,
        payload=_reaction_payload(),
        seeds=lambda s: s.add(
            _outbox(
                message_source="operator",
                template_code="operator_relay",
                chatwoot_message_id=123,
                chatwoot_conversation_id=DEST_CONVERSATION_ID,
            )
        ),
    )

    cw.send_message.assert_called_once()
    call = cw.send_message.call_args
    assert call.args[0] == DEST_CONVERSATION_ID
    assert call.args[1] == "👍"
    assert call.kwargs["message_type"] == "incoming"
    attrs = call.kwargs["content_attributes"]
    assert attrs["in_reply_to"] == 123
    assert attrs["in_reply_to_external_id"] == TARGET_WAMID
    assert attrs["whatsapp_event_type"] == "reaction"
    assert attrs["whatsapp_reaction_target_kind"] == "chatwoot_agent_message"

    assert evt.chatwoot_message_id == MESSAGE_ID
    assert evt.forwarded_chatwoot_conversation_id == DEST_CONVERSATION_ID
    assert evt.whatsapp_message_id == REACTION_WAMID
    assert evt.error is None


# ---------------------------------------------------------------------------
# 3. native reply → prior inbound WhatsAppEvent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaction_forwarded_to_chatwoot_with_native_reply_to_prior_inbound_message(session_maker) -> None:
    evt, cw = await _run_reaction(
        session_maker,
        payload=_reaction_payload(),
        seeds=lambda s: s.add(
            _prior_inbound_event(chatwoot_message_id=456, forwarded_chatwoot_conversation_id=DEST_CONVERSATION_ID)
        ),
    )

    call = cw.send_message.call_args
    assert call.args[1] == "👍"
    attrs = call.kwargs["content_attributes"]
    assert attrs["in_reply_to"] == 456
    assert attrs["in_reply_to_external_id"] == TARGET_WAMID
    assert attrs["whatsapp_reaction_target_kind"] == "inbound_whatsapp_event"
    assert evt.chatwoot_message_id == MESSAGE_ID
    assert evt.forwarded_chatwoot_conversation_id == DEST_CONVERSATION_ID


# ---------------------------------------------------------------------------
# 4. outbox fallback (no chatwoot message id)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaction_fallback_when_target_is_outbox_message_without_chatwoot_message_id(session_maker) -> None:
    async def seeds(s: Any) -> None:
        # record_id is a FK to records — seed a real record (flush first so the
        # OutboxMessage insert sees it) with an explicit id.
        s.add(
            Record(
                id=4242,
                company_id=1,
                altegio_record_id=777,
                client_id=1,
                altegio_client_id=1,
                raw={},
            )
        )
        await s.flush()
        s.add(_outbox(template_code="reminder_24h", record_id=4242))

    evt, cw = await _run_reaction(session_maker, payload=_reaction_payload(), seeds=seeds)

    call = cw.send_message.call_args
    assert call.args[1] == "👍 Реакция на отправленное сообщение WhatsApp (reminder_24h)"
    attrs = call.kwargs["content_attributes"]
    assert "in_reply_to" not in attrs
    assert attrs["whatsapp_reaction_target_kind"] == "outbox_message"
    assert attrs["whatsapp_reaction_target_outbox_id"] is not None
    assert attrs["whatsapp_reaction_target_template_code"] == "reminder_24h"
    assert attrs["whatsapp_reaction_target_record_id"] == 4242
    assert attrs["whatsapp_reaction_target_provider_message_id"] == TARGET_WAMID
    assert evt.error is None


# ---------------------------------------------------------------------------
# 5. outbox lookup is phone-scoped (wrong phone ignored)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaction_outbox_target_is_phone_scoped(session_maker) -> None:
    evt, cw = await _run_reaction(
        session_maker,
        payload=_reaction_payload(from_phone=FROM_PHONE),
        # Same provider_message_id but a DIFFERENT phone — must be ignored.
        seeds=lambda s: s.add(_outbox(phone_e164="+10000000001", template_code="record_created")),
    )

    call = cw.send_message.call_args
    assert call.args[1] == "👍 Реакция на сообщение в WhatsApp"
    attrs = call.kwargs["content_attributes"]
    assert attrs["whatsapp_reaction_target_kind"] == "unknown"
    assert "whatsapp_reaction_target_outbox_id" not in attrs
    assert "in_reply_to" not in attrs


# ---------------------------------------------------------------------------
# 6. phone variant (with/without "+") still matches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaction_outbox_target_matches_phone_variant(session_maker) -> None:
    # Reaction from "+381638400431"; outbox stored without the "+".
    evt, cw = await _run_reaction(
        session_maker,
        payload=_reaction_payload(),
        seeds=lambda s: s.add(_outbox(phone_e164="381638400431", template_code="reminder_24h")),
    )

    call = cw.send_message.call_args
    attrs = call.kwargs["content_attributes"]
    assert attrs["whatsapp_reaction_target_kind"] == "outbox_message"
    assert attrs["whatsapp_reaction_target_template_code"] == "reminder_24h"


# ---------------------------------------------------------------------------
# 7. duplicate provider_message_id → matching phone wins
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaction_outbox_target_prefers_matching_phone_over_wrong_phone_duplicate(session_maker) -> None:
    def seeds(s: Any) -> None:
        s.add(_outbox(phone_e164="+10000000001", template_code="record_created"))
        s.add(_outbox(phone_e164=PHONE_E164, template_code="reminder_24h"))

    evt, cw = await _run_reaction(session_maker, payload=_reaction_payload(), seeds=seeds)

    call = cw.send_message.call_args
    attrs = call.kwargs["content_attributes"]
    assert attrs["whatsapp_reaction_target_kind"] == "outbox_message"
    assert attrs["whatsapp_reaction_target_template_code"] == "reminder_24h"
    assert call.args[1] == "👍 Реакция на отправленное сообщение WhatsApp (reminder_24h)"


# ---------------------------------------------------------------------------
# 8. missing phone → extractor skips (no unsafe lookup possible)
# ---------------------------------------------------------------------------


def test_reaction_missing_phone_is_skipped_by_extractor() -> None:
    payload = _reaction_payload()
    # Drop the sender phone from the reaction message.
    payload["entry"][0]["changes"][0]["value"]["messages"][0].pop("from")
    assert _extract_actions(payload) == []


@pytest.mark.asyncio
async def test_resolve_reaction_target_missing_phone_returns_unknown(session_maker) -> None:
    # Defense-in-depth: even seeded with a matching provider_message_id, a None
    # phone must never resolve to an OutboxMessage.
    async with session_maker() as session:
        async with session.begin():
            session.add(_outbox(phone_e164=PHONE_E164, template_code="reminder_24h"))
            await session.flush()
            target = await _resolve_reaction_target(session, TARGET_WAMID, phone_e164=None)
    assert target == ReactionTarget(
        kind="unknown",
        provider_message_id=TARGET_WAMID,
        chatwoot_route=ChatwootRoute.GENERAL,
    )


# ---------------------------------------------------------------------------
# 9. unknown fallback when nothing matches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaction_fallback_when_target_message_not_found(session_maker) -> None:
    evt, cw = await _run_reaction(session_maker, payload=_reaction_payload())

    call = cw.send_message.call_args
    assert call.args[1] == "👍 Реакция на сообщение в WhatsApp"
    attrs = call.kwargs["content_attributes"]
    assert attrs["whatsapp_reaction_target_kind"] == "unknown"
    assert "in_reply_to" not in attrs
    assert evt.chatwoot_message_id == MESSAGE_ID


# ---------------------------------------------------------------------------
# 10. empty / missing emoji (reaction removal) must not crash
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaction_with_empty_emoji_does_not_crash(session_maker) -> None:
    evt, cw = await _run_reaction(session_maker, payload=_reaction_payload(emoji=None))

    call = cw.send_message.call_args
    assert call.args[1] == "Реакция удалена в WhatsApp"
    assert call.kwargs["message_type"] == "incoming"
    assert evt.chatwoot_message_id == MESSAGE_ID
    assert evt.error is None


# ---------------------------------------------------------------------------
# 11. reaction must not trigger commands / opt-out / promo / WhatsApp send
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaction_does_not_trigger_system_commands_or_llm(session_maker) -> None:
    provider = _CaptureProvider()
    safe_send_mock = AsyncMock(return_value=("wamid.NO", None))
    opt_out_mock = AsyncMock(return_value=0)
    promo_mock = AsyncMock(return_value=None)

    with (
        patch("altegio_bot.workers.whatsapp_inbox_worker.safe_send", safe_send_mock),
        patch("altegio_bot.workers.whatsapp_inbox_worker._set_opt_out", opt_out_mock),
        patch("altegio_bot.workers.whatsapp_inbox_worker.handle_promo_command", promo_mock),
    ):
        evt, cw = await _run_reaction(session_maker, payload=_reaction_payload(), provider=provider)

    cw.send_message.assert_called_once()
    assert provider.sent == []
    safe_send_mock.assert_not_called()
    opt_out_mock.assert_not_called()
    promo_mock.assert_not_called()
    assert evt.error is None


# ---------------------------------------------------------------------------
# 12. chatwoot-origin reaction → loop prevention (no mirror back)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaction_chatwoot_origin_skipped_loop_prevention(session_maker) -> None:
    evt, cw = await _run_reaction(
        session_maker,
        payload=_reaction_payload(),
        dedupe_key="chatwoot:99:1",
    )

    cw.send_message.assert_not_called()
    cw.get_or_create_incoming_conversation.assert_not_called()
    assert evt.error is None
    assert evt.forwarded_chatwoot_conversation_id is None


# ---------------------------------------------------------------------------
# 13. mixed payload: statuses[] + reaction → both processed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaction_status_mixed_payload_processes_statuses_and_reaction(session_maker) -> None:
    status_wamid = "wamid.STATUS_TARGET"

    def seeds(s: Any) -> None:
        s.add(
            _outbox(
                provider_message_id=status_wamid,
                phone_e164=PHONE_E164,
                template_code="reminder_24h",
            )
        )

    payload = _reaction_payload(
        extra_statuses=[
            {
                "id": status_wamid,
                "status": "delivered",
                "timestamp": "1700000001",
                "recipient_id": FROM_PHONE,
            }
        ]
    )

    evt, cw = await _run_reaction(session_maker, payload=payload, seeds=seeds)

    # Reaction was forwarded.
    cw.send_message.assert_called_once()
    assert cw.send_message.call_args.args[1] == "👍 Реакция на сообщение в WhatsApp"

    # Status was applied to the seeded outbox.
    async with session_maker() as session:
        ob = (
            await session.execute(select(OutboxMessage).where(OutboxMessage.provider_message_id == status_wamid))
        ).scalar_one()
        assert ob.status == "delivered"


# ---------------------------------------------------------------------------
# 14. native target in another conversation → fallback, mismatch flag
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaction_native_target_cross_conversation_falls_back_without_in_reply_to(session_maker) -> None:
    evt, cw = await _run_reaction(
        session_maker,
        payload=_reaction_payload(),
        seeds=lambda s: s.add(
            _outbox(
                message_source="operator",
                template_code="operator_relay",
                chatwoot_message_id=123,
                chatwoot_conversation_id=100,  # different from destination
            )
        ),
        destination_conversation_id=DEST_CONVERSATION_ID,
    )

    call = cw.send_message.call_args
    assert call.args[1] == "👍 Реакция на сообщение в WhatsApp"
    attrs = call.kwargs["content_attributes"]
    assert "in_reply_to" not in attrs
    assert attrs["whatsapp_reaction_target_conversation_mismatch"] is True
    assert attrs["whatsapp_reaction_target_kind"] == "chatwoot_agent_message"


# ---------------------------------------------------------------------------
# 15. native agent target requires message_source="operator" (Fix 1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaction_bot_outbox_with_chatwoot_ids_is_not_native_agent_target(session_maker) -> None:
    """A bot/automatic OutboxMessage that happens to carry Chatwoot ids must NOT
    become a native chatwoot_agent_message; it degrades to the outbox fallback."""
    evt, cw = await _run_reaction(
        session_maker,
        payload=_reaction_payload(),
        seeds=lambda s: s.add(
            _outbox(
                message_source="bot",
                template_code="reminder_24h",
                chatwoot_message_id=999,
                chatwoot_conversation_id=DEST_CONVERSATION_ID,
            )
        ),
    )

    call = cw.send_message.call_args
    assert call.args[1] == "👍 Реакция на отправленное сообщение WhatsApp (reminder_24h)"
    attrs = call.kwargs["content_attributes"]
    assert attrs["whatsapp_reaction_target_kind"] == "outbox_message"
    assert attrs["whatsapp_reaction_target_outbox_id"] is not None
    assert attrs["whatsapp_reaction_target_template_code"] == "reminder_24h"
    assert "in_reply_to" not in attrs


@pytest.mark.asyncio
async def test_resolve_reaction_target_bot_row_is_not_native(session_maker) -> None:
    """Resolver unit check: a bot row carrying Chatwoot ids resolves to
    outbox_message (never native) because step 1 requires message_source=operator.

    (The operator-positive path is covered by the integration tests above.)
    """
    async with session_maker() as session:
        async with session.begin():
            session.add(
                _outbox(
                    message_source="bot",
                    template_code="reminder_24h",
                    chatwoot_message_id=321,
                    chatwoot_conversation_id=DEST_CONVERSATION_ID,
                )
            )
            await session.flush()
            bot_target = await _resolve_reaction_target(session, TARGET_WAMID, phone_e164=PHONE_E164)
    assert bot_target.kind == "outbox_message"
    assert bot_target.chatwoot_message_id is None


# ---------------------------------------------------------------------------
# 16. prior inbound event must be Meta-origin (wa:%) (Fix 2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaction_prior_inbound_event_must_be_meta_origin(session_maker) -> None:
    """A WhatsAppEvent matching by wamid/phone/Chatwoot ids but with a
    chatwoot-origin dedupe_key must NOT resolve as inbound_whatsapp_event."""
    evt, cw = await _run_reaction(
        session_maker,
        payload=_reaction_payload(),
        seeds=lambda s: s.add(
            _prior_inbound_event(
                chatwoot_message_id=456,
                forwarded_chatwoot_conversation_id=DEST_CONVERSATION_ID,
                dedupe_key="chatwoot:99:1",
            )
        ),
    )

    call = cw.send_message.call_args
    assert call.args[1] == "👍 Реакция на сообщение в WhatsApp"
    attrs = call.kwargs["content_attributes"]
    assert attrs["whatsapp_reaction_target_kind"] == "unknown"
    assert "in_reply_to" not in attrs
