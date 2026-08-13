"""PR-7.2: the branch a phone number provably belongs to.

Two flows share this answer and must not diverge: a contextless customer
inbound (which inbox shows it) and an operator reply typed in General (which
sender may carry it back to Meta). The second had no answer at all before —
production event 20794 died as ``operator_relay: inbox_mapping_missing``
because General is deliberately absent from the branch map.

The rules under test are about refusing to guess. EasyWeek and Altegio share
one integer space for company ids, so identity is always the PAIR
(provider, company_id); "several answers" and "the data contradicts itself"
are distinct from "nothing known", and only the last may use General.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.chatwoot_affinity import (
    AffinityOutcome,
    resolve_tenant_affinity,
)
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    Client,
    MessageJob,
    OutboxMessage,
    Record,
    WhatsAppSender,
)
from altegio_bot.settings import Settings, settings

NOW = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
PHONE = "+491700000001"

DURLACH = (PROVIDER_EASYWEEK, 999501)
RASTATT = (PROVIDER_EASYWEEK, 999502)
KARLSRUHE = (PROVIDER_ALTEGIO, 758285)
# The collision that makes a bare numeric company id useless as identity.
ALTEGIO_TWIN = (PROVIDER_ALTEGIO, 999501)


async def _client(session: AsyncSession, client_id: int, tenant: tuple[str, int], *, phone: str = PHONE) -> Client:
    client = Client(
        id=client_id,
        provider=tenant[0],
        company_id=tenant[1],
        altegio_client_id=client_id,
        phone_e164=phone,
    )
    session.add(client)
    await session.commit()
    return client


async def _record(
    session: AsyncSession,
    record_id: int,
    tenant: tuple[str, int],
    *,
    client_id: int,
    starts_at: datetime,
    is_deleted: bool = False,
) -> Record:
    record = Record(
        id=record_id,
        provider=tenant[0],
        company_id=tenant[1],
        altegio_record_id=record_id,
        client_id=client_id,
        starts_at=starts_at,
        is_deleted=is_deleted,
    )
    session.add(record)
    await session.commit()
    return record


async def _delivered_bot_outbox(
    session: AsyncSession,
    outbox_id: int,
    tenant: tuple[str, int],
    *,
    sent_at: datetime,
    status: str = "delivered",
    with_job: bool = True,
    job_company: int | None = None,
    meta: dict | None = None,
) -> OutboxMessage:
    job_id = None
    if with_job:
        job = MessageJob(
            provider=tenant[0],
            company_id=job_company if job_company is not None else tenant[1],
            job_type="record_created",
            run_at=sent_at,
            status="done",
            dedupe_key=f"job:{outbox_id}",
        )
        session.add(job)
        await session.flush()
        job_id = int(job.id)

    row = OutboxMessage(
        id=outbox_id,
        company_id=tenant[1],
        job_id=job_id,
        phone_e164=PHONE,
        template_code="record_created",
        language="de",
        body="",
        status=status,
        scheduled_at=sent_at,
        sent_at=sent_at,
        message_source="bot",
        meta=meta or {},
    )
    session.add(row)
    await session.commit()
    return row


async def _operator_outbox(
    session: AsyncSession,
    outbox_id: int,
    tenant: tuple[str, int],
    *,
    sent_at: datetime,
    sender_company: int | None = None,
    outbox_company: int | None = None,
) -> OutboxMessage:
    sender = WhatsAppSender(
        provider=tenant[0],
        company_id=sender_company if sender_company is not None else tenant[1],
        sender_code="default",
        phone_number_id=f"pnid-{outbox_id}",
        is_active=True,
    )
    session.add(sender)
    await session.flush()

    row = OutboxMessage(
        id=outbox_id,
        company_id=outbox_company if outbox_company is not None else tenant[1],
        sender_id=int(sender.id),
        phone_e164=PHONE,
        template_code="operator",
        language="de",
        body="",
        status="delivered",
        scheduled_at=sent_at,
        sent_at=sent_at,
        message_source="operator",
    )
    session.add(row)
    await session.commit()
    return row


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def test_the_routing_mode_defaults_to_todays_behaviour() -> None:
    assert settings.chatwoot_inbound_routing_mode == "context"


@pytest.mark.parametrize("mode", ["context", "affinity", "general"])
def test_every_documented_mode_is_accepted(monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    monkeypatch.setenv("CHATWOOT_INBOUND_ROUTING_MODE", mode)

    assert Settings().chatwoot_inbound_routing_mode == mode


@pytest.mark.parametrize("bad", ["Affinity", "affinity ", "on", "1", "", "tenant"])
def test_an_invalid_mode_fails_fast(monkeypatch: pytest.MonkeyPatch, bad: str) -> None:
    """A silent fallback would route customers to a branch nobody chose."""
    monkeypatch.setenv("CHATWOOT_INBOUND_ROUTING_MODE", bad)

    with pytest.raises(Exception) as caught:
        Settings()
    assert "CHATWOOT_INBOUND_ROUTING_MODE" in str(caught.value)


# ---------------------------------------------------------------------------
# Evidence 1: the last proven communication
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "tenant"),
    [("durlach", DURLACH), ("rastatt", RASTATT), ("karlsruhe", KARLSRUHE)],
)
async def test_a_recent_delivered_notification_proves_its_branch(
    session_maker: async_sessionmaker[AsyncSession],
    label: str,
    tenant: tuple[str, int],
) -> None:
    async with session_maker() as session:
        await _delivered_bot_outbox(session, 1, tenant, sent_at=NOW - timedelta(hours=2))

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert result.outcome is AffinityOutcome.PROVEN, f"{label}: {result.reason}"
    assert (result.identity.provider, result.identity.company_id) == tenant
    assert result.source == "communication"


async def test_the_newer_communication_wins(session_maker: async_sessionmaker[AsyncSession]) -> None:
    async with session_maker() as session:
        await _delivered_bot_outbox(session, 1, DURLACH, sent_at=NOW - timedelta(days=3))
        await _delivered_bot_outbox(session, 2, RASTATT, sent_at=NOW - timedelta(hours=1))

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert (result.identity.provider, result.identity.company_id) == RASTATT


@pytest.mark.parametrize("status", ["sent", "queued", "failed", "unknown", "canceled"])
async def test_only_a_confirmed_delivery_proves_anything(
    session_maker: async_sessionmaker[AsyncSession],
    status: str,
) -> None:
    """`sent` means Meta accepted it — a `failed` callback can still follow."""
    async with session_maker() as session:
        await _delivered_bot_outbox(session, 1, DURLACH, sent_at=NOW - timedelta(hours=1), status=status)

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert result.outcome is AffinityOutcome.NO_EVIDENCE


async def test_a_jobless_general_ack_is_not_tenant_proof(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """STOP/START answers are routed to General on purpose."""
    async with session_maker() as session:
        await _delivered_bot_outbox(
            session,
            1,
            DURLACH,
            sent_at=NOW - timedelta(hours=1),
            with_job=False,
            meta={"chatwoot_route": "general"},
        )

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert result.outcome is AffinityOutcome.NO_EVIDENCE


async def test_a_bot_row_without_a_job_proves_nothing(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """`OutboxMessage.company_id` alone carries no provider."""
    async with session_maker() as session:
        await _delivered_bot_outbox(session, 1, DURLACH, sent_at=NOW - timedelta(hours=1), with_job=False)

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert result.outcome is AffinityOutcome.NO_EVIDENCE


async def test_an_operator_row_takes_identity_from_its_sender(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        await _operator_outbox(session, 1, RASTATT, sent_at=NOW - timedelta(hours=1))

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert result.outcome is AffinityOutcome.PROVEN
    assert (result.identity.provider, result.identity.company_id) == RASTATT


async def test_an_outbox_disagreeing_with_its_sender_is_invalid(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        await _operator_outbox(session, 1, DURLACH, sent_at=NOW, outbox_company=999599)

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert result.outcome is AffinityOutcome.INVALID
    assert result.reason == "operator_outbox_sender_company_mismatch"


async def test_an_outbox_disagreeing_with_its_job_is_invalid(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        await _delivered_bot_outbox(session, 1, DURLACH, sent_at=NOW, job_company=999599)

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert result.outcome is AffinityOutcome.INVALID
    assert result.reason == "outbox_job_company_mismatch"


async def test_the_same_company_number_on_two_providers_never_merges(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """999501 is a Durlach EasyWeek id AND, here, an Altegio one."""
    async with session_maker() as session:
        await _delivered_bot_outbox(session, 1, ALTEGIO_TWIN, sent_at=NOW - timedelta(hours=1))

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert result.identity.provider == PROVIDER_ALTEGIO
    assert (result.identity.provider, result.identity.company_id) != DURLACH


# ---------------------------------------------------------------------------
# Evidence 2 and 3: bookings, then the client itself
# ---------------------------------------------------------------------------


async def test_the_nearest_future_booking_decides(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        await _client(session, 9001, DURLACH)
        await _client(session, 9002, RASTATT)
        await _record(session, 9001, RASTATT, client_id=9002, starts_at=NOW + timedelta(days=9))
        await _record(session, 9002, DURLACH, client_id=9001, starts_at=NOW + timedelta(days=1))

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert (result.identity.provider, result.identity.company_id) == DURLACH
    assert result.source == "booking"


async def test_without_a_future_booking_the_latest_past_one_decides(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        await _client(session, 9001, DURLACH)
        await _client(session, 9002, RASTATT)
        await _record(session, 9001, DURLACH, client_id=9001, starts_at=NOW - timedelta(days=30))
        await _record(session, 9002, RASTATT, client_id=9002, starts_at=NOW - timedelta(days=2))

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert (result.identity.provider, result.identity.company_id) == RASTATT


async def test_two_branches_tied_at_the_top_are_ambiguous(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """A coin flip here answers a customer as the wrong salon."""
    same = NOW + timedelta(days=2)
    async with session_maker() as session:
        await _client(session, 9001, DURLACH)
        await _client(session, 9002, RASTATT)
        await _record(session, 9001, DURLACH, client_id=9001, starts_at=same)
        await _record(session, 9002, RASTATT, client_id=9002, starts_at=same)

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert result.outcome is AffinityOutcome.AMBIGUOUS
    assert result.reason == "conflicting_top_booking"


async def test_a_deleted_booking_does_not_decide(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        await _client(session, 9001, DURLACH)
        await _record(session, 9001, DURLACH, client_id=9001, starts_at=NOW + timedelta(days=1), is_deleted=True)

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    # Falls through to the single-client rule rather than using the dead row.
    assert result.source == "client"


async def test_a_record_outside_its_clients_tenant_is_invalid(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        await _client(session, 9001, DURLACH)
        await _record(session, 9001, RASTATT, client_id=9001, starts_at=NOW + timedelta(days=1))

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert result.outcome is AffinityOutcome.INVALID
    assert result.reason == "record_client_scope_mismatch"


async def test_a_single_client_without_bookings_proves_its_branch(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        await _client(session, 9001, KARLSRUHE)

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert result.outcome is AffinityOutcome.PROVEN
    assert (result.identity.provider, result.identity.company_id) == KARLSRUHE
    assert result.source == "client"


async def test_clients_in_two_branches_are_ambiguous(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        await _client(session, 9001, DURLACH)
        await _client(session, 9002, RASTATT)

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert result.outcome is AffinityOutcome.AMBIGUOUS
    assert result.reason == "multiple_client_tenants"


async def test_an_unknown_phone_has_no_evidence(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """The one outcome General is actually for."""
    async with session_maker() as session:
        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert result.outcome is AffinityOutcome.NO_EVIDENCE
    assert result.identity is None


async def test_communication_outranks_a_newer_booking_elsewhere(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        await _client(session, 9001, RASTATT)
        await _record(session, 9001, RASTATT, client_id=9001, starts_at=NOW + timedelta(days=1))
        await _delivered_bot_outbox(session, 1, DURLACH, sent_at=NOW - timedelta(hours=1))

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    assert (result.identity.provider, result.identity.company_id) == DURLACH
    assert result.source == "communication"


async def test_no_phone_variant_means_no_evidence(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        result = await resolve_tenant_affinity(session, [], now=NOW)

    assert result.outcome is AffinityOutcome.NO_EVIDENCE
    assert result.reason == "no_phone"


# ---------------------------------------------------------------------------
# Log hygiene
# ---------------------------------------------------------------------------


async def test_the_result_carries_no_customer_data(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        await _client(session, 9001, DURLACH)
        await _delivered_bot_outbox(session, 1, DURLACH, sent_at=NOW - timedelta(hours=1))

        result = await resolve_tenant_affinity(session, [PHONE], now=NOW)

    rendered = str(result.as_safe_dict())
    assert PHONE not in rendered
    assert "phone" not in rendered
    assert set(result.as_safe_dict()) == {"outcome", "provider", "company_id", "source", "reason"}


# ---------------------------------------------------------------------------
# The production scenario: an operator reply typed in General
# ---------------------------------------------------------------------------
#
# 2026-08-13, conversation 230, message 9343, source inbox 8, branch map
# {9,10,11}. The reply terminated as `operator_relay: inbox_mapping_missing`,
# no Outbox was created and Meta was never called. Event 20794 is terminal and
# is NOT replayed — these tests drive NEW events through the same code path.

from typing import Any  # noqa: E402

from altegio_bot.models.models import WhatsAppEvent  # noqa: E402
from altegio_bot.workers import whatsapp_inbox_worker as wiw  # noqa: E402

GENERAL_INBOX = 8
BRANCH_MAP = (
    '{"9":{"provider":"easyweek","company_id":999501},'
    '"10":{"provider":"easyweek","company_id":999502},'
    '"11":{"provider":"altegio","company_id":758285}}'
)


def _relay_settings(monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(wiw.settings, "chatwoot_inbox_company_map", BRANCH_MAP)
    monkeypatch.setattr(wiw.settings, "chatwoot_inbox_id", GENERAL_INBOX)
    monkeypatch.setattr(wiw.settings, "chatwoot_inbound_routing_mode", mode)


async def _general_relay_hint(
    session: AsyncSession,
    *,
    inbox_id: object = GENERAL_INBOX,
    phone: str = PHONE,
) -> tuple[object, str | None]:
    return await wiw._general_relay_tenant_hint(session, chatwoot_inbox_id=inbox_id, phone_e164=phone)


@pytest.mark.parametrize("mode", ["affinity", "general"])
async def test_a_general_reply_with_proven_affinity_resolves_its_branch(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """The exact failure of event 20794, now resolved rather than dropped."""
    _relay_settings(monkeypatch, mode)

    async with session_maker() as session:
        await _delivered_bot_outbox(session, 1, DURLACH, sent_at=NOW - timedelta(hours=2))

        identity, error = await _general_relay_hint(session)

    assert error is None, f"{mode}: {error}"
    assert (identity.provider, identity.company_id) == DURLACH


async def test_context_mode_keeps_a_general_reply_blocked(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`context` is the rollout default and must not change behaviour."""
    _relay_settings(monkeypatch, "context")

    async with session_maker() as session:
        await _delivered_bot_outbox(session, 1, DURLACH, sent_at=NOW - timedelta(hours=2))

        identity, error = await _general_relay_hint(session)

    assert identity is None
    assert error == "operator_relay: inbox_mapping_missing"


@pytest.mark.parametrize("mode", ["affinity", "general"])
@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("no_evidence", "operator_relay: general_affinity_no_evidence"),
        ("ambiguous", "operator_relay: general_affinity_ambiguous"),
        ("invalid", "operator_relay: general_affinity_invalid"),
    ],
)
async def test_an_unprovable_general_reply_never_picks_a_sender(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    label: str,
    expected: str,
) -> None:
    """Guessing here answers a customer as the wrong salon."""
    _relay_settings(monkeypatch, mode)

    async with session_maker() as session:
        if label == "ambiguous":
            await _client(session, 9001, DURLACH)
            await _client(session, 9002, RASTATT)
        elif label == "invalid":
            await _operator_outbox(session, 1, DURLACH, sent_at=NOW, outbox_company=999599)

        identity, error = await _general_relay_hint(session)

    assert identity is None
    assert error == expected


@pytest.mark.parametrize("mode", ["affinity", "general", "context"])
async def test_an_arbitrary_unmapped_inbox_keeps_its_existing_outcome(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """Inbox 99 is not General: it must not reach the resolver at all."""
    _relay_settings(monkeypatch, mode)

    async with session_maker() as session:
        await _delivered_bot_outbox(session, 1, DURLACH, sent_at=NOW - timedelta(hours=2))

        identity, error = await _general_relay_hint(session, inbox_id=99)

    assert identity is None
    assert error == "operator_relay: inbox_mapping_missing"


async def test_a_general_that_overlaps_a_branch_fails_closed(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _relay_settings(monkeypatch, "affinity")
    monkeypatch.setattr(wiw.settings, "chatwoot_inbox_id", 9)  # a branch inbox

    async with session_maker() as session:
        await _delivered_bot_outbox(session, 1, DURLACH, sent_at=NOW - timedelta(hours=2))

        identity, error = await _general_relay_hint(session, inbox_id=9)

    assert identity is None
    assert error == "operator_relay: general_inbox_overlaps_branch"


async def test_an_invalid_branch_map_fails_closed(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _relay_settings(monkeypatch, "affinity")
    monkeypatch.setattr(wiw.settings, "chatwoot_inbox_company_map", "{not json")

    async with session_maker() as session:
        identity, error = await _general_relay_hint(session)

    assert identity is None
    assert error == "operator_relay: invalid_inbox_company_map"


async def test_a_legacy_integer_only_map_fails_closed(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A provider-less map cannot separate EasyWeek from Altegio."""
    _relay_settings(monkeypatch, "affinity")
    monkeypatch.setattr(wiw.settings, "chatwoot_inbox_company_map", '{"9": 999501}')

    async with session_maker() as session:
        identity, error = await _general_relay_hint(session)

    assert identity is None
    assert error == "operator_relay: provider_scope_missing"


async def test_the_terminal_production_event_is_not_replayed(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A processed event stays processed: the fix applies to NEW messages."""
    monkeypatch.setattr(wiw, "SessionLocal", session_maker)
    _relay_settings(monkeypatch, "affinity")

    async with session_maker() as session:
        async with session.begin():
            payload: dict[str, Any] = {
                "_chatwoot_operator_relay": {
                    "recipient_phone": PHONE,
                    "text": "already handled",
                    "conversation_id": 230,
                    "message_id": 9343,
                    "phone_number_id": "PNID_GENERAL",
                    "chatwoot_inbox_id": GENERAL_INBOX,
                },
            }
            event = WhatsAppEvent(
                dedupe_key="chatwoot_out:230:9343",
                status="processed",
                error="operator_relay: inbox_mapping_missing",
                query={},
                headers={},
                payload=payload,
                chatwoot_conversation_id=230,
            )
            session.add(event)
            await session.flush()
            event_id = int(event.id)

    async with session_maker() as session:
        reloaded = await session.get(WhatsAppEvent, event_id)

    assert reloaded.status == "processed", "a terminal event must not be reopened"
    assert reloaded.error == "operator_relay: inbox_mapping_missing"


async def test_the_relay_hint_logs_no_customer_data(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _relay_settings(monkeypatch, "affinity")

    async with session_maker() as session:
        await _delivered_bot_outbox(session, 1, DURLACH, sent_at=NOW - timedelta(hours=2))

        with caplog.at_level("INFO"):
            await _general_relay_hint(session)

    assert PHONE not in caplog.text
    assert BRANCH_MAP not in caplog.text
