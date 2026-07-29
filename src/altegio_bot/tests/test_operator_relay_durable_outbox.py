"""Durable operator-relay Outbox lifecycle.

Proves the P1 guarantee: a committed, DB-unique send intent exists BEFORE the
first Meta side effect, the provider call happens outside any DB transaction,
and neither sequential/concurrent replay nor crash recovery can send twice.

Everything drives the production entry point ``process_one_event`` with the
module-global ``SessionLocal`` pointed at the test session factory (which shares
the test database), so the staged transactions commit for real between stages.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import func, select

import altegio_bot.workers.whatsapp_inbox_worker as wiw
from altegio_bot.models.models import OutboxMessage, WhatsAppEvent, WhatsAppSender

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PHONE = "+49111222333"
PNID = "PNID_DURABLE"


def _relay_payload(
    *,
    recipient_phone: str = PHONE,
    text: str = "Hallo vom Operator",
    conversation_id: int = 700,
    message_id: int = 800,
    phone_number_id: str = PNID,
    agent_name: str = "Anna",
) -> dict[str, Any]:
    return {
        "_chatwoot_operator_relay": {
            "recipient_phone": recipient_phone,
            "text": text,
            "conversation_id": conversation_id,
            "message_id": message_id,
            "phone_number_id": phone_number_id,
            "agent_name": agent_name,
        }
    }


async def _make_sender(session_maker, *, sender_id: int, company_id: int = 1, phone_number_id: str = PNID) -> None:
    async with session_maker() as s:
        async with s.begin():
            s.add(
                WhatsAppSender(
                    id=sender_id,
                    company_id=company_id,
                    sender_code="default",
                    phone_number_id=phone_number_id,
                    display_phone="+49000000000",
                    is_active=True,
                )
            )


async def _open_window(session_maker, *, phone: str = PHONE, dedupe_key: str = "win:durable") -> None:
    now = datetime.now(timezone.utc)
    async with session_maker() as s:
        async with s.begin():
            s.add(
                WhatsAppEvent(
                    dedupe_key=dedupe_key,
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
                                                {"from": phone, "type": "text", "text": {"body": "x"}, "id": "wamid.w"}
                                            ],
                                            "metadata": {"phone_number_id": "PNID_WIN"},
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                )
            )


async def _insert_relay_event(session_maker, *, dedupe_key: str, payload: dict[str, Any], conversation_id: int) -> int:
    async with session_maker() as s:
        async with s.begin():
            evt = WhatsAppEvent(
                dedupe_key=dedupe_key,
                status="received",
                query={},
                headers={},
                payload=payload,
                chatwoot_conversation_id=conversation_id,
            )
            s.add(evt)
            await s.flush()
            return int(evt.id)


async def _reload_event(session_maker, event_id: int) -> WhatsAppEvent:
    async with session_maker() as s:
        return await s.get(WhatsAppEvent, event_id)


async def _outboxes_for(session_maker, event_id: int) -> list[OutboxMessage]:
    async with session_maker() as s:
        rows = (
            await s.execute(
                select(OutboxMessage)
                .where(OutboxMessage.source_whatsapp_event_id == event_id)
                .order_by(OutboxMessage.id.asc())
            )
        ).scalars()
        return list(rows)


def _enable_relay(monkeypatch, session_maker, *, mode: str = "private_note_only") -> None:
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(wiw, "SessionLocal", session_maker)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_closed_window_mode", mode)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_reopen_private_note_enabled", False)


class _RecordingProvider:
    """Provider double recording every send/send_template call."""

    _supports_mirror_kwargs = False

    def __init__(self, wamid: str = "wamid.DURABLE") -> None:
        self.wamid = wamid
        self.sent: list[dict[str, Any]] = []
        self.templates: list[dict[str, Any]] = []

    async def send(self, sender_id: int, phone_e164: str, text: str, **kwargs: Any) -> str:
        self.sent.append({"sender_id": sender_id, "phone": phone_e164, "text": text})
        return self.wamid

    async def send_template(
        self, sender_id: int, phone_e164: str, template_name: str, language: str, params: list[str], **kwargs: Any
    ) -> str:
        self.templates.append(
            {"sender_id": sender_id, "phone": phone_e164, "template": template_name, "params": params}
        )
        return self.wamid


class _PermanentErrorProvider:
    _supports_mirror_kwargs = False

    async def send(self, *a: Any, **k: Any) -> str:
        raise RuntimeError("Recipient phone number not in allowed list")  # deterministic, non-transient

    async def send_template(self, *a: Any, **k: Any) -> str:
        raise RuntimeError("Recipient phone number not in allowed list")


# ===========================================================================
# §20 committed-before-send: provider sees the durable row in a SEPARATE session
# ===========================================================================


class _DurableCheckProvider:
    """During the provider call, open an INDEPENDENT session and assert that the
    Outbox row for the event is already committed and in 'sending'."""

    _supports_mirror_kwargs = False

    def __init__(self, session_maker, event_id: int, *, wamid: str = "wamid.CHECK") -> None:
        self._session_maker = session_maker
        self._event_id = event_id
        self.wamid = wamid
        self.observed: dict[str, Any] | None = None
        self.sent: list[dict[str, Any]] = []
        self.templates: list[dict[str, Any]] = []

    async def _observe(self) -> None:
        async with self._session_maker() as s:
            row = (
                await s.execute(select(OutboxMessage).where(OutboxMessage.source_whatsapp_event_id == self._event_id))
            ).scalar_one_or_none()
            assert row is not None, "durable Outbox row must be committed and visible before the provider call"
            self.observed = {
                "status": row.status,
                "sender_id": row.sender_id,
                "company_id": row.company_id,
                "phone_e164": row.phone_e164,
                "body": row.body,
                "attempt_started_at": row.attempt_started_at,
                "template_code": row.template_code,
            }

    async def send(self, sender_id: int, phone_e164: str, text: str, **kwargs: Any) -> str:
        await self._observe()
        self.sent.append({"sender_id": sender_id, "phone": phone_e164, "text": text})
        return self.wamid

    async def send_template(
        self, sender_id: int, phone_e164: str, template_name: str, language: str, params: list[str], **kwargs: Any
    ) -> str:
        await self._observe()
        self.templates.append({"template": template_name, "params": params})
        return self.wamid


@pytest.mark.asyncio
async def test_text_outbox_committed_before_send(session_maker, monkeypatch) -> None:
    _enable_relay(monkeypatch, session_maker)
    await _make_sender(session_maker, sender_id=10)
    await _open_window(session_maker)
    event_id = await _insert_relay_event(
        session_maker, dedupe_key="dur:text:committed", payload=_relay_payload(), conversation_id=700
    )
    provider = _DurableCheckProvider(session_maker, event_id, wamid="wamid.TXT")

    await wiw.process_one_event(event_id, provider)

    assert provider.observed is not None
    assert provider.observed["status"] == "sending"  # committed BEFORE the send
    assert provider.observed["attempt_started_at"] is not None
    assert provider.observed["phone_e164"] == PHONE
    assert provider.observed["body"] == "Hallo vom Operator"
    assert len(provider.sent) == 1

    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1
    assert rows[0].status == "sent"
    assert rows[0].provider_message_id == "wamid.TXT"
    assert rows[0].sent_at is not None
    event = await _reload_event(session_maker, event_id)
    assert event.status == "processed"
    assert event.error is None


@pytest.mark.asyncio
async def test_template_outbox_committed_before_send(session_maker, monkeypatch) -> None:
    _enable_relay(monkeypatch, session_maker, mode="reopen_template")
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_reopen_template_name", "reopen_hello")
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_reopen_template_language", "de")
    await _make_sender(session_maker, sender_id=11)
    # No window event → window closed → reopen_template path.
    event_id = await _insert_relay_event(
        session_maker, dedupe_key="dur:tpl:committed", payload=_relay_payload(), conversation_id=701
    )
    provider = _DurableCheckProvider(session_maker, event_id, wamid="wamid.TPL")

    await wiw.process_one_event(event_id, provider)

    assert provider.observed is not None
    assert provider.observed["status"] == "sending"
    assert provider.observed["template_code"] == "operator_reopen_template"
    assert len(provider.templates) == 1

    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1
    assert rows[0].status == "sent"
    assert rows[0].provider_message_id == "wamid.TPL"


# ===========================================================================
# §21 idempotency: sequential and concurrent replay send exactly once
# ===========================================================================


@pytest.mark.asyncio
async def test_sequential_replay_sends_once(session_maker, monkeypatch) -> None:
    _enable_relay(monkeypatch, session_maker)
    await _make_sender(session_maker, sender_id=12)
    await _open_window(session_maker)
    event_id = await _insert_relay_event(
        session_maker, dedupe_key="dur:seq", payload=_relay_payload(), conversation_id=702
    )
    provider = _RecordingProvider()

    await wiw.process_one_event(event_id, provider)
    await wiw.process_one_event(event_id, provider)

    assert len(provider.sent) == 1
    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1
    assert rows[0].status == "sent"


@pytest.mark.asyncio
async def test_concurrent_replay_sends_once(session_maker, monkeypatch) -> None:
    _enable_relay(monkeypatch, session_maker)
    await _make_sender(session_maker, sender_id=13)
    await _open_window(session_maker)
    event_id = await _insert_relay_event(
        session_maker, dedupe_key="dur:conc", payload=_relay_payload(), conversation_id=703
    )
    provider = _RecordingProvider()

    await asyncio.gather(
        wiw.process_one_event(event_id, provider),
        wiw.process_one_event(event_id, provider),
    )

    assert len(provider.sent) == 1  # exactly one Meta call across both workers
    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1
    assert rows[0].status == "sent"


# ===========================================================================
# §22 crash windows
# ===========================================================================


@pytest.mark.asyncio
async def test_crash_after_prepare_before_claim_resumes(session_maker, monkeypatch) -> None:
    """A committed 'queued' row (crash before claim) is claimed and sent on the
    next run — exactly once."""
    _enable_relay(monkeypatch, session_maker)
    await _make_sender(session_maker, sender_id=14)
    await _open_window(session_maker)
    event_id = await _insert_relay_event(
        session_maker, dedupe_key="dur:crash:prepare", payload=_relay_payload(), conversation_id=704
    )
    provider = _RecordingProvider()

    prepared = await wiw._prepare_operator_relay(event_id, provider)
    assert prepared.outbox_id is not None
    assert len(provider.sent) == 0  # no send yet
    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1 and rows[0].status == "queued"

    # Full run resumes: claim + send + finalize.
    await wiw.process_one_event(event_id, provider)
    assert len(provider.sent) == 1
    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1 and rows[0].status == "sent"


@pytest.mark.asyncio
async def test_crash_after_claim_before_provider_recovers_unknown(session_maker, monkeypatch) -> None:
    """A committed 'sending' row (crash before/around the provider call) is
    recovered to 'unknown' — never auto-resent."""
    _enable_relay(monkeypatch, session_maker)
    await _make_sender(session_maker, sender_id=15)
    await _open_window(session_maker)
    event_id = await _insert_relay_event(
        session_maker, dedupe_key="dur:crash:claim", payload=_relay_payload(), conversation_id=705
    )
    provider = _RecordingProvider()

    prepared = await wiw._prepare_operator_relay(event_id, provider)
    claimed = (await wiw._claim_operator_relay(prepared.outbox_id, event_id)).claimed
    assert claimed is not None  # committed 'sending', provider not yet called
    assert len(provider.sent) == 0

    # Force staleness and recover.
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_stale_sending_seconds", 0)
    recovered = await wiw.recover_stale_operator_relay_sending()
    assert recovered == 1
    assert len(provider.sent) == 0  # recovery never sends

    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1
    assert rows[0].status == "unknown"
    assert rows[0].meta.get("manual_review_required") is True
    assert rows[0].meta.get("recovery_reason") == "stale_sending_attempt"
    event = await _reload_event(session_maker, event_id)
    assert event.error == "operator_relay: delivery outcome unknown"

    # A later normal run must not resend a 'sending'/'unknown' row.
    await wiw.process_one_event(event_id, provider)
    assert len(provider.sent) == 0


@pytest.mark.asyncio
async def test_provider_success_then_finalize_failure_keeps_sending(session_maker, monkeypatch) -> None:
    """Meta accepted (wamid) but finalize crashed: the durable 'sending' row
    survives, no second send happens, and recovery marks it 'unknown'."""
    _enable_relay(monkeypatch, session_maker)
    await _make_sender(session_maker, sender_id=16)
    await _open_window(session_maker)
    event_id = await _insert_relay_event(
        session_maker, dedupe_key="dur:finalize:fail", payload=_relay_payload(), conversation_id=706
    )
    provider = _RecordingProvider(wamid="wamid.FINFAIL")

    prepared = await wiw._prepare_operator_relay(event_id, provider)
    claimed = (await wiw._claim_operator_relay(prepared.outbox_id, event_id)).claimed
    outcome = await wiw._execute_operator_relay(claimed, provider)
    assert outcome.kind == "sent" and outcome.provider_message_id == "wamid.FINFAIL"
    assert len(provider.sent) == 1

    # Finalize blows up (simulated DB failure): a separate session must still see
    # the durable 'sending' row.
    async def _boom(*a: Any, **k: Any) -> bool:
        raise RuntimeError("finalize db failure")

    monkeypatch.setattr(wiw, "_finalize_operator_relay", _boom)
    with pytest.raises(RuntimeError):
        await wiw._finalize_operator_relay(claimed, outcome)

    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1 and rows[0].status == "sending"

    # Replay must not call the provider again (row not 'queued').
    monkeypatch.undo()
    _enable_relay(monkeypatch, session_maker)
    await wiw.process_one_event(event_id, provider)
    assert len(provider.sent) == 1  # unchanged

    # Recovery converts the ambiguous attempt to unknown.
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_stale_sending_seconds", 0)
    assert await wiw.recover_stale_operator_relay_sending() == 1
    rows = await _outboxes_for(session_maker, event_id)
    assert rows[0].status == "unknown"
    assert len(provider.sent) == 1


# ===========================================================================
# §23 known outcomes
# ===========================================================================


@pytest.mark.asyncio
async def test_permanent_failure_marks_failed_no_retry(session_maker, monkeypatch) -> None:
    _enable_relay(monkeypatch, session_maker)
    monkeypatch.setattr(wiw.settings, "meta_circuit_breaker_enabled", False)
    await _make_sender(session_maker, sender_id=17)
    await _open_window(session_maker)
    event_id = await _insert_relay_event(
        session_maker, dedupe_key="dur:perm", payload=_relay_payload(), conversation_id=707
    )
    provider = _PermanentErrorProvider()

    await wiw.process_one_event(event_id, provider)

    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1
    assert rows[0].status == "failed"
    assert rows[0].error == "operator_relay: send failed (permanent)"
    event = await _reload_event(session_maker, event_id)
    assert event.error == "operator_relay: send failed (permanent)"

    # Second run: no new provider call, no new row.
    await wiw.process_one_event(event_id, provider)
    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1 and rows[0].status == "failed"


@pytest.mark.asyncio
async def test_private_note_only_window_closed_cancels_no_send(session_maker, monkeypatch) -> None:
    _enable_relay(monkeypatch, session_maker, mode="private_note_only")
    await _make_sender(session_maker, sender_id=18)
    # No window event → closed → private_note_only → canceled audit, no Meta call.
    event_id = await _insert_relay_event(
        session_maker, dedupe_key="dur:pno", payload=_relay_payload(), conversation_id=708
    )
    provider = _RecordingProvider()

    await wiw.process_one_event(event_id, provider)

    assert len(provider.sent) == 0
    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1
    assert rows[0].status == "canceled"
    event = await _reload_event(session_maker, event_id)
    assert event.status == "processed"

    # Replay creates no second canceled row.
    await wiw.process_one_event(event_id, provider)
    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1
    assert len(provider.sent) == 0


@pytest.mark.asyncio
async def test_circuit_closed_pre_send_cancels_no_send(session_maker, monkeypatch) -> None:
    _enable_relay(monkeypatch, session_maker)
    monkeypatch.setattr(wiw.settings, "meta_circuit_breaker_enabled", True)

    async def _paused(**kwargs: Any) -> bool:
        return True

    monkeypatch.setattr(wiw.meta_circuit, "should_pause_meta_sends", _paused)
    await _make_sender(session_maker, sender_id=19)
    await _open_window(session_maker)
    event_id = await _insert_relay_event(
        session_maker, dedupe_key="dur:circuit", payload=_relay_payload(), conversation_id=709
    )
    provider = _RecordingProvider()

    await wiw.process_one_event(event_id, provider)

    assert len(provider.sent) == 0
    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1
    assert rows[0].status == "canceled"
    assert rows[0].meta.get("cancel_reason") == "meta_circuit_closed"


# ===========================================================================
# §24 delivery-status compatibility
# ===========================================================================


@pytest.mark.asyncio
async def test_sent_row_still_matches_delivery_and_read(session_maker, monkeypatch) -> None:
    _enable_relay(monkeypatch, session_maker)
    await _make_sender(session_maker, sender_id=20)
    await _open_window(session_maker)
    event_id = await _insert_relay_event(
        session_maker, dedupe_key="dur:delivery", payload=_relay_payload(), conversation_id=710
    )
    provider = _RecordingProvider(wamid="wamid.DELIV")
    await wiw.process_one_event(event_id, provider)

    # The sent row is discoverable by provider_message_id (how status webhooks
    # match), and an 'unknown'/'sending' row without a wamid is never matched.
    async with session_maker() as s:
        matched = (
            await s.execute(select(OutboxMessage).where(OutboxMessage.provider_message_id == "wamid.DELIV"))
        ).scalar_one_or_none()
        assert matched is not None
        assert matched.status == "sent"
        assert matched.source_whatsapp_event_id == event_id

        # No stray operator rows with a NULL provider_message_id for this event.
        null_pid = (
            await s.execute(
                select(func.count())
                .select_from(OutboxMessage)
                .where(
                    OutboxMessage.source_whatsapp_event_id == event_id,
                    OutboxMessage.provider_message_id.is_(None),
                )
            )
        ).scalar_one()
        assert null_pid == 0
