"""Production polling-loop recovery for operator relay.

These tests drive the REAL production scheduling surface — ``lock_next_batch``,
``run_poll_cycle`` and the recovery functions — not just ``process_one_event`` in
isolation, and prove that a worker crash/restart never strands an operator event
or Outbox and never causes a second automatic provider call.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import func, select

import altegio_bot.workers.whatsapp_inbox_worker as wiw
from altegio_bot.models.models import OutboxMessage, WhatsAppEvent, WhatsAppSender

PHONE = "+49111222333"
PNID = "PNID_RECOVERY"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _relay_payload(*, conversation_id: int, text: str = "Hallo") -> dict[str, Any]:
    return {
        "_chatwoot_operator_relay": {
            "recipient_phone": PHONE,
            "text": text,
            "conversation_id": conversation_id,
            "message_id": conversation_id + 1,
            "phone_number_id": PNID,
            "agent_name": "Anna",
        }
    }


class _RecordingProvider:
    _supports_mirror_kwargs = False

    def __init__(self, wamid: str = "wamid.RECOVERY") -> None:
        self.wamid = wamid
        self.sent: list[dict[str, Any]] = []
        self.templates: list[dict[str, Any]] = []

    async def send(self, sender_id: int, phone_e164: str, text: str, **kwargs: Any) -> str:
        self.sent.append({"sender_id": sender_id, "phone": phone_e164, "text": text})
        return self.wamid

    async def send_template(self, *a: Any, **k: Any) -> str:
        self.templates.append({"a": a})
        return self.wamid


def _enable(monkeypatch, session_maker) -> None:
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(wiw, "SessionLocal", session_maker)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_reopen_private_note_enabled", False)


async def _make_sender(session_maker, *, sender_id: int) -> None:
    async with session_maker() as s:
        async with s.begin():
            s.add(
                WhatsAppSender(
                    id=sender_id,
                    company_id=1,
                    sender_code="default",
                    phone_number_id=PNID,
                    display_phone="+49000000000",
                    is_active=True,
                )
            )


async def _open_window(session_maker, *, dedupe_key: str) -> None:
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
                                                {"from": PHONE, "type": "text", "text": {"body": "x"}, "id": "wamid.w"}
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


async def _insert_received(session_maker, *, dedupe_key: str, conversation_id: int, age_seconds: float = 0.0) -> int:
    now = datetime.now(timezone.utc)
    async with session_maker() as s:
        async with s.begin():
            evt = WhatsAppEvent(
                dedupe_key=dedupe_key,
                received_at=now - timedelta(seconds=age_seconds),
                status="received",
                query={},
                headers={},
                payload=_relay_payload(conversation_id=conversation_id),
                chatwoot_conversation_id=conversation_id,
            )
            s.add(evt)
            await s.flush()
            return int(evt.id)


async def _lock_batch(session_maker, batch_size: int = 50) -> list[int]:
    """Simulate the production batch step: received → processing (committed)."""
    async with session_maker() as s:
        async with s.begin():
            events = await wiw.lock_next_batch(s, batch_size)
            return [int(e.id) for e in events]


async def _reload_event(session_maker, event_id: int) -> WhatsAppEvent:
    async with session_maker() as s:
        return await s.get(WhatsAppEvent, event_id)


async def _outboxes_for(session_maker, event_id: int) -> list[OutboxMessage]:
    async with session_maker() as s:
        return list(
            (await s.execute(select(OutboxMessage).where(OutboxMessage.source_whatsapp_event_id == event_id))).scalars()
        )


# ===========================================================================
# Test A — crash before prepare: stale processing without Outbox → received
# ===========================================================================


@pytest.mark.asyncio
async def test_crash_before_prepare_stale_processing_recovers(session_maker, monkeypatch) -> None:
    _enable(monkeypatch, session_maker)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_stale_processing_seconds", 300)
    await _make_sender(session_maker, sender_id=30)
    await _open_window(session_maker, dedupe_key="win:recA")
    # Aged so it is already past the stale-processing threshold once locked.
    event_id = await _insert_received(
        session_maker, dedupe_key="chatwoot_out:recA", conversation_id=800, age_seconds=600
    )
    provider = _RecordingProvider()

    # Production batch marks it processing; then the worker crashes before prepare.
    assert await _lock_batch(session_maker) == [event_id]
    assert (await _reload_event(session_maker, event_id)).status == "processing"
    assert await _outboxes_for(session_maker, event_id) == []  # no durable intent yet

    # Recovery returns it to 'received' (no Outbox → provider never started).
    recovered = await wiw.recover_stale_processing_events()
    assert recovered == 1
    assert (await _reload_event(session_maker, event_id)).status == "received"

    # The next poll cycle prepares + sends exactly once.
    stats = await wiw.run_poll_cycle(provider, batch_size=50, run_recovery=False)
    assert stats.processed == 1 and stats.failed == 0
    assert len(provider.sent) == 1
    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1 and rows[0].status == "sent"
    assert (await _reload_event(session_maker, event_id)).status == "processed"


@pytest.mark.asyncio
async def test_processing_with_queued_outbox_is_not_reset(session_maker, monkeypatch) -> None:
    """Stale-processing recovery must NEVER touch an event that already has an
    Outbox (queued/sending/terminal)."""
    _enable(monkeypatch, session_maker)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_stale_processing_seconds", 0)
    await _make_sender(session_maker, sender_id=31)
    await _open_window(session_maker, dedupe_key="win:recA2")
    event_id = await _insert_received(
        session_maker, dedupe_key="chatwoot_out:recA2", conversation_id=810, age_seconds=600
    )

    await _lock_batch(session_maker)
    await wiw._prepare_operator_relay(event_id, _RecordingProvider())  # commits queued Outbox

    recovered = await wiw.recover_stale_processing_events()
    assert recovered == 0  # has an Outbox → skipped
    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1 and rows[0].status == "queued"


# ===========================================================================
# Test B — crash after prepare: committed queued Outbox is resumed
# ===========================================================================


@pytest.mark.asyncio
async def test_crash_after_prepare_queued_resumed(session_maker, monkeypatch) -> None:
    _enable(monkeypatch, session_maker)
    await _make_sender(session_maker, sender_id=32)
    await _open_window(session_maker, dedupe_key="win:recB")
    event_id = await _insert_received(session_maker, dedupe_key="chatwoot_out:recB", conversation_id=820)
    provider = _RecordingProvider(wamid="wamid.RESUME")

    await _lock_batch(session_maker)
    prepared = await wiw._prepare_operator_relay(event_id, provider)  # commits queued (crash here)
    assert prepared.outbox_id is not None
    assert len(provider.sent) == 0

    resumed = await wiw.resume_queued_operator_relay(provider)
    assert resumed == 1
    assert len(provider.sent) == 1  # exactly one provider call
    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1 and rows[0].status == "sent"  # no second Outbox
    assert rows[0].provider_message_id == "wamid.RESUME"


# ===========================================================================
# Test C — crash after claim: committed sending → unknown (no provider)
# ===========================================================================


@pytest.mark.asyncio
async def test_crash_after_claim_recovers_unknown(session_maker, monkeypatch) -> None:
    _enable(monkeypatch, session_maker)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_stale_sending_seconds", 0)
    await _make_sender(session_maker, sender_id=33)
    await _open_window(session_maker, dedupe_key="win:recC")
    event_id = await _insert_received(session_maker, dedupe_key="chatwoot_out:recC", conversation_id=830)
    provider = _RecordingProvider()

    await _lock_batch(session_maker)
    prepared = await wiw._prepare_operator_relay(event_id, provider)
    claimed = (await wiw._claim_operator_relay(prepared.outbox_id, event_id)).claimed  # committed 'sending' (crash)
    assert claimed is not None
    assert len(provider.sent) == 0

    stats = await wiw.recover_operator_relay_lifecycle(provider)
    assert stats.recovered_sending == 1
    assert len(provider.sent) == 0  # recovery never sends
    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1 and rows[0].status == "unknown"
    assert rows[0].meta.get("manual_review_required") is True
    assert (await _reload_event(session_maker, event_id)).error == "operator_relay: delivery outcome unknown"

    # A later poll cycle must not resend.
    await wiw.run_poll_cycle(provider, batch_size=50, run_recovery=True)
    assert len(provider.sent) == 0


# ===========================================================================
# Test D — a finalize exception isolates: it must not kill the loop
# ===========================================================================


@pytest.mark.asyncio
async def test_finalize_exception_does_not_stop_batch(session_maker, monkeypatch) -> None:
    _enable(monkeypatch, session_maker)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_stale_sending_seconds", 0)
    await _make_sender(session_maker, sender_id=34)
    await _open_window(session_maker, dedupe_key="win:recD")
    await _insert_received(session_maker, dedupe_key="chatwoot_out:recD:1", conversation_id=840)
    await _insert_received(session_maker, dedupe_key="chatwoot_out:recD:2", conversation_id=850)
    provider = _RecordingProvider()

    real_finalize = wiw._finalize_operator_relay
    calls = {"n": 0}

    async def _flaky_finalize(claimed, outcome):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("finalize db failure for the first event")
        return await real_finalize(claimed, outcome)

    monkeypatch.setattr(wiw, "_finalize_operator_relay", _flaky_finalize)

    stats = await wiw.run_poll_cycle(provider, batch_size=50, run_recovery=False)

    # The loop survived the first event's failure and still processed the second.
    assert stats.processed + stats.failed == 2
    assert stats.failed == 1
    # Both events reached the provider (both were claimed and sent).
    assert len(provider.sent) == 2

    # The failed event keeps a durable 'sending' row (never lost, never resent).
    async with session_maker() as s:
        by_event = {r.source_whatsapp_event_id: r.status for r in (await s.execute(select(OutboxMessage))).scalars()}
    statuses = sorted(by_event.values())
    assert "sending" in statuses  # the crashed-finalize event
    assert "sent" in statuses  # the healthy event

    # Stale recovery turns the stranded 'sending' into 'unknown' — no resend.
    monkeypatch.setattr(wiw, "_finalize_operator_relay", real_finalize)
    recovered = await wiw.recover_stale_operator_relay_sending(provider)
    assert recovered == 1
    assert len(provider.sent) == 2  # unchanged
    async with session_maker() as s:
        total = (await s.execute(select(func.count()).select_from(OutboxMessage))).scalar_one()
    assert total == 2  # still exactly one Outbox per event


# ===========================================================================
# Recovery cycle ordering: sending recovered BEFORE queued resume
# ===========================================================================


@pytest.mark.asyncio
async def test_recovery_cycle_reports_all_three_actions(session_maker, monkeypatch) -> None:
    _enable(monkeypatch, session_maker)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_stale_sending_seconds", 0)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_stale_processing_seconds", 0)
    await _make_sender(session_maker, sender_id=35)
    await _open_window(session_maker, dedupe_key="win:recE")

    # (1) a stale 'sending' row; (2) a stale 'processing' event without Outbox;
    # (3) a committed 'queued' row to resume.
    e_sending = await _insert_received(session_maker, dedupe_key="cw:recE:s", conversation_id=860, age_seconds=600)
    e_processing = await _insert_received(session_maker, dedupe_key="cw:recE:p", conversation_id=870, age_seconds=600)
    e_queued = await _insert_received(session_maker, dedupe_key="cw:recE:q", conversation_id=880)
    provider = _RecordingProvider()

    await _lock_batch(session_maker)  # all three → processing
    # sending: prepare + claim
    p1 = await wiw._prepare_operator_relay(e_sending, provider)
    await wiw._claim_operator_relay(p1.outbox_id, e_sending)
    # queued: prepare only
    await wiw._prepare_operator_relay(e_queued, provider)
    # processing: nothing (no Outbox)

    stats = await wiw.recover_operator_relay_lifecycle(provider)
    assert stats.recovered_sending == 1
    assert stats.recovered_processing == 1
    assert stats.resumed_queued == 1
    # queued resume actually sent; the stale-sending recovery did NOT send.
    assert len(provider.sent) == 1
    assert (await _outboxes_for(session_maker, e_sending))[0].status == "unknown"
    assert (await _outboxes_for(session_maker, e_queued))[0].status == "sent"
    assert (await _reload_event(session_maker, e_processing)).status == "received"


@pytest.mark.asyncio
async def test_run_poll_cycle_runs_recovery_when_requested(session_maker, monkeypatch) -> None:
    """run_poll_cycle(run_recovery=True) must actually drive the recovery cycle:
    a committed queued intent left by a crash is resumed and sent."""
    _enable(monkeypatch, session_maker)
    await _make_sender(session_maker, sender_id=36)
    await _open_window(session_maker, dedupe_key="win:recF")
    event_id = await _insert_received(session_maker, dedupe_key="cw:recF", conversation_id=890)
    provider = _RecordingProvider()

    await _lock_batch(session_maker)
    await wiw._prepare_operator_relay(event_id, provider)  # queued; event still 'processing'

    stats = await wiw.run_poll_cycle(provider, batch_size=50, run_recovery=True)
    assert stats.recovery is not None
    assert stats.recovery.resumed_queued == 1
    assert len(provider.sent) == 1
    rows = await _outboxes_for(session_maker, event_id)
    assert len(rows) == 1 and rows[0].status == "sent"


# ===========================================================================
# Starvation: older non-operator 'processing' rows must not fill the batch
# ===========================================================================


async def _insert_non_operator_processing(session_maker, *, dedupe_key: str, age_seconds: float) -> int:
    """A stale 'processing' event that is NOT an operator relay (no marker)."""
    now = datetime.now(timezone.utc)
    async with session_maker() as s:
        async with s.begin():
            evt = WhatsAppEvent(
                dedupe_key=dedupe_key,
                received_at=now - timedelta(seconds=age_seconds),
                status="processing",
                query={},
                headers={},
                payload={"entry": [{"changes": [{"value": {"statuses": []}}]}]},
            )
            s.add(evt)
            await s.flush()
            return int(evt.id)


@pytest.mark.asyncio
async def test_non_operator_rows_do_not_starve_relay_recovery(session_maker, monkeypatch) -> None:
    """Older non-operator 'processing' rows must not consume the bounded recovery
    batch and strand a newer operator relay event in 'processing' forever."""
    _enable(monkeypatch, session_maker)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_stale_processing_seconds", 300)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_recovery_batch_size", 2)
    await _make_sender(session_maker, sender_id=40)
    await _open_window(session_maker, dedupe_key="win:starve")

    # Two OLDER non-operator stale rows would fill a batch_size=2 batch.
    a_id = await _insert_non_operator_processing(session_maker, dedupe_key="wa:starve:A", age_seconds=9000)
    b_id = await _insert_non_operator_processing(session_maker, dedupe_key="wa:starve:B", age_seconds=8000)
    # A NEWER operator relay event, stale but younger than both.
    c_id = await _insert_received(
        session_maker, dedupe_key="chatwoot_out:starve:C", conversation_id=900, age_seconds=600
    )
    await _lock_batch(session_maker)  # C: received -> processing

    recovered = await wiw.recover_stale_processing_events()

    assert recovered == 1  # only the operator relay event
    assert (await _reload_event(session_maker, a_id)).status == "processing"  # untouched
    assert (await _reload_event(session_maker, b_id)).status == "processing"  # untouched
    assert (await _reload_event(session_maker, c_id)).status == "received"  # rescued

    # The next production poll actually delivers it, exactly once.
    provider = _RecordingProvider()
    await wiw.run_poll_cycle(provider, batch_size=50, run_recovery=False)
    assert len(provider.sent) == 1
    rows = await _outboxes_for(session_maker, c_id)
    assert len(rows) == 1 and rows[0].status == "sent"
    assert (await _reload_event(session_maker, c_id)).status == "processed"
