"""Durable terminal actions and pre-claim safety gates for operator relay.

Covers the last blocker set before merge:

  * secondary actions owed after a committed terminal outcome are DURABLE and
    are recovered after a crash (they are the only manual-review signal, and no
    other recovery path revisits a terminal row);
  * the Meta-circuit action and the Chatwoot note are INDEPENDENT — either one
    failing must never suppress the other;
  * a note failure never rewrites the primary WhatsApp lifecycle result;
  * a recovered ``queued`` intent revalidates the live send gates (feature flag,
    Meta circuit, 24h window) before it may be claimed;
  * a deterministic *text* failure is surfaced to the operator.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import func, select

import altegio_bot.workers.whatsapp_inbox_worker as wiw
from altegio_bot.models.models import OutboxMessage, WhatsAppEvent, WhatsAppSender

PHONE = "+49111222333"
PNID = "PNID_TERMINAL"


# ---------------------------------------------------------------------------
# Doubles / helpers
# ---------------------------------------------------------------------------


class _Provider:
    """Records every send; optionally raises a fixed error."""

    _supports_mirror_kwargs = False

    def __init__(self, *, wamid: str = "wamid.TERM", raise_message: str | None = None) -> None:
        self._wamid = wamid
        self._raise = raise_message
        self.calls = 0

    async def send(self, sender_id: int, phone_e164: str, text: str, **kwargs: Any) -> str:
        self.calls += 1
        if self._raise is not None:
            raise RuntimeError(self._raise)
        return self._wamid

    async def send_template(self, *a: Any, **k: Any) -> str:
        self.calls += 1
        if self._raise is not None:
            raise RuntimeError(self._raise)
        return self._wamid


def _chatwoot_double(sent: list[str], *, raise_exc: Exception | None = None):
    class _CW:
        async def send_message(self, conversation_id: Any, text: str, **kwargs: Any) -> None:
            sent.append(text)
            if raise_exc is not None:
                raise raise_exc

        async def aclose(self) -> None:
            return None

    return _CW


def _enable(monkeypatch, session_maker, *, notes: bool = True) -> None:
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(wiw, "SessionLocal", session_maker)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_reopen_private_note_enabled", notes)
    monkeypatch.setattr(wiw.settings, "meta_circuit_breaker_enabled", False)


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


async def _open_window(session_maker, *, dedupe_key: str, hours_ago: float = 1.0) -> None:
    now = datetime.now(timezone.utc)
    async with session_maker() as s:
        async with s.begin():
            s.add(
                WhatsAppEvent(
                    dedupe_key=dedupe_key,
                    received_at=now - timedelta(hours=hours_ago),
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


async def _insert_relay(session_maker, *, dedupe_key: str, conversation_id: int) -> int:
    async with session_maker() as s:
        async with s.begin():
            evt = WhatsAppEvent(
                dedupe_key=dedupe_key,
                status="received",
                query={},
                headers={},
                payload={
                    "_chatwoot_operator_relay": {
                        "recipient_phone": PHONE,
                        "text": "Hallo",
                        "conversation_id": conversation_id,
                        "message_id": conversation_id + 1,
                        "phone_number_id": PNID,
                        "agent_name": "Anna",
                    }
                },
                chatwoot_conversation_id=conversation_id,
            )
            s.add(evt)
            await s.flush()
            return int(evt.id)


async def _row(session_maker, event_id: int) -> OutboxMessage:
    async with session_maker() as s:
        return (
            await s.execute(select(OutboxMessage).where(OutboxMessage.source_whatsapp_event_id == event_id))
        ).scalar_one()


async def _event(session_maker, event_id: int) -> WhatsAppEvent:
    async with session_maker() as s:
        return await s.get(WhatsAppEvent, event_id)


async def _row_count(session_maker) -> int:
    async with session_maker() as s:
        return (await s.execute(select(func.count()).select_from(OutboxMessage))).scalar_one()


# ===========================================================================
# Durable terminal actions: survive a crash after the terminal commit
# ===========================================================================


@pytest.mark.asyncio
async def test_pending_note_survives_crash_after_terminal_commit(session_maker, monkeypatch) -> None:
    """A crash between the finalize COMMIT and the note delivery must not lose
    the manual-review signal: recovery finds the durable pending marker."""
    _enable(monkeypatch, session_maker)
    await _make_sender(session_maker, sender_id=50)
    await _open_window(session_maker, dedupe_key="win:term1")
    event_id = await _insert_relay(session_maker, dedupe_key="cw:term1", conversation_id=1000)

    # Simulate the crash: the dispatcher never runs after finalize.
    async def _crash_dispatch(outbox_id: int) -> None:
        raise RuntimeError("process died before side effects")

    monkeypatch.setattr(wiw, "_dispatch_terminal_actions_for", _crash_dispatch)
    provider = _Provider(raise_message="Unexpected Meta response")  # -> unknown
    with pytest.raises(RuntimeError):
        await wiw.process_one_event(event_id, provider)

    # The terminal outcome IS committed and the owed note is durably pending.
    row = await _row(session_maker, event_id)
    assert row.status == "unknown"
    assert row.meta.get("private_note_status") == "pending"
    assert row.meta.get("private_note_kind") == wiw._NOTE_KIND_UNKNOWN
    assert row.meta.get("manual_review_required") is True

    # Recovery (production path) delivers it.
    monkeypatch.undo()
    _enable(monkeypatch, session_maker)
    sent: list[str] = []
    monkeypatch.setattr(wiw, "ChatwootClient", _chatwoot_double(sent))

    dispatched = await wiw.dispatch_pending_terminal_actions()

    assert dispatched == 1
    assert sent == [wiw._OPERATOR_RELAY_UNKNOWN_NOTE]
    row = await _row(session_maker, event_id)
    assert row.meta.get("private_note_status") == "sent"
    assert row.status == "unknown"  # primary lifecycle untouched


@pytest.mark.asyncio
async def test_recovery_cycle_dispatches_pending_terminal_actions(session_maker, monkeypatch) -> None:
    """The production recovery cycle includes the terminal-action dispatch step."""
    _enable(monkeypatch, session_maker)
    await _make_sender(session_maker, sender_id=51)
    await _open_window(session_maker, dedupe_key="win:term2")
    event_id = await _insert_relay(session_maker, dedupe_key="cw:term2", conversation_id=1010)

    async def _crash_dispatch(outbox_id: int) -> None:
        raise RuntimeError("crash")

    monkeypatch.setattr(wiw, "_dispatch_terminal_actions_for", _crash_dispatch)
    with pytest.raises(RuntimeError):
        await wiw.process_one_event(event_id, _Provider(raise_message="Unexpected Meta response"))

    monkeypatch.undo()
    _enable(monkeypatch, session_maker)
    sent: list[str] = []
    monkeypatch.setattr(wiw, "ChatwootClient", _chatwoot_double(sent))
    provider = _Provider()

    stats = await wiw.recover_operator_relay_lifecycle(provider)

    assert stats.dispatched_actions == 1
    assert sent == [wiw._OPERATOR_RELAY_UNKNOWN_NOTE]
    assert provider.calls == 0  # recovery of terminal actions NEVER sends to Meta


@pytest.mark.asyncio
async def test_terminal_action_recovery_never_calls_meta(session_maker, monkeypatch) -> None:
    _enable(monkeypatch, session_maker)
    await _make_sender(session_maker, sender_id=52)
    await _open_window(session_maker, dedupe_key="win:term3")
    event_id = await _insert_relay(session_maker, dedupe_key="cw:term3", conversation_id=1020)
    sent: list[str] = []
    monkeypatch.setattr(wiw, "ChatwootClient", _chatwoot_double(sent))

    await wiw.process_one_event(event_id, _Provider(raise_message="Unexpected Meta response"))

    provider = _Provider()
    await wiw.dispatch_pending_terminal_actions()
    assert provider.calls == 0
    assert await _row_count(session_maker) == 1  # never a second Outbox


# ===========================================================================
# Circuit action and note are INDEPENDENT
# ===========================================================================


@pytest.mark.asyncio
async def test_circuit_exception_does_not_block_private_note(session_maker, monkeypatch) -> None:
    """A failing Meta-circuit close must not suppress the operator note."""
    _enable(monkeypatch, session_maker)
    monkeypatch.setattr(wiw.settings, "meta_circuit_breaker_enabled", True)

    async def _boom(**kwargs: Any) -> None:
        raise RuntimeError("circuit backend down")

    monkeypatch.setattr(wiw.meta_circuit, "close_meta_circuit", _boom)
    sent: list[str] = []
    monkeypatch.setattr(wiw, "ChatwootClient", _chatwoot_double(sent))
    await _make_sender(session_maker, sender_id=53)
    await _open_window(session_maker, dedupe_key="win:term4")
    event_id = await _insert_relay(session_maker, dedupe_key="cw:term4", conversation_id=1030)

    # status=503 -> transient -> unknown + circuit action pending
    await wiw.process_one_event(event_id, _Provider(raise_message="Meta send failed status=503"))

    row = await _row(session_maker, event_id)
    assert row.status == "unknown"
    # The circuit action failed but is retryable...
    assert row.meta.get("circuit_action_status") == "pending"
    assert row.meta.get("circuit_action_error") == "RuntimeError"
    # ...and the note was still delivered.
    assert sent == [wiw._OPERATOR_RELAY_UNKNOWN_NOTE]
    assert row.meta.get("private_note_status") == "sent"


@pytest.mark.asyncio
async def test_note_exception_does_not_block_circuit_action(session_maker, monkeypatch) -> None:
    """A failing Chatwoot note must not suppress the Meta-circuit close."""
    _enable(monkeypatch, session_maker)
    monkeypatch.setattr(wiw.settings, "meta_circuit_breaker_enabled", True)
    closed: list[dict[str, Any]] = []

    async def _close(**kwargs: Any) -> None:
        closed.append(kwargs)

    monkeypatch.setattr(wiw.meta_circuit, "close_meta_circuit", _close)
    monkeypatch.setattr(wiw, "ChatwootClient", _chatwoot_double([], raise_exc=RuntimeError("chatwoot down")))
    await _make_sender(session_maker, sender_id=54)
    await _open_window(session_maker, dedupe_key="win:term5")
    event_id = await _insert_relay(session_maker, dedupe_key="cw:term5", conversation_id=1040)

    await wiw.process_one_event(event_id, _Provider(raise_message="Meta send failed status=503"))

    row = await _row(session_maker, event_id)
    assert closed, "circuit close must still run when the note fails"
    assert row.meta.get("circuit_action_status") == "completed"
    assert row.meta.get("private_note_status") == "pending"  # retryable
    assert row.meta.get("private_note_error") == "RuntimeError"
    # Primary lifecycle untouched by either secondary failure.
    assert row.status == "unknown"
    assert row.error == "operator_relay: delivery outcome unknown"


# ===========================================================================
# Deterministic TEXT failure must be visible to the operator
# ===========================================================================


@pytest.mark.asyncio
async def test_deterministic_text_failure_notifies_operator(session_maker, monkeypatch) -> None:
    _enable(monkeypatch, session_maker)
    sent: list[str] = []
    monkeypatch.setattr(wiw, "ChatwootClient", _chatwoot_double(sent))
    await _make_sender(session_maker, sender_id=55)
    await _open_window(session_maker, dedupe_key="win:term6")
    event_id = await _insert_relay(session_maker, dedupe_key="cw:term6", conversation_id=1050)
    provider = _Provider(raise_message="(#131026) Message undeliverable")  # documented rejection

    await wiw.process_one_event(event_id, provider)

    assert provider.calls == 1
    row = await _row(session_maker, event_id)
    assert row.status == "failed"
    assert row.error == "operator_relay: send failed (permanent)"
    assert row.meta.get("private_note_kind") == wiw._NOTE_KIND_TEXT_FAILED
    assert row.meta.get("private_note_status") == "sent"
    assert sent == [wiw._OPERATOR_RELAY_TEXT_FAILED_NOTE]
    # A confirmed rejection may state plainly that it was not sent.
    assert "konnte nicht gesendet werden" in sent[0]
    # No raw provider error / phone / message text leaks into the note or meta.
    for forbidden in ("131026", "Message undeliverable", PHONE):
        assert forbidden not in sent[0]
    assert "131026" not in json.dumps(row.meta)


@pytest.mark.asyncio
async def test_text_failure_note_disabled_keeps_lifecycle(session_maker, monkeypatch) -> None:
    _enable(monkeypatch, session_maker, notes=False)

    def _boom_cw():
        raise AssertionError("Chatwoot must not be contacted when notes are disabled")

    monkeypatch.setattr(wiw, "ChatwootClient", _boom_cw)
    await _make_sender(session_maker, sender_id=56)
    await _open_window(session_maker, dedupe_key="win:term7")
    event_id = await _insert_relay(session_maker, dedupe_key="cw:term7", conversation_id=1060)

    await wiw.process_one_event(event_id, _Provider(raise_message="(#131026) Message undeliverable"))

    row = await _row(session_maker, event_id)
    assert row.status == "failed"
    assert row.error == "operator_relay: send failed (permanent)"
    assert row.meta.get("private_note_status") == "disabled"


@pytest.mark.asyncio
async def test_unknown_note_is_not_replaced_by_failure_text(session_maker, monkeypatch) -> None:
    """An indeterminate outcome must keep the truthful note, never the
    deterministic 'was not sent' wording."""
    _enable(monkeypatch, session_maker)
    sent: list[str] = []
    monkeypatch.setattr(wiw, "ChatwootClient", _chatwoot_double(sent))
    await _make_sender(session_maker, sender_id=57)
    await _open_window(session_maker, dedupe_key="win:term8")
    event_id = await _insert_relay(session_maker, dedupe_key="cw:term8", conversation_id=1070)

    await wiw.process_one_event(event_id, _Provider(raise_message="Unexpected Meta response"))

    assert sent == [wiw._OPERATOR_RELAY_UNKNOWN_NOTE]
    assert "konnte nicht gesendet werden" not in sent[0]
    assert sent[0] != wiw._OPERATOR_RELAY_TEXT_FAILED_NOTE


# ===========================================================================
# Pre-claim safety gates on a RECOVERED queued intent
# ===========================================================================


async def _queued_intent(session_maker, monkeypatch, *, sender_id: int, dedupe: str, conv: int) -> tuple[int, int]:
    """Prepare (only) a committed queued text intent; return (event_id, outbox_id)."""
    await _make_sender(session_maker, sender_id=sender_id)
    await _open_window(session_maker, dedupe_key=f"win:{dedupe}")
    event_id = await _insert_relay(session_maker, dedupe_key=f"cw:{dedupe}", conversation_id=conv)
    prepared = await wiw._prepare_operator_relay(event_id, _Provider())
    assert prepared.outbox_id is not None
    return event_id, prepared.outbox_id


@pytest.mark.asyncio
async def test_queued_text_canceled_when_relay_disabled(session_maker, monkeypatch) -> None:
    _enable(monkeypatch, session_maker, notes=False)
    event_id, _ = await _queued_intent(session_maker, monkeypatch, sender_id=60, dedupe="gate1", conv=1100)

    # Operator switches the relay off before the crashed intent is resumed.
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_enabled", False)
    provider = _Provider()
    await wiw.resume_queued_operator_relay(provider)

    assert provider.calls == 0
    row = await _row(session_maker, event_id)
    assert row.status == "canceled"
    assert row.meta.get("cancel_reason") == "operator_relay_disabled_before_claim"
    assert (await _event(session_maker, event_id)).status == "processed"


@pytest.mark.asyncio
async def test_queued_text_canceled_when_circuit_closed(session_maker, monkeypatch) -> None:
    _enable(monkeypatch, session_maker)
    sent: list[str] = []
    monkeypatch.setattr(wiw, "ChatwootClient", _chatwoot_double(sent))
    event_id, _ = await _queued_intent(session_maker, monkeypatch, sender_id=61, dedupe="gate2", conv=1110)

    # The circuit closed after the intent was committed.
    monkeypatch.setattr(wiw.settings, "meta_circuit_breaker_enabled", True)

    async def _paused(**kwargs: Any) -> bool:
        return True

    monkeypatch.setattr(wiw.meta_circuit, "should_pause_meta_sends", _paused)
    provider = _Provider()
    await wiw.resume_queued_operator_relay(provider)

    assert provider.calls == 0
    row = await _row(session_maker, event_id)
    assert row.status == "canceled"
    assert row.meta.get("cancel_reason") == "meta_circuit_closed_before_claim"
    assert sent == [wiw._OPERATOR_RELAY_CIRCUIT_PAUSED_NOTE]  # operator still notified


@pytest.mark.asyncio
async def test_queued_text_canceled_when_window_expired(session_maker, monkeypatch) -> None:
    """The decisive case: a free-form text prepared while the 24h window was open
    must NOT be sent hours later once the window has closed."""
    _enable(monkeypatch, session_maker)
    sent: list[str] = []
    monkeypatch.setattr(wiw, "ChatwootClient", _chatwoot_double(sent))
    event_id, _ = await _queued_intent(session_maker, monkeypatch, sender_id=62, dedupe="gate3", conv=1120)

    # The window expires while the intent sits queued (crash/restart gap).
    async def _window_closed(session: Any, phone: str, now: Any) -> tuple[bool, Any]:
        return False, None

    monkeypatch.setattr(wiw, "is_whatsapp_customer_window_open", _window_closed)
    provider = _Provider()
    await wiw.resume_queued_operator_relay(provider)

    assert provider.calls == 0  # no Meta policy violation
    row = await _row(session_maker, event_id)
    assert row.status == "canceled"
    assert row.meta.get("cancel_reason") == "customer_service_window_closed_before_claim"
    # The old text intent is NOT silently converted into a template.
    assert row.meta.get("send_type") == "text"
    assert sent == [wiw._relay_note_text(wiw._NOTE_KIND_WINDOW_CLOSED, row)]
    assert (await _event(session_maker, event_id)).status == "processed"


@pytest.mark.asyncio
async def test_queued_template_still_sends_with_closed_window(session_maker, monkeypatch) -> None:
    """A closed window is EXPECTED for a reopen-template intent and must not
    cancel it — only the template-relevant gates are re-checked."""
    _enable(monkeypatch, session_maker, notes=False)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_closed_window_mode", "reopen_template")
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_reopen_template_name", "reopen_tpl")
    await _make_sender(session_maker, sender_id=63)
    # No window event at all -> closed -> template intent.
    event_id = await _insert_relay(session_maker, dedupe_key="cw:gate4", conversation_id=1130)
    prepared = await wiw._prepare_operator_relay(event_id, _Provider())
    assert prepared.send_type == "template"

    provider = _Provider()
    resumed = await wiw.resume_queued_operator_relay(provider)

    assert resumed == 1
    assert provider.calls == 1  # sent exactly once despite the closed window
    row = await _row(session_maker, event_id)
    assert row.status == "sent"


@pytest.mark.asyncio
async def test_canceled_before_claim_is_not_resent_on_replay(session_maker, monkeypatch) -> None:
    _enable(monkeypatch, session_maker, notes=False)
    event_id, _ = await _queued_intent(session_maker, monkeypatch, sender_id=64, dedupe="gate5", conv=1140)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_enabled", False)
    provider = _Provider()
    await wiw.resume_queued_operator_relay(provider)
    assert provider.calls == 0

    # Re-enable and replay: the canceled row is terminal, never resurrected.
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_relay_enabled", True)
    await wiw.process_one_event(event_id, provider)
    await wiw.resume_queued_operator_relay(provider)

    assert provider.calls == 0
    assert await _row_count(session_maker) == 1
    assert (await _row(session_maker, event_id)).status == "canceled"


@pytest.mark.asyncio
async def test_two_concurrent_recovery_workers_claim_at_most_once(session_maker, monkeypatch) -> None:
    """Two workers resuming the same queued intent must produce exactly one send
    and exactly one terminal transition — the gate re-check must not open a race."""
    import asyncio

    _enable(monkeypatch, session_maker, notes=False)
    event_id, _ = await _queued_intent(session_maker, monkeypatch, sender_id=65, dedupe="gate6", conv=1150)
    provider = _Provider()

    await asyncio.gather(
        wiw.resume_queued_operator_relay(provider),
        wiw.resume_queued_operator_relay(provider),
    )

    assert provider.calls == 1
    assert await _row_count(session_maker) == 1
    assert (await _row(session_maker, event_id)).status == "sent"
