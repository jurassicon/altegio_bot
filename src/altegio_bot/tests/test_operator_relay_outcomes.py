"""Conservative provider-outcome classification and truthful unknown handling.

Covers:
  * the outcome matrix (§15): only a documented deterministic rejection is
    ``failed``; every uncertain post-request result is ``unknown``;
  * the truthful manual-review note for every ``unknown`` regardless of the
    circuit breaker (§18);
  * the secondary Chatwoot note never overwriting the primary lifecycle error
    (§22).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import func, select

import altegio_bot.workers.whatsapp_inbox_worker as wiw
from altegio_bot.models.models import OutboxMessage, WhatsAppEvent, WhatsAppSender

PHONE = "+49111222333"
PNID = "PNID_OUTCOME"


class _Provider:
    """Provider double whose send behavior is configurable per test."""

    _supports_mirror_kwargs = False

    def __init__(self, *, wamid: Any = "wamid.OK", raise_message: str | None = None) -> None:
        self._wamid = wamid
        self._raise = raise_message
        self.calls = 0

    async def send(self, sender_id: int, phone_e164: str, text: str, **kwargs: Any) -> Any:
        self.calls += 1
        if self._raise is not None:
            raise RuntimeError(self._raise)
        return self._wamid

    async def send_template(self, *a: Any, **k: Any) -> Any:
        self.calls += 1
        if self._raise is not None:
            raise RuntimeError(self._raise)
        return self._wamid


def _mock_cw(sent_texts: list[str], *, raise_exc: Exception | None = None):
    class _CW:
        async def send_message(self, conversation_id: Any, text: str, **kwargs: Any) -> None:
            sent_texts.append(text)
            if raise_exc is not None:
                raise raise_exc

        async def aclose(self) -> None:
            return None

    return _CW


def _enable(monkeypatch, session_maker, *, notes: bool = False) -> None:
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


async def _row_count(session_maker, event_id: int) -> int:
    async with session_maker() as s:
        return (
            await s.execute(
                select(func.count())
                .select_from(OutboxMessage)
                .where(OutboxMessage.source_whatsapp_event_id == event_id)
            )
        ).scalar_one()


# ===========================================================================
# §15 outcome matrix
# ===========================================================================

_MATRIX = [
    # (raise_message, wamid, expected_status, expected_event_error)
    ("__ok__", "wamid.SENT", "sent", None),
    ("(#131026) Message undeliverable", None, "failed", "operator_relay: send failed (permanent)"),
    ("template does not exist", None, "failed", "operator_relay: send failed (permanent)"),
    ("access token expired code=190", None, "failed", "operator_relay: send failed (permanent)"),
    ("Recipient phone number not in allowed list", None, "failed", "operator_relay: send failed (permanent)"),
    ("timeout", None, "unknown", "operator_relay: delivery outcome unknown"),
    ("connection reset by peer", None, "unknown", "operator_relay: delivery outcome unknown"),
    ("Unexpected Meta response", None, "unknown", "operator_relay: delivery outcome unknown"),
    ("Expecting value: line 1 column 1 (char 0)", None, "unknown", "operator_relay: delivery outcome unknown"),
    ("some totally unknown glitch", None, "unknown", "operator_relay: delivery outcome unknown"),
    ("__none_wamid__", None, "unknown", "operator_relay: delivery outcome unknown"),  # 2xx without wamid
]


@pytest.mark.parametrize("raise_message,wamid,expected_status,expected_error", _MATRIX)
@pytest.mark.asyncio
async def test_outcome_matrix(session_maker, monkeypatch, raise_message, wamid, expected_status, expected_error):
    _enable(monkeypatch, session_maker)
    sid = 400 + _MATRIX.index((raise_message, wamid, expected_status, expected_error))
    await _make_sender(session_maker, sender_id=sid)
    await _open_window(session_maker, dedupe_key=f"win:matrix:{sid}")
    event_id = await _insert_relay(session_maker, dedupe_key=f"cw:matrix:{sid}", conversation_id=900 + sid)

    if raise_message == "__ok__":
        provider = _Provider(wamid=wamid)
    elif raise_message == "__none_wamid__":
        provider = _Provider(wamid=None)  # 2xx, no raise, but no usable wamid
    else:
        provider = _Provider(raise_message=raise_message)

    await wiw.process_one_event(event_id, provider)

    assert provider.calls == 1
    row = await _row(session_maker, event_id)
    assert row.status == expected_status
    assert (await _event(session_maker, event_id)).error == expected_error
    if expected_status == "unknown":
        assert row.meta.get("manual_review_required") is True
    if expected_status == "sent":
        assert row.provider_message_id == wamid

    # No automatic resend on a second run, whatever the outcome.
    await wiw.process_one_event(event_id, provider)
    assert provider.calls == 1
    assert await _row_count(session_maker, event_id) == 1


@pytest.mark.asyncio
async def test_real_send_disabled_is_deterministic_not_unknown(session_maker, monkeypatch):
    """A pre-request refusal (no HTTP) is a deterministic failure, not unknown."""
    _enable(monkeypatch, session_maker)
    # Force safe_send's pre-request guard: provider key = meta_cloud, real send off.
    monkeypatch.setenv("WHATSAPP_PROVIDER", "meta_cloud")
    monkeypatch.delenv("ALLOW_REAL_SEND", raising=False)
    await _make_sender(session_maker, sender_id=460)
    await _open_window(session_maker, dedupe_key="win:realdisabled")
    event_id = await _insert_relay(session_maker, dedupe_key="cw:realdisabled", conversation_id=960)
    provider = _Provider()

    await wiw.process_one_event(event_id, provider)

    assert provider.calls == 0  # request never made
    row = await _row(session_maker, event_id)
    assert row.status == "failed"
    assert row.meta.get("error_kind") == "real_send_disabled"


# ===========================================================================
# §18 truthful manual-review note for every unknown
# ===========================================================================


@pytest.mark.asyncio
async def test_unknown_note_circuit_enabled(session_maker, monkeypatch):
    _enable(monkeypatch, session_maker, notes=True)
    monkeypatch.setattr(wiw.settings, "meta_circuit_breaker_enabled", True)
    closed: list[dict[str, Any]] = []

    async def _close(**kwargs: Any) -> None:
        closed.append(kwargs)

    monkeypatch.setattr(wiw.meta_circuit, "close_meta_circuit", _close)
    texts: list[str] = []
    monkeypatch.setattr(wiw, "ChatwootClient", _mock_cw(texts))

    await _make_sender(session_maker, sender_id=470)
    await _open_window(session_maker, dedupe_key="win:note1")
    event_id = await _insert_relay(session_maker, dedupe_key="cw:note1", conversation_id=970)
    await wiw.process_one_event(event_id, _Provider(raise_message="status=503 timeout"))

    assert closed  # transient → circuit closed
    assert len(texts) == 1
    assert texts[0] == wiw._OPERATOR_RELAY_UNKNOWN_NOTE
    assert "nicht an WhatsApp gesendet" not in texts[0]  # never claims "not sent"
    row = await _row(session_maker, event_id)
    assert row.status == "unknown" and row.meta.get("manual_review_required") is True


@pytest.mark.asyncio
async def test_unknown_note_circuit_disabled_still_sent(session_maker, monkeypatch):
    _enable(monkeypatch, session_maker, notes=True)  # breaker disabled by _enable
    closed: list[dict[str, Any]] = []

    async def _close(**kwargs: Any) -> None:
        closed.append(kwargs)

    monkeypatch.setattr(wiw.meta_circuit, "close_meta_circuit", _close)
    texts: list[str] = []
    monkeypatch.setattr(wiw, "ChatwootClient", _mock_cw(texts))

    await _make_sender(session_maker, sender_id=471)
    await _open_window(session_maker, dedupe_key="win:note2")
    event_id = await _insert_relay(session_maker, dedupe_key="cw:note2", conversation_id=971)
    await wiw.process_one_event(event_id, _Provider(raise_message="Unexpected Meta response"))

    assert closed == []  # circuit action gated on breaker
    assert texts == [wiw._OPERATOR_RELAY_UNKNOWN_NOTE]  # note still sent
    row = await _row(session_maker, event_id)
    assert row.status == "unknown" and row.meta.get("manual_review_required") is True


@pytest.mark.asyncio
async def test_unknown_note_disabled_records_metadata(session_maker, monkeypatch):
    _enable(monkeypatch, session_maker, notes=False)  # note network call must not happen

    def _boom_cw():  # any use would be a bug
        raise AssertionError("ChatwootClient must not be constructed when notes disabled")

    monkeypatch.setattr(wiw, "ChatwootClient", _boom_cw)
    await _make_sender(session_maker, sender_id=472)
    await _open_window(session_maker, dedupe_key="win:note3")
    event_id = await _insert_relay(session_maker, dedupe_key="cw:note3", conversation_id=972)
    await wiw.process_one_event(event_id, _Provider(raise_message="Unexpected Meta response"))

    row = await _row(session_maker, event_id)
    assert row.status == "unknown"
    assert row.meta.get("private_note_status") == "disabled"
    assert row.meta.get("manual_review_required") is True


# ===========================================================================
# §22 note failure never overwrites the primary lifecycle error
# ===========================================================================


@pytest.mark.asyncio
async def test_unknown_note_failure_preserves_primary_error(session_maker, monkeypatch):
    _enable(monkeypatch, session_maker, notes=True)
    texts: list[str] = []
    monkeypatch.setattr(wiw, "ChatwootClient", _mock_cw(texts, raise_exc=RuntimeError("chatwoot down token=SECRET")))
    await _make_sender(session_maker, sender_id=480)
    await _open_window(session_maker, dedupe_key="win:notefail")
    event_id = await _insert_relay(session_maker, dedupe_key="cw:notefail", conversation_id=980)
    await wiw.process_one_event(event_id, _Provider(raise_message="Unexpected Meta response"))

    row = await _row(session_maker, event_id)
    assert row.status == "unknown"
    # Primary WhatsApp lifecycle error is preserved; the note failure lives in meta.
    assert row.error == "operator_relay: delivery outcome unknown"
    assert row.meta.get("private_note_status") == "failed"
    assert row.meta.get("private_note_error") == "RuntimeError"
    assert row.meta.get("private_note_updated_at") is not None
    assert "SECRET" not in (row.error or "")


@pytest.mark.asyncio
async def test_failed_note_failure_preserves_primary_error(session_maker, monkeypatch):
    """A deterministic failure keeps its marker even if the (template-failure)
    note send fails."""
    _enable(monkeypatch, session_maker, notes=True)
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_closed_window_mode", "reopen_template")
    monkeypatch.setattr(wiw.settings, "chatwoot_operator_reopen_template_name", "reopen_tpl")
    monkeypatch.setattr(wiw, "ChatwootClient", _mock_cw([], raise_exc=RuntimeError("chatwoot down")))
    await _make_sender(session_maker, sender_id=481)
    # No window → template path; deterministic template rejection → failed.
    event_id = await _insert_relay(session_maker, dedupe_key="cw:failnote", conversation_id=981)
    await wiw.process_one_event(event_id, _Provider(raise_message="template does not exist"))

    row = await _row(session_maker, event_id)
    assert row.status == "failed"
    assert row.error == "operator_relay: send failed (permanent)"
    assert row.meta.get("private_note_status") == "failed"
