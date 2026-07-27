"""The worker logs that runbook §4.1 describes must contain no PII and no injection.

Two paths carry customer/agent data through `whatsapp_inbox_worker`:
  * WhatsApp → Chatwoot (forwarding an inbound message);
  * Chatwoot → WhatsApp (operator relay).

Both used to log the customer phone, the client name and the agent name at INFO.
Beyond the PII leak, those strings are sender-controlled, so a newline or an ANSI
escape inside them could forge an extra log line. These tests pin both properties
against the real `handle_event` code path.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest

import altegio_bot.workers.whatsapp_inbox_worker as worker_module
from altegio_bot.models.models import WhatsAppEvent, WhatsAppSender
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.workers.whatsapp_inbox_worker import handle_event

PHONE = "+4915199999999"
CLIENT_NAME = "PRIVATE CUSTOMER"
MESSAGE_TEXT = "STRENG GEHEIMER KUNDENTEXT"
AGENT_NAME = "Agent\n2026-07-24 INFO forged"
HOSTILE_CONV_ID = "501\nforged"
HOSTILE_MSG_ID = "5001\x1b[31m"

LINE_SEP = chr(0x2028)
PARA_SEP = chr(0x2029)


class _CaptureProvider(WhatsAppProvider):
    """Minimal provider stand-in: records sends, never touches the network."""

    wamid = "wamid.LOGTEST"

    def __init__(self) -> None:
        self.sent: list[tuple] = []

    async def send(self, sender_id, phone_e164, text, contact_name=None) -> str:
        self.sent.append((sender_id, phone_e164, text))
        return self.wamid

    async def send_template(self, *args, **kwargs) -> str:
        self.sent.append((args, kwargs))
        return self.wamid


class _FakeChatwoot:
    """Stands in for ChatwootClient so the forward path runs without network."""

    def __init__(self, *args, **kwargs) -> None:
        pass

    async def get_or_create_incoming_conversation(self, *args, **kwargs):
        return 4242

    async def send_message(self, *args, **kwargs):
        return 777

    async def aclose(self):
        return None


def _assert_no_pii_and_no_injection(caplog) -> str:
    messages = [r.getMessage() for r in caplog.records]
    blob = "\n".join(messages)

    # PII must not appear at all.
    assert PHONE not in blob
    assert CLIENT_NAME not in blob
    assert MESSAGE_TEXT not in blob
    assert "Agent" not in blob

    # Sender-controlled strings must not forge a log line or emit control bytes.
    for record_message in messages:
        assert "\n" not in record_message
        assert "\r" not in record_message
        assert "\x1b" not in record_message
        assert LINE_SEP not in record_message
        assert PARA_SEP not in record_message

    # NOTE: the literal word "forged" may still appear *inside* an escaped
    # field — that is fine and is exactly what escaping is for. What must never
    # happen is a raw newline turning it into its own physical log line, which
    # the per-record checks above already guarantee.
    return blob


@pytest.mark.asyncio
async def test_operator_relay_logs_carry_no_pii_or_injection(session_maker, monkeypatch, caplog) -> None:
    """Chatwoot → WhatsApp relay: no phone, no agent name, no message body."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=911,
                    company_id=1,
                    sender_code="default",
                    phone_number_id="PNID_LOGTEST",
                    display_phone="+49",
                    is_active=True,
                )
            )
            # Open the 24h window so the direct-text branch is exercised.
            now = datetime.now(timezone.utc)
            session.add(
                WhatsAppEvent(
                    dedupe_key="wa:inbound:logtest",
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
                                                {
                                                    "from": PHONE.lstrip("+"),
                                                    "type": "text",
                                                    "text": {"body": "Hallo"},
                                                    "id": "wamid.win",
                                                }
                                            ],
                                            "metadata": {"phone_number_id": "PNID_LOGTEST"},
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                )
            )
            relay_evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:logtest",
                status="received",
                error=None,
                query={},
                headers={},
                payload={
                    "_chatwoot_operator_relay": {
                        "recipient_phone": PHONE,
                        "text": MESSAGE_TEXT,
                        "conversation_id": HOSTILE_CONV_ID,
                        "message_id": HOSTILE_MSG_ID,
                        "phone_number_id": "PNID_LOGTEST",
                        "agent_name": AGENT_NAME,
                        "agent_id": 1,
                        "contact_name": CLIENT_NAME,
                    }
                },
            )
            session.add(relay_evt)
            await session.flush()

            with caplog.at_level(logging.DEBUG):
                await handle_event(session, relay_evt, provider)

    blob = _assert_no_pii_and_no_injection(caplog)
    # Technical identifiers stay available for tracing.
    assert "conv_id=" in blob
    assert "operator_relay" in blob


@pytest.mark.asyncio
async def test_incoming_forward_logs_carry_no_pii_or_injection(session_maker, monkeypatch, caplog) -> None:
    """WhatsApp → Chatwoot forwarding: no phone, no client name, no body."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=912,
                    company_id=1,
                    sender_code="default",
                    phone_number_id="PNID_FWD",
                    display_phone="+49",
                    is_active=True,
                )
            )
            evt = WhatsAppEvent(
                dedupe_key="wa:inbound:fwd:logtest",
                status="received",
                error=None,
                query={},
                headers={},
                payload={
                    "entry": [
                        {
                            "changes": [
                                {
                                    "value": {
                                        "messages": [
                                            {
                                                "from": PHONE.lstrip("+"),
                                                "type": "text",
                                                "text": {"body": MESSAGE_TEXT},
                                                "id": "wamid.fwd",
                                            }
                                        ],
                                        "contacts": [{"profile": {"name": CLIENT_NAME}}],
                                        "metadata": {"phone_number_id": "PNID_FWD"},
                                    }
                                }
                            ]
                        }
                    ]
                },
            )
            session.add(evt)
            await session.flush()

            with caplog.at_level(logging.DEBUG):
                await handle_event(session, evt, provider)

    blob = _assert_no_pii_and_no_injection(caplog)
    # Positive assertions: prove the branch actually ran and its technical
    # tracing survived. Without these the test would still pass if forwarding
    # stopped logging entirely.
    assert "Forwarded incoming message to Chatwoot" in blob
    assert "event_id=" in blob
    assert "conversation_id=" in blob
    assert "message_id=" in blob


# ---------------------------------------------------------------------------
# AST defense-in-depth guard
# ---------------------------------------------------------------------------

# Symbols whose *value* is customer/agent PII. Names bound to these, and
# attribute accesses ending in these, must never be handed to a logger raw.
_PII_SYMBOLS = frozenset(
    {
        "phone_e164",
        "phone",
        "recipient_phone",
        "raw_phone",
        "client_name",
        "contact_name",
        "agent_name",
        "message_text",
        "text",
        "body",
    }
)
_LOG_METHODS = frozenset({"info", "warning", "error", "exception", "debug"})


def _find_logger_pii_references(source: str) -> list[tuple[int, str]]:
    """Defense-in-depth check for DIRECT use of known PII-bearing symbols in
    logger arguments. Runtime branch tests remain the primary guarantee.

    This is deliberately NOT taint analysis. It inspects every positional arg,
    keyword value, f-string field, attribute, and nested dict/list/tuple of a
    ``logger.<level>(...)`` call, and flags a bare reference to a PII symbol
    (``ast.Name.id`` or ``ast.Attribute.attr``). A reference wrapped in ANY call
    — ``safe_log_value(phone)``, ``bool(phone)``, ``len(text)`` — is treated as
    sanitised and not flagged, because the guard does not descend into nested
    calls. That is its known blind spot, accepted on purpose.
    """
    import ast

    offenders: list[tuple[int, str]] = []

    def _scan(node: ast.AST, lineno: int) -> None:
        # Do NOT descend into nested calls: a PII symbol handed to another
        # function (safe_log_value/bool/…) is considered sanitised.
        if isinstance(node, ast.Call):
            return
        if isinstance(node, ast.Name) and node.id in _PII_SYMBOLS:
            offenders.append((lineno, node.id))
            return
        if isinstance(node, ast.Attribute) and node.attr in _PII_SYMBOLS:
            offenders.append((lineno, node.attr))
            return
        for child in ast.iter_child_nodes(node):
            _scan(child, lineno)

    for call in ast.walk(ast.parse(source)):
        if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Attribute):
            continue
        if call.func.attr not in _LOG_METHODS:
            continue
        if not (isinstance(call.func.value, ast.Name) and call.func.value.id == "logger"):
            continue
        roots: list[ast.AST] = list(call.args)
        roots.extend(kw.value for kw in call.keywords)
        for root in roots:
            _scan(root, call.lineno)

    return offenders


def test_worker_log_calls_never_reference_pii_symbols() -> None:
    """No logger call in the worker may hand a PII symbol to the logger raw."""
    from pathlib import Path

    src = Path(worker_module.__file__).read_text(encoding="utf-8")
    offenders = _find_logger_pii_references(src)
    assert offenders == [], f"PII passed to logger at {offenders}"


@pytest.mark.parametrize(
    "snippet",
    [
        'logger.info("%s", phone_e164)',
        'logger.info(f"phone={phone_e164}")',
        'logger.info("%s", client.phone_e164)',
        'logger.info("x", extra={"phone": phone_e164})',
        'logger.info("x", phone=phone_e164)',
        'logger.warning("%s", [agent_name])',
        'logger.info("%s", (recipient_phone,))',
    ],
)
def test_ast_guard_catches_pii(snippet: str) -> None:
    assert _find_logger_pii_references(snippet), f"guard missed: {snippet}"


@pytest.mark.parametrize(
    "snippet",
    [
        'logger.info("event_id=%s", event.id)',
        'logger.info("phone_present=%s", bool(phone_e164))',
        'logger.info("conv_id=%s", safe_log_value(conversation_id, limit=32))',
        'logger.info("error_type=%s", type(exc).__name__)',
        'logger.info("company_id=%s", company_id)',
    ],
)
def test_ast_guard_allows_safe_calls(snippet: str) -> None:
    # bool(phone_e164) is intentionally allowed: the value is not disclosed and
    # the guard does not descend into wrapping calls.
    assert _find_logger_pii_references(snippet) == [], f"guard false-positive: {snippet}"


# ---------------------------------------------------------------------------
# Operator-relay error branches: technical ids escaped, no PII/secret/injection
# ---------------------------------------------------------------------------
#
# Assertions here are scoped to the worker's own logger. `providers.dummy`'s
# safe_send logs the raw provider error itself — that is the documented
# remaining-debt path (section 9), out of scope for this change.

_WORKER_LOGGER = "whatsapp_inbox_worker"

HOSTILE_PNID = "PNI" + chr(0x2028) + "forged"
SECRET_ERR = "token=SECRETVAL https://secret-host/?token=SECRETVAL\nforged status=500"
PERMANENT_ERR = "invalid template token=SECRETVAL https://secret-host/?t=SECRETVAL\nforged"


class _RaisingSendProvider(WhatsAppProvider):
    """provider.send raises a transient-looking error carrying a secret + newline."""

    def __init__(self, message: str) -> None:
        self._message = message
        self.sent: list = []

    async def send(self, *args, **kwargs) -> str:
        raise RuntimeError(self._message)

    async def send_template(self, *args, **kwargs) -> str:
        return "wamid.tpl"


class _RaisingTemplateProvider(WhatsAppProvider):
    """send_template raises a permanent error carrying a secret + newline."""

    def __init__(self, message: str) -> None:
        self._message = message
        self.sent: list = []

    async def send(self, *args, **kwargs) -> str:
        return "wamid.txt"

    async def send_template(self, *args, **kwargs) -> str:
        raise RuntimeError(self._message)


class _RaisingChatwoot:
    """Chatwoot client whose note send raises a secret-bearing exception."""

    def __init__(self, *args, **kwargs) -> None:
        pass

    async def get_or_create_incoming_conversation(self, *args, **kwargs):
        return 4242

    async def send_message(self, *args, **kwargs):
        raise RuntimeError(SECRET_ERR)

    async def aclose(self):
        return None


def _worker_messages(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.name == _WORKER_LOGGER]


def _assert_worker_log_clean(caplog) -> str:
    messages = _worker_messages(caplog)
    blob = "\n".join(messages)

    assert PHONE not in blob
    assert CLIENT_NAME not in blob
    assert MESSAGE_TEXT not in blob
    assert "Agent" not in blob
    assert "SECRETVAL" not in blob
    assert "secret-host" not in blob
    assert "token=" not in blob

    for message in messages:
        assert "\n" not in message
        assert "\r" not in message
        assert "\x1b" not in message
        assert LINE_SEP not in message
        assert PARA_SEP not in message

    return blob


def _relay_event(**relay_overrides) -> WhatsAppEvent:
    relay = {
        "recipient_phone": PHONE,
        "text": MESSAGE_TEXT,
        "conversation_id": HOSTILE_CONV_ID,
        "message_id": HOSTILE_MSG_ID,
        "phone_number_id": HOSTILE_PNID,
        "agent_name": AGENT_NAME,
        "agent_id": 1,
        "contact_name": CLIENT_NAME,
    }
    relay.update(relay_overrides)
    return WhatsAppEvent(
        dedupe_key=f"chatwoot_out:branch:{relay_overrides.get('_k', 'x')}",
        status="received",
        error=None,
        query={},
        headers={},
        payload={"_chatwoot_operator_relay": {k: v for k, v in relay.items() if k != "_k"}},
    )


async def _run_relay(session_maker, monkeypatch, caplog, *, provider, sender_id, window_open, event):
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_relay_enabled", True)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=sender_id,
                    company_id=1,
                    sender_code="default",
                    phone_number_id=HOSTILE_PNID,
                    display_phone="+49",
                    is_active=True,
                )
            )
            if window_open:
                session.add(
                    WhatsAppEvent(
                        dedupe_key=f"wa:inbound:branch:{sender_id}",
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
                                                    {
                                                        "from": PHONE.lstrip("+"),
                                                        "type": "text",
                                                        "text": {"body": "hi"},
                                                        "id": "w1",
                                                    }
                                                ],
                                                "metadata": {"phone_number_id": HOSTILE_PNID},
                                            }
                                        }
                                    ]
                                }
                            ]
                        },
                    )
                )
            session.add(event)
            await session.flush()
            with caplog.at_level(logging.DEBUG):
                await handle_event(session, event, provider)


@pytest.mark.asyncio
async def test_relay_log_circuit_already_closed(session_maker, monkeypatch, caplog) -> None:
    async def _paused(*args, **kwargs):
        return True

    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    monkeypatch.setattr(worker_module.meta_circuit, "should_pause_meta_sends", _paused)
    provider = _CaptureProvider()

    await _run_relay(
        session_maker,
        monkeypatch,
        caplog,
        provider=provider,
        sender_id=960,
        window_open=True,
        event=_relay_event(_k="circuit"),
    )

    assert provider.sent == []  # paused before send
    blob = _assert_worker_log_clean(caplog)
    assert "Meta circuit closed" in blob


@pytest.mark.asyncio
async def test_relay_log_transient_send_failure(session_maker, monkeypatch, caplog) -> None:
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    provider = _RaisingSendProvider(SECRET_ERR)

    await _run_relay(
        session_maker,
        monkeypatch,
        caplog,
        provider=provider,
        sender_id=961,
        window_open=True,
        event=_relay_event(_k="transient"),
    )

    blob = _assert_worker_log_clean(caplog)
    assert "transient Meta error closed circuit" in blob
    assert "error_kind=" in blob


@pytest.mark.asyncio
async def test_relay_log_permanent_send_failure(session_maker, monkeypatch, caplog) -> None:
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    # Token-expired is classified permanent, so the transient path is skipped.
    provider = _RaisingSendProvider("access token expired token=SECRETVAL https://h/?t=SECRETVAL\nx")

    await _run_relay(
        session_maker,
        monkeypatch,
        caplog,
        provider=provider,
        sender_id=962,
        window_open=True,
        event=_relay_event(_k="perm"),
    )

    blob = _assert_worker_log_clean(caplog)
    assert "send failed" in blob
    assert "error_kind=permanent" in blob


@pytest.mark.asyncio
async def test_relay_log_closed_window_note_success(session_maker, monkeypatch, caplog) -> None:
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_closed_window_mode", "private_note_only")
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_reopen_private_note_enabled", True)
    provider = _CaptureProvider()

    await _run_relay(
        session_maker,
        monkeypatch,
        caplog,
        provider=provider,
        sender_id=963,
        window_open=False,
        event=_relay_event(_k="cwnote_ok"),
    )

    assert provider.sent == []
    blob = _assert_worker_log_clean(caplog)
    assert "closed-window note sent" in blob


@pytest.mark.asyncio
async def test_relay_log_closed_window_note_failure(session_maker, monkeypatch, caplog) -> None:
    monkeypatch.setattr(worker_module, "ChatwootClient", _RaisingChatwoot)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_closed_window_mode", "private_note_only")
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_reopen_private_note_enabled", True)
    provider = _CaptureProvider()

    event = _relay_event(_k="cwnote_fail")
    await _run_relay(
        session_maker,
        monkeypatch,
        caplog,
        provider=provider,
        sender_id=964,
        window_open=False,
        event=event,
    )

    blob = _assert_worker_log_clean(caplog)
    assert "closed-window note failed" in blob
    assert "error_type=RuntimeError" in blob
    # Persisted error carries only the class name, never the raw exception text.
    assert "SECRETVAL" not in (event.error or "")


@pytest.mark.asyncio
async def test_relay_log_reopen_template_failure_and_failure_note(session_maker, monkeypatch, caplog) -> None:
    monkeypatch.setattr(worker_module, "ChatwootClient", _RaisingChatwoot)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_closed_window_mode", "reopen_template")
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_reopen_template_name", "reopen_tpl")
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_reopen_private_note_enabled", True)
    provider = _RaisingTemplateProvider(PERMANENT_ERR)

    await _run_relay(
        session_maker,
        monkeypatch,
        caplog,
        provider=provider,
        sender_id=965,
        window_open=False,
        event=_relay_event(_k="reopen_fail"),
    )

    blob = _assert_worker_log_clean(caplog)
    assert "reopen template failed" in blob
    assert "error_kind=permanent" in blob
    # The failure-note Chatwoot send also raised → its class name, no raw text.
    assert "failure note failed" in blob
    assert "error_type=RuntimeError" in blob


@pytest.mark.asyncio
async def test_relay_log_native_target_not_found(session_maker, monkeypatch, caplog) -> None:
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    provider = _CaptureProvider()

    await _run_relay(
        session_maker,
        monkeypatch,
        caplog,
        provider=provider,
        sender_id=966,
        window_open=True,
        event=_relay_event(_k="native", reply_to_chatwoot_message_id=999999),
    )

    blob = _assert_worker_log_clean(caplog)
    assert "native reply context target not found" in blob
