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
from sqlalchemy import select

import altegio_bot.workers.whatsapp_inbox_worker as worker_module
from altegio_bot.models.models import OutboxMessage, WhatsAppEvent, WhatsAppSender
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.workers.whatsapp_inbox_worker import handle_event, process_one_event

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

    async def send(self, sender_id, phone_e164, text, contact_name=None, **kwargs) -> str:
        # Accept extra kwargs (e.g. reply_to_provider_message_id) that safe_send
        # forwards when a native reply context is resolved.
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
    monkeypatch.setattr(worker_module, "SessionLocal", session_maker)
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
            evt_id = relay_evt.id

    with caplog.at_level(logging.DEBUG):
        await process_one_event(evt_id, provider)

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


# Wrapper calls that provably do NOT disclose their argument's value, so a PII
# symbol passed to one of them is safe. safe_log_value is NOT here: it escapes
# log injection but returns the value itself, so safe_log_value(phone_e164) still
# leaks the phone. Everything else (str/repr/normalize_phone/identity/…) is
# recursed into — those DO reveal the value.
_SAFE_LOG_WRAPPERS = frozenset({"bool", "len", "type"})


def _find_logger_pii_references(source: str) -> list[tuple[int, str]]:
    """Defense-in-depth check for use of known PII-bearing symbols in logger
    arguments. This is NOT full taint analysis — runtime branch tests remain the
    primary guarantee.

    It inspects every positional arg, keyword value, f-string field, attribute,
    and nested dict/list/tuple of a ``logger.<level>(...)`` call, and flags a
    reference to a PII symbol (``ast.Name.id`` or ``ast.Attribute.attr``).

    Only an explicit allowlist of value-hiding wrapper calls is trusted
    (``bool``/``len``/``type``). ``safe_log_value`` is deliberately NOT trusted
    for PII symbols — it escapes injection but returns the value. Any other call
    — including ``str(phone)`` and ``normalize_phone(phone)``, which DO reveal the
    value — is recursed into, so the argument is still checked.
    """
    import ast

    offenders: list[tuple[int, str]] = []

    def _is_safe_wrapper(call: ast.Call) -> bool:
        return isinstance(call.func, ast.Name) and call.func.id in _SAFE_LOG_WRAPPERS

    def _scan(node: ast.AST, lineno: int) -> None:
        if isinstance(node, ast.Call):
            # An allowlisted wrapper hides its argument's value → stop. Any other
            # call is NOT assumed safe: recurse into its args, keywords and func.
            if _is_safe_wrapper(node):
                return
            for arg in node.args:
                _scan(arg, lineno)
            for kw in node.keywords:
                _scan(kw.value, lineno)
            _scan(node.func, lineno)
            return
        if isinstance(node, ast.Name) and node.id in _PII_SYMBOLS:
            offenders.append((lineno, node.id))
            return
        if isinstance(node, ast.Attribute):
            if node.attr in _PII_SYMBOLS:
                offenders.append((lineno, node.attr))
                return
            _scan(node.value, lineno)
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
        'logger.info("%s", str(phone_e164))',
        'logger.info("%s", repr(phone_e164))',
        'logger.info("%s", normalize_phone(phone_e164))',
        'logger.info("%s", identity(phone_e164))',
        'logger.info("%s", repr(text))',
        'logger.info("%s", safe_log_value(phone_e164))',
        'logger.info("phone=%s", safe_log_value(phone_e164, limit=32))',
        'logger.info("x", extra={"phone": phone_e164})',
        'logger.info("x", phone=phone_e164)',
        'logger.warning("%s", [agent_name])',
        'logger.info("%s", (recipient_phone,))',
        'logger.info("%s", {"body": text})',
    ],
)
def test_ast_guard_catches_pii(snippet: str) -> None:
    assert _find_logger_pii_references(snippet), f"guard missed: {snippet}"


@pytest.mark.parametrize(
    "snippet",
    [
        'logger.info("event_id=%s", event.id)',
        'logger.info("phone_present=%s", bool(phone_e164))',
        'logger.info("text_len=%s", len(text))',
        # safe_log_value is allowed ONLY for non-PII technical ids: here it is
        # injection-escaping for conversation_id, not PII redaction.
        'logger.info("conv_id=%s", safe_log_value(conversation_id, limit=32))',
        'logger.info("error_type=%s", type(exc).__name__)',
        'logger.info("company_id=%s", company_id)',
    ],
)
def test_ast_guard_allows_only_safe_wrappers(snippet: str) -> None:
    # Only value-hiding wrappers are trusted (bool/len/type). safe_log_value is
    # trusted here only because conversation_id is not a PII symbol.
    assert _find_logger_pii_references(snippet) == [], f"guard false-positive: {snippet}"


# ---------------------------------------------------------------------------
# Operator-relay error branches: technical ids escaped, no PII/secret/injection
# ---------------------------------------------------------------------------
#
# The worker-scoped helper below asserts the worker's own logger is clean; the
# global test at the end of this module additionally proves that NO logger —
# including providers.dummy's safe_send/safe_send_template — leaks the raw
# provider exception. (The former "out of scope debt" for provider-helper
# logging is fixed: it now logs only error_type=<class name>.)

_WORKER_LOGGER = "whatsapp_inbox_worker"

HOSTILE_PNID = "PNI" + chr(0x2028) + "forged"
SECRET_ERR = "token=SECRETVAL https://secret-host/?token=SECRETVAL\nforged status=500"
PERMANENT_ERR = "template does not exist token=SECRETVAL https://secret-host/?t=SECRETVAL\nforged"


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
    monkeypatch.setattr(worker_module, "SessionLocal", session_maker)
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
            event_id = event.id
    # Drive the durable pipeline (prepare/claim/execute/finalize) via the
    # production wrapper so logs reflect the real, committed lifecycle.
    with caplog.at_level(logging.DEBUG):
        await process_one_event(event_id, provider)
    return event_id


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
    # Transient outcome is ambiguous → durable row 'unknown' (manual review),
    # circuit closed. The error kind is logged; the raw provider text is not.
    assert "send outcome unknown" in blob
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
    assert "private note sent" in blob


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
    assert "private note failed" in blob
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
    assert "send failed" in blob
    assert "error_kind=permanent" in blob
    # The failure-note Chatwoot send also raised → its class name, no raw text.
    assert "private note failed" in blob
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


# ---------------------------------------------------------------------------
# Additional runtime log branches required by the runbook guarantee
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_relay_log_circuit_pause_note_failure(session_maker, monkeypatch, caplog) -> None:
    """Circuit paused + the Chatwoot pause-note send raises → class name only."""

    async def _paused(*args, **kwargs):
        return True

    monkeypatch.setattr(worker_module, "ChatwootClient", _RaisingChatwoot)
    monkeypatch.setattr(worker_module.meta_circuit, "should_pause_meta_sends", _paused)
    provider = _CaptureProvider()

    await _run_relay(
        session_maker,
        monkeypatch,
        caplog,
        provider=provider,
        sender_id=970,
        window_open=True,
        event=_relay_event(_k="pausenote"),
    )

    blob = _assert_worker_log_clean(caplog)
    assert "private note failed" in blob
    assert "error_type=RuntimeError" in blob


@pytest.mark.asyncio
async def test_relay_log_reopen_template_success_private_note_failure(session_maker, monkeypatch, caplog) -> None:
    """Reopen template SENT, then the follow-up private note raises → class name."""
    monkeypatch.setattr(worker_module, "ChatwootClient", _RaisingChatwoot)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_closed_window_mode", "reopen_template")
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_reopen_template_name", "reopen_tpl")
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_reopen_private_note_enabled", True)
    provider = _CaptureProvider()  # send_template succeeds

    await _run_relay(
        session_maker,
        monkeypatch,
        caplog,
        provider=provider,
        sender_id=971,
        window_open=False,
        event=_relay_event(_k="reopen_ok_note_fail"),
    )

    blob = _assert_worker_log_clean(caplog)
    assert "template sent" in blob
    assert "private note failed" in blob
    assert "error_type=RuntimeError" in blob


@pytest.mark.asyncio
async def test_relay_log_native_reply_target_found(session_maker, monkeypatch, caplog) -> None:
    """Valid ids + a prior operator outbox → native target resolved log line."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    monkeypatch.setattr(worker_module, "SessionLocal", session_maker)
    provider = _CaptureProvider()
    conv_id, msg_id, reply_to = 6001, 7001, 8001
    pnid = "PNID_NATIVE"

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=972,
                    company_id=1,
                    sender_code="default",
                    phone_number_id=pnid,
                    display_phone="+49",
                    is_active=True,
                )
            )
            # Flush the sender before the FK-referencing outbox below.
            await session.flush()
            # Open window.
            session.add(
                WhatsAppEvent(
                    dedupe_key="wa:inbound:native",
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
                                            "metadata": {"phone_number_id": pnid},
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                )
            )
            # Prior operator outbox that the reply points at (Source B).
            session.add(
                OutboxMessage(
                    company_id=1,
                    sender_id=972,
                    phone_e164=PHONE,
                    template_code="operator_relay",
                    language="de",
                    body="earlier",
                    status="sent",
                    provider_message_id="wamid.PRIOR",
                    scheduled_at=datetime.now(timezone.utc),
                    sent_at=datetime.now(timezone.utc),
                    message_source="operator",
                    chatwoot_conversation_id=conv_id,
                    chatwoot_message_id=reply_to,
                )
            )
            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:native",
                status="received",
                error=None,
                query={},
                headers={},
                payload={
                    "_chatwoot_operator_relay": {
                        "recipient_phone": PHONE,
                        "text": MESSAGE_TEXT,
                        "conversation_id": conv_id,
                        "message_id": msg_id,
                        "phone_number_id": pnid,
                        "reply_to_chatwoot_message_id": reply_to,
                        "agent_name": AGENT_NAME,
                    }
                },
            )
            session.add(evt)
            await session.flush()
            evt_id = evt.id

    with caplog.at_level(logging.DEBUG):
        await process_one_event(evt_id, provider)

    assert len(provider.sent) == 1
    blob = _assert_worker_log_clean(caplog)
    assert "native reply context resolved" in blob


async def _process_event(session_maker, monkeypatch, provider, event) -> WhatsAppEvent:
    """Insert an event, run the production process_one_event wrapper, reload it."""
    monkeypatch.setattr(worker_module, "SessionLocal", session_maker)
    async with session_maker() as session:
        async with session.begin():
            session.add(event)
            await session.flush()
            event_id = event.id
    await process_one_event(event_id, provider)
    async with session_maker() as session:
        return await session.get(WhatsAppEvent, event_id)


@pytest.mark.asyncio
async def test_incoming_forward_failure_logs_are_clean(session_maker, monkeypatch, caplog) -> None:
    """WhatsApp → Chatwoot forward failure: only event_id + class name are logged."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module, "ChatwootClient", _RaisingChatwoot)
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=973,
                    company_id=1,
                    sender_code="default",
                    phone_number_id="PNID_FWD_FAIL",
                    display_phone="+49",
                    is_active=True,
                )
            )

    evt = WhatsAppEvent(
        dedupe_key="wa:inbound:fwdfail",
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
                                        "id": "wamid.fwdfail",
                                    }
                                ],
                                "contacts": [{"profile": {"name": CLIENT_NAME}}],
                                "metadata": {"phone_number_id": "PNID_FWD_FAIL"},
                            }
                        }
                    ]
                }
            ]
        },
    )

    with caplog.at_level(logging.DEBUG):
        reloaded = await _process_event(session_maker, monkeypatch, provider, evt)

    # Forward failure surfaces as a failed event (retryable), not a silent loss.
    assert reloaded.status == "failed"
    blob = _assert_worker_log_clean(caplog)
    assert "forward failed" in blob


# ---------------------------------------------------------------------------
# Routing errors: stable reason codes, no raw sender-controlled value, no
# injection in worker logs or event.error
# ---------------------------------------------------------------------------

HOSTILE_ROUTING_PNID = "UNKNOWN\n2026 INFO forged"
HOSTILE_INBOX_ID = "INBOX forged"


async def _event_error(session_maker, dedupe_key: str) -> str | None:
    async with session_maker() as session:
        result = await session.execute(select(WhatsAppEvent).where(WhatsAppEvent.dedupe_key == dedupe_key))
        evt = result.scalars().first()
        return evt.error if evt else None


@pytest.mark.asyncio
async def test_routing_sender_not_found_stable_and_clean(session_maker, monkeypatch, caplog) -> None:
    provider = _CaptureProvider()
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    # The seeded sender (HOSTILE_PNID) does not match the relay's pnid → not found.
    await _run_relay(
        session_maker,
        monkeypatch,
        caplog,
        provider=provider,
        sender_id=980,
        window_open=True,
        event=_relay_event(_k="notfound", phone_number_id=HOSTILE_ROUTING_PNID),
    )

    assert provider.sent == []
    err = await _event_error(session_maker, "chatwoot_out:branch:notfound")
    assert err == "operator_relay: sender_not_found"
    assert "forged" not in err
    _assert_worker_log_clean(caplog)


@pytest.mark.asyncio
async def test_routing_ambiguous_sender_stable_and_clean(session_maker, monkeypatch, caplog) -> None:
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module, "SessionLocal", session_maker)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            for sid, company in ((990, 11), (991, 22)):
                session.add(
                    WhatsAppSender(
                        id=sid,
                        company_id=company,
                        sender_code="x",
                        phone_number_id=HOSTILE_ROUTING_PNID,
                        display_phone="+49",
                        is_active=True,
                    )
                )
            evt = _relay_event(_k="ambig", phone_number_id=HOSTILE_ROUTING_PNID)
            session.add(evt)
            await session.flush()
            evt_id = evt.id

    with caplog.at_level(logging.DEBUG):
        await process_one_event(evt_id, provider)

    assert provider.sent == []
    err = await _event_error(session_maker, "chatwoot_out:branch:ambig")
    assert err == "operator_relay: ambiguous_sender"
    assert "forged" not in err
    _assert_worker_log_clean(caplog)


@pytest.mark.asyncio
async def test_routing_inbox_mapping_missing_stable_and_clean(session_maker, monkeypatch, caplog) -> None:
    provider = _CaptureProvider()
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    monkeypatch.setattr(worker_module.settings, "chatwoot_inbox_company_map", '{"8": 758285}')

    # A valid but unknown inbox id → mapping_missing (positive_int accepts 99).
    await _run_relay(
        session_maker,
        monkeypatch,
        caplog,
        provider=provider,
        sender_id=992,
        window_open=True,
        event=_relay_event(_k="inboxmiss", chatwoot_inbox_id=99),
    )

    assert provider.sent == []
    err = await _event_error(session_maker, "chatwoot_out:branch:inboxmiss")
    assert err == "operator_relay: inbox_mapping_missing"
    _assert_worker_log_clean(caplog)


@pytest.mark.asyncio
async def test_routing_invalid_inbox_id_stable_and_clean(session_maker, monkeypatch, caplog) -> None:
    """A configured map + hostile non-int inbox id → invalid_inbox_id, no injection."""
    provider = _CaptureProvider()
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    monkeypatch.setattr(worker_module.settings, "chatwoot_inbox_company_map", '{"8": 758285}')

    await _run_relay(
        session_maker,
        monkeypatch,
        caplog,
        provider=provider,
        sender_id=994,
        window_open=True,
        event=_relay_event(_k="badinbox", chatwoot_inbox_id=HOSTILE_INBOX_ID),
    )

    assert provider.sent == []
    err = await _event_error(session_maker, "chatwoot_out:branch:badinbox")
    assert err == "operator_relay: invalid_inbox_id"
    assert "forged" not in err
    _assert_worker_log_clean(caplog)


@pytest.mark.asyncio
async def test_routing_invalid_inbox_map_stable_and_clean(session_maker, monkeypatch, caplog) -> None:
    provider = _CaptureProvider()
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)
    monkeypatch.setattr(worker_module.settings, "chatwoot_inbox_company_map", "{not valid json")

    await _run_relay(
        session_maker,
        monkeypatch,
        caplog,
        provider=provider,
        sender_id=993,
        window_open=True,
        event=_relay_event(_k="badmap", chatwoot_inbox_id=42),
    )

    assert provider.sent == []
    err = await _event_error(session_maker, "chatwoot_out:branch:badmap")
    assert err == "operator_relay: invalid_inbox_company_map"
    blob = _assert_worker_log_clean(caplog)
    # Raw config / exception body must never appear.
    assert "not valid json" not in blob


# ---------------------------------------------------------------------------
# Global log hygiene: NO logger (incl. providers.dummy) leaks a provider secret
# ---------------------------------------------------------------------------

# A single provider error string carrying a secret, a URL, a newline-injection,
# a CR, an ANSI escape, and Unicode line/paragraph separators. It must never
# surface in ANY captured log record after a text OR a template send.
_GLOBAL_SECRET_ERR = (
    "token=SECRETVAL https://secret-host/?token=SECRETVAL\n2026-07-28 INFO forged\r\x1b[31m" + LINE_SEP + PARA_SEP
)


async def _insert_secret_relay(session_maker, *, dedupe_key, conversation_id, recipient) -> int:
    async with session_maker() as session:
        async with session.begin():
            evt = WhatsAppEvent(
                dedupe_key=dedupe_key,
                status="received",
                error=None,
                query={},
                headers={},
                payload={
                    "_chatwoot_operator_relay": {
                        "recipient_phone": recipient,
                        "text": MESSAGE_TEXT,
                        "conversation_id": conversation_id,
                        "message_id": conversation_id + 1,
                        "phone_number_id": "PNID_SECRET",
                        "agent_name": AGENT_NAME,
                    }
                },
            )
            session.add(evt)
            await session.flush()
            return int(evt.id)


@pytest.mark.asyncio
async def test_provider_secret_never_leaks_in_any_logger(session_maker, monkeypatch, caplog) -> None:
    """A secret-bearing provider exception must not appear in ANY logger — the
    worker, the provider helper, or anything else — for text and template sends."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    monkeypatch.setattr(worker_module, "SessionLocal", session_maker)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_relay_enabled", True)
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_closed_window_mode", "reopen_template")
    monkeypatch.setattr(worker_module.settings, "chatwoot_operator_reopen_template_name", "reopen_tpl")
    monkeypatch.setattr(worker_module, "ChatwootClient", _FakeChatwoot)  # unknown note → no network secret

    phone_open = "+4915111111111"
    phone_closed = "+4915122222222"
    async with session_maker() as session:
        async with session.begin():
            # One sender serves both events (same phone_number_id).
            session.add(
                WhatsAppSender(
                    id=990,
                    company_id=1,
                    sender_code="default",
                    phone_number_id="PNID_SECRET",
                    display_phone="+49",
                    is_active=True,
                )
            )
            # Open the 24h window for the text recipient only.
            session.add(
                WhatsAppEvent(
                    dedupe_key="wa:inbound:secretleak",
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
                                                    "from": phone_open.lstrip("+"),
                                                    "type": "text",
                                                    "text": {"body": "hi"},
                                                    "id": "w1",
                                                }
                                            ],
                                            "metadata": {"phone_number_id": "PNID_SECRET"},
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                )
            )

    text_event_id = await _insert_secret_relay(
        session_maker, dedupe_key="chatwoot_out:secretleak:text", conversation_id=6100, recipient=phone_open
    )
    tpl_event_id = await _insert_secret_relay(
        session_maker, dedupe_key="chatwoot_out:secretleak:tpl", conversation_id=6200, recipient=phone_closed
    )

    with caplog.at_level(logging.DEBUG):
        # Text path (window open): provider.send raises the secret-bearing error.
        await process_one_event(text_event_id, _RaisingSendProvider(_GLOBAL_SECRET_ERR))
        # Template path (window closed): send_template raises the secret-bearing error.
        await process_one_event(tpl_event_id, _RaisingTemplateProvider(_GLOBAL_SECRET_ERR))

    messages = [r.getMessage() for r in caplog.records]
    blob = "\n".join(messages)

    # No secret / URL / operator text / customer phone anywhere.
    for forbidden in ("SECRETVAL", "secret-host", "token=", "https://", MESSAGE_TEXT, PHONE):
        assert forbidden not in blob, f"leak of {forbidden!r} in a log record"

    # No raw injection bytes turning any record into a forged physical line.
    for msg in messages:
        assert "\n" not in msg
        assert "\r" not in msg
        assert "\x1b" not in msg
        assert LINE_SEP not in msg
        assert PARA_SEP not in msg

    # Positive: the provider helper logged a SAFE structured marker for both paths.
    provider_logs = [r.getMessage() for r in caplog.records if r.name == "altegio_bot.providers.dummy"]
    assert any("provider send failed error_type=RuntimeError" in m for m in provider_logs)
    assert any("provider template send failed error_type=RuntimeError" in m for m in provider_logs)

    # And the test genuinely went through the provider: both rows are 'unknown'.
    async with session_maker() as session:
        rows = (
            await session.execute(
                select(OutboxMessage).where(OutboxMessage.source_whatsapp_event_id.in_([text_event_id, tpl_event_id]))
            )
        ).scalars()
        statuses = sorted(r.status for r in rows)
    assert statuses == ["unknown", "unknown"]
