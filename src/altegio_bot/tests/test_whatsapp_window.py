"""Tests for whatsapp_window — 24h customer service window detection.

Covers:
 1. Meta-origin inbound within 24h → window open.
 2. No inbound → window closed.
 3. Boundary: exactly 24h → still open.
 4. Boundary: 23h59m → open.
 5. Boundary: 24h+1s → closed.
 6. Chatwoot-origin events excluded by each marker.
 7. Operator relay events excluded.
 8. Events from a different phone don't count.
 9. Events older than 26h excluded (performance guard / time-bound query).
10. Phone normalization: caller without '+' matches inbound with '+'.
11. Settings validation: invalid closed_window_mode, invalid param_mode,
    reopen_template + empty name, defaults.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from altegio_bot.models.models import WhatsAppEvent
from altegio_bot.whatsapp_window import (
    get_last_meta_inbound_at,
    is_whatsapp_customer_window_open,
)

# ---------------------------------------------------------------------------
# Test fixtures / helpers
# ---------------------------------------------------------------------------

PHONE = "+49111222333"
OTHER_PHONE = "+49999888777"

NOW = datetime.now(timezone.utc)


def _meta_inbound_payload(phone: str, text: str = "Hallo") -> dict[str, Any]:
    """Minimal Meta-origin inbound payload from the given phone."""
    return {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "messages": [
                                {
                                    "from": phone,
                                    "type": "text",
                                    "text": {"body": text},
                                    "id": "wamid.TEST",
                                    "timestamp": "1700000000",
                                }
                            ],
                            "metadata": {"phone_number_id": "PNID_WIN_TEST"},
                        }
                    }
                ]
            }
        ]
    }


def _make_event(
    *,
    received_at: datetime,
    payload: dict[str, Any],
    dedupe_key: str = "meta:win:001",
    chatwoot_conversation_id: int | None = None,
) -> WhatsAppEvent:
    return WhatsAppEvent(
        dedupe_key=dedupe_key,
        received_at=received_at,
        status="processed",
        query={},
        headers={},
        payload=payload,
        chatwoot_conversation_id=chatwoot_conversation_id,
    )


# ---------------------------------------------------------------------------
# Tests: window open / closed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_meta_inbound_within_1h_opens_window(session_maker) -> None:
    """Meta-origin inbound 1h ago → window open."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                _make_event(
                    received_at=NOW - timedelta(hours=1),
                    payload=_meta_inbound_payload(PHONE),
                    dedupe_key="meta:win:1h",
                )
            )

        window_open, last_inbound = await is_whatsapp_customer_window_open(session, PHONE, NOW)

    assert window_open is True
    assert last_inbound is not None


@pytest.mark.asyncio
async def test_no_inbound_window_closed(session_maker) -> None:
    """No inbound events at all → window closed."""
    async with session_maker() as session:
        window_open, last_inbound = await is_whatsapp_customer_window_open(session, PHONE, NOW)

    assert window_open is False
    assert last_inbound is None


# ---------------------------------------------------------------------------
# Tests: boundary conditions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_boundary_exactly_24h_is_open(session_maker) -> None:
    """Inbound exactly 24h ago is still within the window (inclusive)."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                _make_event(
                    received_at=NOW - timedelta(hours=24),
                    payload=_meta_inbound_payload(PHONE),
                    dedupe_key="meta:win:24h",
                )
            )

        window_open, _ = await is_whatsapp_customer_window_open(session, PHONE, NOW)

    assert window_open is True


@pytest.mark.asyncio
async def test_boundary_23h59m_is_open(session_maker) -> None:
    """Inbound 23h 59m ago → window open."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                _make_event(
                    received_at=NOW - timedelta(hours=23, minutes=59),
                    payload=_meta_inbound_payload(PHONE),
                    dedupe_key="meta:win:23h59",
                )
            )

        window_open, _ = await is_whatsapp_customer_window_open(session, PHONE, NOW)

    assert window_open is True


@pytest.mark.asyncio
async def test_boundary_24h_plus_1s_is_closed(session_maker) -> None:
    """Inbound 24h+1s ago → window closed."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                _make_event(
                    received_at=NOW - timedelta(hours=24, seconds=1),
                    payload=_meta_inbound_payload(PHONE),
                    dedupe_key="meta:win:24h1s",
                )
            )

        window_open, _ = await is_whatsapp_customer_window_open(session, PHONE, NOW)

    assert window_open is False


# ---------------------------------------------------------------------------
# Tests: Chatwoot-origin exclusion
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_payload_with_chatwoot_key_not_counted(session_maker) -> None:
    """Event with '_chatwoot' in payload does not count as Meta inbound."""
    async with session_maker() as session:
        async with session.begin():
            payload = _meta_inbound_payload(PHONE)
            payload["_chatwoot"] = {"conversation_id": 1}
            session.add(
                _make_event(
                    received_at=NOW - timedelta(hours=1),
                    payload=payload,
                    dedupe_key="cw:key:001",
                )
            )

        window_open, _ = await is_whatsapp_customer_window_open(session, PHONE, NOW)

    assert window_open is False


@pytest.mark.asyncio
async def test_payload_with_operator_relay_key_not_counted(session_maker) -> None:
    """Event with '_chatwoot_operator_relay' in payload does not count."""
    async with session_maker() as session:
        async with session.begin():
            payload = {
                "_chatwoot_operator_relay": {"recipient_phone": PHONE, "text": "Hi"},
                "entry": [
                    {"changes": [{"value": {"messages": [{"from": PHONE, "type": "text", "text": {"body": "x"}}]}}]}
                ],
            }
            session.add(
                _make_event(
                    received_at=NOW - timedelta(hours=1),
                    payload=payload,
                    dedupe_key="cw:relay:001",
                )
            )

        window_open, _ = await is_whatsapp_customer_window_open(session, PHONE, NOW)

    assert window_open is False


@pytest.mark.asyncio
async def test_dedupe_key_starting_with_chatwoot_not_counted(session_maker) -> None:
    """Event with dedupe_key starting with 'chatwoot:' does not count."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                _make_event(
                    received_at=NOW - timedelta(hours=1),
                    payload=_meta_inbound_payload(PHONE),
                    dedupe_key="chatwoot:99:1",
                )
            )

        window_open, _ = await is_whatsapp_customer_window_open(session, PHONE, NOW)

    assert window_open is False


@pytest.mark.asyncio
async def test_chatwoot_conversation_id_set_not_counted(session_maker) -> None:
    """Event with chatwoot_conversation_id set does not count."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                _make_event(
                    received_at=NOW - timedelta(hours=1),
                    payload=_meta_inbound_payload(PHONE),
                    dedupe_key="meta:cw_conv:001",
                    chatwoot_conversation_id=99,
                )
            )

        window_open, _ = await is_whatsapp_customer_window_open(session, PHONE, NOW)

    assert window_open is False


# ---------------------------------------------------------------------------
# Tests: phone matching
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_inbound_from_different_phone_not_counted(session_maker) -> None:
    """Inbound from a different phone does not open the window for our phone."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                _make_event(
                    received_at=NOW - timedelta(hours=1),
                    payload=_meta_inbound_payload(OTHER_PHONE),
                    dedupe_key="meta:other_phone:001",
                )
            )

        window_open, _ = await is_whatsapp_customer_window_open(session, PHONE, NOW)

    assert window_open is False


# ---------------------------------------------------------------------------
# Tests: phone normalization
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_phone_normalization_caller_without_plus(session_maker) -> None:
    """Query phone without '+' must match inbound whose 'from' has '+'."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                _make_event(
                    received_at=NOW - timedelta(hours=1),
                    payload=_meta_inbound_payload("+49111222333"),  # 'from' has '+'
                    dedupe_key="meta:norm:no_plus",
                )
            )

        # Query without '+' — _norm_phone normalises it to "+49111222333".
        last_inbound = await get_last_meta_inbound_at(session, "49111222333", NOW)

    assert last_inbound is not None


@pytest.mark.asyncio
async def test_phone_normalization_invalid_phone_returns_none(session_maker) -> None:
    """get_last_meta_inbound_at returns None immediately for an empty phone."""
    async with session_maker() as session:
        last_inbound = await get_last_meta_inbound_at(session, "", NOW)

    assert last_inbound is None


# ---------------------------------------------------------------------------
# Test: performance guard (26h lookback)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_event_older_than_26h_excluded(session_maker) -> None:
    """Events older than 26h are excluded — the query is time-bounded."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                _make_event(
                    received_at=NOW - timedelta(hours=27),
                    payload=_meta_inbound_payload(PHONE),
                    dedupe_key="meta:old:27h",
                )
            )

        last_inbound = await get_last_meta_inbound_at(session, PHONE, NOW)

    assert last_inbound is None


@pytest.mark.asyncio
async def test_event_at_exactly_26h_cutoff_is_included(session_maker) -> None:
    """Events at exactly 26h ago are still within the query window."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                _make_event(
                    received_at=NOW - timedelta(hours=26),
                    payload=_meta_inbound_payload(PHONE),
                    dedupe_key="meta:cutoff:26h",
                )
            )

        last_inbound = await get_last_meta_inbound_at(session, PHONE, NOW)

    # Present in query result, but the 24h window check will say closed
    assert last_inbound is not None


# ---------------------------------------------------------------------------
# Tests: settings validation
# ---------------------------------------------------------------------------

_REOPEN_ENV_NAMES = (
    "CHATWOOT_OPERATOR_CLOSED_WINDOW_MODE",
    "CHATWOOT_OPERATOR_REOPEN_TEMPLATE_NAME",
    "CHATWOOT_OPERATOR_REOPEN_TEMPLATE_LANGUAGE",
    "CHATWOOT_OPERATOR_REOPEN_TEMPLATE_PARAM_MODE",
    "CHATWOOT_OPERATOR_REOPEN_PRIVATE_NOTE_ENABLED",
)

_SETTINGS_BASE = {
    "database_url": "sqlite+aiosqlite:///:memory:",
    "altegio_webhook_secret": "test",
}


def _clear_reopen_env(monkeypatch) -> None:
    for name in _REOPEN_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def test_settings_invalid_closed_window_mode(monkeypatch) -> None:
    """Invalid closed_window_mode raises ValidationError."""
    from pydantic import ValidationError

    from altegio_bot.settings import Settings

    _clear_reopen_env(monkeypatch)
    monkeypatch.setenv("CHATWOOT_OPERATOR_CLOSED_WINDOW_MODE", "bad_mode")
    with pytest.raises(ValidationError, match="closed_window_mode"):
        Settings(_env_file=None, **_SETTINGS_BASE)


def test_settings_invalid_param_mode(monkeypatch) -> None:
    """Invalid param_mode raises a ValidationError at settings construction."""
    from pydantic import ValidationError

    from altegio_bot.settings import Settings

    _clear_reopen_env(monkeypatch)
    monkeypatch.setenv("CHATWOOT_OPERATOR_REOPEN_TEMPLATE_PARAM_MODE", "bad_mode")
    with pytest.raises(ValidationError, match="param_mode"):
        Settings(_env_file=None, **_SETTINGS_BASE)


def test_settings_reopen_template_mode_with_empty_name(monkeypatch) -> None:
    """mode=reopen_template without a template name raises ValidationError."""
    from pydantic import ValidationError

    from altegio_bot.settings import Settings

    _clear_reopen_env(monkeypatch)
    monkeypatch.setenv("CHATWOOT_OPERATOR_CLOSED_WINDOW_MODE", "reopen_template")
    monkeypatch.setenv("CHATWOOT_OPERATOR_REOPEN_TEMPLATE_NAME", "")
    with pytest.raises(ValidationError, match="TEMPLATE_NAME"):
        Settings(_env_file=None, **_SETTINGS_BASE)


def test_settings_defaults_preserve_backward_compat(monkeypatch) -> None:
    """Default settings use private_note_only (safe, backward compatible)."""
    from altegio_bot.settings import Settings

    _clear_reopen_env(monkeypatch)
    s = Settings(_env_file=None, **_SETTINGS_BASE)
    assert s.chatwoot_operator_closed_window_mode == "private_note_only"
    assert s.chatwoot_operator_reopen_template_name == ""
    assert s.chatwoot_operator_reopen_template_language == "de"
    assert s.chatwoot_operator_reopen_template_param_mode == "contact_name"
    assert s.chatwoot_operator_reopen_private_note_enabled is True


def test_settings_valid_closed_window_modes(monkeypatch) -> None:
    """All allowed closed_window_mode values are accepted without error."""
    from altegio_bot.settings import Settings

    for mode in ("private_note_only",):
        _clear_reopen_env(monkeypatch)
        s = Settings(_env_file=None, chatwoot_operator_closed_window_mode=mode, **_SETTINGS_BASE)
        assert s.chatwoot_operator_closed_window_mode == mode

    # reopen_template requires a template name.
    _clear_reopen_env(monkeypatch)
    s = Settings(
        _env_file=None,
        chatwoot_operator_closed_window_mode="reopen_template",
        chatwoot_operator_reopen_template_name="my_template",
        **_SETTINGS_BASE,
    )
    assert s.chatwoot_operator_closed_window_mode == "reopen_template"


def test_settings_valid_param_modes(monkeypatch) -> None:
    """All allowed param_mode values are accepted without error."""
    from altegio_bot.settings import Settings

    for mode in ("none", "contact_name"):
        _clear_reopen_env(monkeypatch)
        s = Settings(_env_file=None, chatwoot_operator_reopen_template_param_mode=mode, **_SETTINGS_BASE)
        assert s.chatwoot_operator_reopen_template_param_mode == mode
