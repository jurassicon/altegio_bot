from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import respx
from sqlalchemy import select as sa_select
from sqlalchemy.ext.asyncio import async_sessionmaker

from altegio_bot.models.models import ContactRateLimit, OutboxMessage
from altegio_bot.scripts.fix_phone_e164_prefix import (
    _apply_db_fix_outbox,
    _apply_db_fix_rate_limits,
    _fetch_bad_phones,
    _find_contact,
    _process_chatwoot,
)

BASE_URL = "https://chatwoot.example.com"
ACCOUNT_ID = 1
HEADERS = {"api_access_token": "token"}


def _make_search_url() -> str:
    return f"{BASE_URL}/api/v1/accounts/{ACCOUNT_ID}/contacts/search"


@respx.mock
@pytest.mark.asyncio
async def test_find_contact_no_double_encoding() -> None:
    route = respx.get(_make_search_url()).mock(return_value=httpx.Response(200, json={"payload": []}))

    async with httpx.AsyncClient() as client:
        await _find_contact(
            client,
            HEADERS,
            ACCOUNT_ID,
            BASE_URL,
            "+4915773416635",
        )

    assert route.called
    request = route.calls[0].request
    assert "q=%2B4915773416635" in str(request.url)
    assert "%252B" not in str(request.url)


@pytest.mark.asyncio
async def test_fetch_bad_phones_uses_all_tables_and_deduplicates(
    session_maker: async_sessionmaker,
    monkeypatch: Any,
) -> None:
    now = datetime.now(timezone.utc)

    async with session_maker() as session:
        async with session.begin():
            session.add_all(
                [
                    OutboxMessage(
                        company_id=1,
                        phone_e164="4915111111111",
                        template_code="t1",
                        language="de",
                        body="msg1",
                        status="sent",
                        scheduled_at=now,
                        meta={},
                    ),
                    OutboxMessage(
                        company_id=1,
                        phone_e164="4915222222222",
                        template_code="t2",
                        language="de",
                        body="msg2",
                        status="sent",
                        scheduled_at=now,
                        meta={},
                    ),
                    ContactRateLimit(
                        phone_e164="4915111111111",
                        next_allowed_at=now,
                    ),
                    ContactRateLimit(
                        phone_e164="4915333333333",
                        next_allowed_at=now,
                    ),
                ]
            )

    monkeypatch.setattr(
        "altegio_bot.scripts.fix_phone_e164_prefix.SessionLocal",
        session_maker,
    )

    phones = await _fetch_bad_phones(limit=None)

    assert phones == [
        "4915111111111",
        "4915222222222",
        "4915333333333",
    ]


@pytest.mark.asyncio
async def test_fetch_bad_phones_respects_limit(
    session_maker: async_sessionmaker,
    monkeypatch: Any,
) -> None:
    now = datetime.now(timezone.utc)

    async with session_maker() as session:
        async with session.begin():
            session.add_all(
                [
                    OutboxMessage(
                        company_id=1,
                        phone_e164="4915000000001",
                        template_code="t1",
                        language="de",
                        body="msg1",
                        status="sent",
                        scheduled_at=now,
                        meta={},
                    ),
                    OutboxMessage(
                        company_id=1,
                        phone_e164="4915000000002",
                        template_code="t2",
                        language="de",
                        body="msg2",
                        status="sent",
                        scheduled_at=now,
                        meta={},
                    ),
                    ContactRateLimit(
                        phone_e164="4915000000003",
                        next_allowed_at=now,
                    ),
                ]
            )

    monkeypatch.setattr(
        "altegio_bot.scripts.fix_phone_e164_prefix.SessionLocal",
        session_maker,
    )

    phones = await _fetch_bad_phones(limit=2)

    assert phones == ["4915000000001", "4915000000002"]


@pytest.mark.asyncio
async def test_apply_db_fix_outbox_respects_scope(
    session_maker: async_sessionmaker,
    monkeypatch: Any,
) -> None:
    now = datetime.now(timezone.utc)

    async with session_maker() as session:
        async with session.begin():
            session.add_all(
                [
                    OutboxMessage(
                        company_id=1,
                        phone_e164="4915111111111",
                        template_code="t1",
                        language="de",
                        body="msg1",
                        status="sent",
                        scheduled_at=now,
                        meta={},
                    ),
                    OutboxMessage(
                        company_id=1,
                        phone_e164="4916222222222",
                        template_code="t2",
                        language="de",
                        body="msg2",
                        status="sent",
                        scheduled_at=now,
                        meta={},
                    ),
                ]
            )

    monkeypatch.setattr(
        "altegio_bot.scripts.fix_phone_e164_prefix.SessionLocal",
        session_maker,
    )

    count = await _apply_db_fix_outbox(["4915111111111"])
    assert count == 1

    async with session_maker() as session:
        rows = (await session.execute(sa_select(OutboxMessage.phone_e164))).scalars().all()

    phones = set(rows)
    assert "+4915111111111" in phones
    assert "4916222222222" in phones


@pytest.mark.asyncio
async def test_apply_db_fix_rate_limits_updates_plain_phone(
    session_maker: async_sessionmaker,
    monkeypatch: Any,
) -> None:
    now = datetime.now(timezone.utc)

    async with session_maker() as session:
        async with session.begin():
            session.add(
                ContactRateLimit(
                    phone_e164="4915333333333",
                    next_allowed_at=now,
                )
            )

    monkeypatch.setattr(
        "altegio_bot.scripts.fix_phone_e164_prefix.SessionLocal",
        session_maker,
    )

    count = await _apply_db_fix_rate_limits(["4915333333333"])
    assert count == 1

    async with session_maker() as session:
        rows = (await session.execute(sa_select(ContactRateLimit.phone_e164))).scalars().all()

    assert rows == ["+4915333333333"]


@pytest.mark.asyncio
async def test_apply_db_fix_rate_limits_merges_duplicate_pair(
    session_maker: async_sessionmaker,
    monkeypatch: Any,
) -> None:
    early = datetime.now(timezone.utc)
    late = early + timedelta(hours=2)

    async with session_maker() as session:
        async with session.begin():
            session.add_all(
                [
                    ContactRateLimit(
                        phone_e164="4915444444444",
                        next_allowed_at=late,
                    ),
                    ContactRateLimit(
                        phone_e164="+4915444444444",
                        next_allowed_at=early,
                    ),
                ]
            )

    monkeypatch.setattr(
        "altegio_bot.scripts.fix_phone_e164_prefix.SessionLocal",
        session_maker,
    )

    count = await _apply_db_fix_rate_limits(["4915444444444"])
    assert count == 1

    async with session_maker() as session:
        rows = (
            await session.execute(
                sa_select(
                    ContactRateLimit.phone_e164,
                    ContactRateLimit.next_allowed_at,
                )
            )
        ).all()

    assert len(rows) == 1
    assert rows[0][0] == "+4915444444444"
    assert rows[0][1] == late


def _mock_find(
    with_plus: dict[str, Any] | None,
    without_plus: dict[str, Any] | None,
) -> Any:
    async def _side_effect(
        client: Any,
        headers: Any,
        account_id: Any,
        base_url: Any,
        phone: str,
    ) -> dict[str, Any] | None:
        if phone.startswith("+"):
            return with_plus
        return without_plus

    return _side_effect


@pytest.mark.asyncio
async def test_process_chatwoot_manual_review_blocks_merge(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        "altegio_bot.settings.settings.chatwoot_enabled",
        True,
    )
    monkeypatch.setattr(
        "altegio_bot.settings.settings.chatwoot_account_id",
        ACCOUNT_ID,
    )
    monkeypatch.setattr(
        "altegio_bot.settings.settings.chatwoot_base_url",
        BASE_URL,
    )
    monkeypatch.setattr(
        "altegio_bot.settings.settings.chatwoot_api_token",
        "token",
    )

    contact_with = {"id": 100, "phone_number": "+4915773416635"}
    contact_without = {"id": 200, "phone_number": "4915773416635"}
    merge_mock = AsyncMock()

    with (
        patch(
            "altegio_bot.scripts.fix_phone_e164_prefix._find_contact",
            side_effect=_mock_find(contact_with, contact_without),
        ),
        patch(
            "altegio_bot.scripts.fix_phone_e164_prefix._get_contact_conversations",
            AsyncMock(return_value=[{"id": 7, "status": "open"}]),
        ),
        patch(
            "altegio_bot.scripts.fix_phone_e164_prefix._merge_contacts",
            merge_mock,
        ),
        patch(
            "altegio_bot.scripts.fix_phone_e164_prefix._update_contact_phone",
            AsyncMock(),
        ),
        patch("asyncio.sleep", AsyncMock()),
    ):
        stats = await _process_chatwoot(
            ["4915773416635"],
            dry_run=False,
            delay=0,
        )

    assert stats["manual_review"] == 1
    assert stats["merged"] == 0
    merge_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_chatwoot_updates_only_without_plus(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        "altegio_bot.settings.settings.chatwoot_enabled",
        True,
    )
    monkeypatch.setattr(
        "altegio_bot.settings.settings.chatwoot_account_id",
        ACCOUNT_ID,
    )
    monkeypatch.setattr(
        "altegio_bot.settings.settings.chatwoot_base_url",
        BASE_URL,
    )
    monkeypatch.setattr(
        "altegio_bot.settings.settings.chatwoot_api_token",
        "token",
    )

    contact_without = {"id": 200, "phone_number": "4915773416635"}
    update_mock = AsyncMock()

    with (
        patch(
            "altegio_bot.scripts.fix_phone_e164_prefix._find_contact",
            side_effect=_mock_find(None, contact_without),
        ),
        patch(
            "altegio_bot.scripts.fix_phone_e164_prefix._merge_contacts",
            AsyncMock(),
        ),
        patch(
            "altegio_bot.scripts.fix_phone_e164_prefix._update_contact_phone",
            update_mock,
        ),
        patch("asyncio.sleep", AsyncMock()),
    ):
        stats = await _process_chatwoot(
            ["4915773416635"],
            dry_run=False,
            delay=0,
        )

    assert stats["updated"] == 1
    assert stats["merged"] == 0
    update_mock.assert_awaited_once()
    _, kwargs = update_mock.call_args
    assert kwargs["phone"] == "+4915773416635"


@pytest.mark.asyncio
async def test_process_chatwoot_dry_run_does_not_write(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        "altegio_bot.settings.settings.chatwoot_enabled",
        True,
    )
    monkeypatch.setattr(
        "altegio_bot.settings.settings.chatwoot_account_id",
        ACCOUNT_ID,
    )
    monkeypatch.setattr(
        "altegio_bot.settings.settings.chatwoot_base_url",
        BASE_URL,
    )
    monkeypatch.setattr(
        "altegio_bot.settings.settings.chatwoot_api_token",
        "token",
    )

    contact_with = {"id": 100, "phone_number": "+4915773416635"}
    contact_without = {"id": 200, "phone_number": "4915773416635"}
    merge_mock = AsyncMock()
    update_mock = AsyncMock()

    with (
        patch(
            "altegio_bot.scripts.fix_phone_e164_prefix._find_contact",
            side_effect=_mock_find(contact_with, contact_without),
        ),
        patch(
            "altegio_bot.scripts.fix_phone_e164_prefix._get_contact_conversations",
            AsyncMock(return_value=[]),
        ),
        patch(
            "altegio_bot.scripts.fix_phone_e164_prefix._merge_contacts",
            merge_mock,
        ),
        patch(
            "altegio_bot.scripts.fix_phone_e164_prefix._update_contact_phone",
            update_mock,
        ),
        patch("asyncio.sleep", AsyncMock()),
    ):
        stats = await _process_chatwoot(
            ["4915773416635"],
            dry_run=True,
            delay=0,
        )

    assert stats["merged"] == 1
    merge_mock.assert_not_awaited()
    update_mock.assert_not_awaited()
