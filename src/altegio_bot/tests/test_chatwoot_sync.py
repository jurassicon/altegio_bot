"""Тесты для sync_beautiful_message_to_chatwoot."""

from __future__ import annotations

import pytest
import respx
from httpx import Response

from altegio_bot.chatwoot_sync import sync_beautiful_message_to_chatwoot

# ──────────────────────────────────────────────────────────────────────────────
# Фикстуры и константы
# ──────────────────────────────────────────────────────────────────────────────

BASE_URL = "https://chatwoot.example.com"
ACCOUNT_ID = 1
TOKEN = "test-token"
CONV_ID = 42
WAMID = "wamid.HBgNNzg5OTk5OTk5OTk5OTk5ABEIARoYFDM0NTY3ODkwMTIz"
FORMATTED = "Привет, Иван! Ваш визит 25 марта в 14:00. Ждём вас!"

MESSAGES_URL = f"{BASE_URL}/api/v1/accounts/{ACCOUNT_ID}/conversations/{CONV_ID}/messages"
DELETE_URL = f"{MESSAGES_URL}/99"


async def _noop_sleep(_: float) -> None:
    """Корректная async-замена asyncio.sleep для Python 3.12+."""
    return


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Заменяем asyncio.sleep на мгновенный no-op, чтобы тесты не ждали 2–3 секунды."""
    import altegio_bot.chatwoot_sync as module

    monkeypatch.setattr(module.asyncio, "sleep", _noop_sleep)


def _call(**kwargs):
    """Сокращение для вызова тестируемой функции с базовыми параметрами."""
    defaults = dict(
        chatwoot_base_url=BASE_URL,
        chatwoot_account_id=ACCOUNT_ID,
        chatwoot_api_token=TOKEN,
        conversation_id=CONV_ID,
        wamid=WAMID,
        formatted_text=FORMATTED,
    )
    defaults.update(kwargs)
    return sync_beautiful_message_to_chatwoot(**defaults)


# ──────────────────────────────────────────────────────────────────────────────
# Тесты
# ──────────────────────────────────────────────────────────────────────────────


@respx.mock
@pytest.mark.asyncio
async def test_happy_path():
    """Успешный сценарий: нашли сообщение, удалили, создали новое."""
    respx.get(MESSAGES_URL).mock(
        return_value=Response(
            200,
            json={"payload": [{"id": 99, "source_id": WAMID, "content": "Шаблон {1} {2}"}]},
        )
    )
    respx.delete(DELETE_URL).mock(return_value=Response(200, json={"success": True}))
    respx.post(MESSAGES_URL).mock(return_value=Response(200, json={"id": 100, "content": FORMATTED}))

    result = await _call()

    assert result is True
    assert respx.calls.call_count == 3


@respx.mock
@pytest.mark.asyncio
async def test_message_not_found_still_posts():
    """Если wamid не найден после всех попыток — всё равно публикуем красивое сообщение."""
    respx.get(MESSAGES_URL).mock(return_value=Response(200, json={"payload": []}))
    respx.post(MESSAGES_URL).mock(return_value=Response(200, json={"id": 101, "content": FORMATTED}))

    result = await _call()

    # Вернули False (не нашли wamid), но POST всё равно был сделан
    assert result is False
    # 3 попытки GET + 1 POST
    assert respx.calls.call_count == 4


@respx.mock
@pytest.mark.asyncio
async def test_delete_fails_but_post_still_happens():
    """Если DELETE упал — всё равно публикуем красивое сообщение (best-effort)."""
    respx.get(MESSAGES_URL).mock(
        return_value=Response(
            200,
            json={"payload": [{"id": 99, "source_id": WAMID, "content": "Шаблон {1}"}]},
        )
    )
    # DELETE вернул 500
    respx.delete(DELETE_URL).mock(return_value=Response(500, text="Internal Server Error"))
    respx.post(MESSAGES_URL).mock(return_value=Response(200, json={"id": 102, "content": FORMATTED}))

    result = await _call()

    # False: удалить не получилось
    assert result is False
    # GET + DELETE + POST — все три вызова были
    assert respx.calls.call_count == 3


@respx.mock
@pytest.mark.asyncio
async def test_get_messages_returns_http_error():
    """Если GET вернул 401 — прекращаем попытки, возвращаем False."""
    respx.get(MESSAGES_URL).mock(return_value=Response(401, json={"error": "Unauthorized"}))

    result = await _call()

    assert result is False
    # Только одна попытка GET — после 4xx не повторяем
    assert respx.calls.call_count == 1


@respx.mock
@pytest.mark.asyncio
async def test_message_found_on_second_attempt():
    """Сообщение появляется только на второй попытке GET."""
    call_count = 0

    def messages_side_effect(request):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # Первая попытка — пусто
            return Response(200, json={"payload": []})
        # Вторая попытка — есть
        return Response(
            200,
            json={"payload": [{"id": 99, "source_id": WAMID, "content": "Шаблон {1}"}]},
        )

    respx.get(MESSAGES_URL).mock(side_effect=messages_side_effect)
    respx.delete(DELETE_URL).mock(return_value=Response(204))
    respx.post(MESSAGES_URL).mock(return_value=Response(200, json={"id": 103, "content": FORMATTED}))

    result = await _call()

    assert result is True
    # 2 попытки GET + 1 DELETE + 1 POST
    assert respx.calls.call_count == 4


@respx.mock
@pytest.mark.asyncio
async def test_message_payload_as_direct_list():
    """Chatwoot иногда возвращает список напрямую, без обёртки payload."""
    respx.get(MESSAGES_URL).mock(
        return_value=Response(
            200,
            json=[{"id": 99, "source_id": WAMID, "content": "Шаблон"}],
        )
    )
    respx.delete(DELETE_URL).mock(return_value=Response(200, json={}))
    respx.post(MESSAGES_URL).mock(return_value=Response(200, json={"id": 104, "content": FORMATTED}))

    result = await _call()

    assert result is True
