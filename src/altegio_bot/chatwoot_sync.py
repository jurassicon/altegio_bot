"""Сервис синхронизации «красивого» сообщения в Chatwoot.

Проблема: Meta нативно шлёт вебхук в Chatwoot после отправки шаблона.
Chatwoot сохраняет сырой шаблон с переменными вида {1}, {2} — это некрасиво
для агентов в UI.

Решение: подождать, пока Chatwoot обработает вебхук, найти сообщение по
wamid (source_id), удалить его и создать новое — уже с красиво
отформатированным текстом, который наш бот уже сгенерировал.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Константы
# ──────────────────────────────────────────────────────────────────────────────

# Сколько секунд ждём перед первым запросом к Chatwoot,
# чтобы Meta успела доставить свой вебхук и Chatwoot создал сообщение.
_SLEEP_MIN_SEC: float = 2.0
_SLEEP_MAX_SEC: float = 3.0

# Таймаут одного HTTP-запроса к Chatwoot API.
_HTTP_TIMEOUT_SEC: float = 15.0

# Максимальное число повторных попыток найти сообщение по wamid
# (на случай, если Chatwoot ещё не успел обработать вебхук за 2–3 с).
_MAX_FIND_RETRIES: int = 3
_RETRY_SLEEP_SEC: float = 2.0


# ──────────────────────────────────────────────────────────────────────────────
# Вспомогательные функции
# ──────────────────────────────────────────────────────────────────────────────


def _build_base_url(chatwoot_base_url: str, chatwoot_account_id: int) -> str:
    """Формирует префикс URL для всех запросов к аккаунту."""
    return f"{chatwoot_base_url.rstrip('/')}/api/v1/accounts/{chatwoot_account_id}"


def _auth_headers(chatwoot_api_token: str) -> dict[str, str]:
    return {
        "api_access_token": chatwoot_api_token,
        "Content-Type": "application/json",
    }


async def _get_messages(
    client: httpx.AsyncClient,
    base: str,
    conversation_id: int,
    headers: dict[str, str],
) -> list[dict[str, Any]]:
    """GET /conversations/{id}/messages → список сообщений.

    Chatwoot возвращает либо список напрямую, либо {'payload': [...]}.
    Оба варианта обрабатываем корректно.
    """
    url = f"{base}/conversations/{conversation_id}/messages"
    logger.debug("Запрашиваем сообщения беседы %d: GET %s", conversation_id, url)

    res = await client.get(url, headers=headers)
    res.raise_for_status()

    data = res.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        payload = data.get("payload", [])
        if isinstance(payload, list):
            return payload
        # Иногда payload — это dict с вложенным ключом messages
        if isinstance(payload, dict):
            return payload.get("messages", [])
    return []


def _find_message_by_wamid(
    messages: list[dict[str, Any]],
    wamid: str,
) -> dict[str, Any] | None:
    """Ищет сообщение, у которого source_id == wamid."""
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        if msg.get("source_id") == wamid:
            return msg
    return None


async def _delete_message(
    client: httpx.AsyncClient,
    base: str,
    conversation_id: int,
    message_id: int,
    headers: dict[str, str],
) -> bool:
    """DELETE /conversations/{conv_id}/messages/{msg_id}.

    Возвращает True при успехе, False если сервер вернул ошибку.
    Не бросает исключений — удаление «best-effort»:
    даже если удалить не получилось, новое красивое сообщение всё рав��о
    будет отправлено (агент увидит оба, что лучше, чем не увидеть ничего).
    """
    url = f"{base}/conversations/{conversation_id}/messages/{message_id}"
    logger.debug("Удаляем сырое сообщение %d в беседе %d: DELETE %s", message_id, conversation_id, url)

    try:
        res = await client.delete(url, headers=headers)
        if res.status_code in (200, 204):
            logger.info(
                "Сырое сообщение %d (беседа %d) успешно удалено.",
                message_id,
                conversation_id,
            )
            return True
        # Chatwoot возвращает 404, если сообщение уже удалено — это нормально.
        if res.status_code == 404:
            logger.warning(
                "Сообщение %d не найдено при удалении (404) — возможно, уже удалено.",
                message_id,
            )
            return True
        logger.error(
            "Не удалось удалить сообщение %d: HTTP %d — %s",
            message_id,
            res.status_code,
            res.text[:200],
        )
        return False
    except httpx.HTTPError as exc:
        logger.error("Сетевая ошибка при удалении сообщения %d: %s", message_id, exc)
        return False


async def _post_beautiful_message(
    client: httpx.AsyncClient,
    base: str,
    conversation_id: int,
    formatted_text: str,
    headers: dict[str, str],
) -> int | None:
    """POST нового исходящего сообщения с красивым текстом.

    message_type=1 — исходящее (outgoing) в числовом формате Chatwoot API.
    Возвращает ID созданного сообщения или None при ошибке.
    """
    url = f"{base}/conversations/{conversation_id}/messages"
    body: dict[str, Any] = {
        "content": formatted_text,
        "message_type": 1,  # 1 = outgoing
        "private": False,
    }
    logger.debug(
        "Публикуем красивое сообщение в беседу %d: POST %s",
        conversation_id,
        url,
    )

    res = await client.post(url, headers=headers, json=body)
    res.raise_for_status()

    data: dict[str, Any] = res.json()
    msg_id = data.get("id")
    if msg_id is None:
        logger.error(
            "Chatwoot не вернул id нового сообщения для беседы %d: %s",
            conversation_id,
            data,
        )
        return None

    logger.info(
        "Красивое сообщение успешно создано (id=%s) в беседе %d.",
        msg_id,
        conversation_id,
    )
    return int(msg_id)


# ──────────────────────────────────────────────────────────────────────────────
# Основная публичная функция
# ──────────────────────────────────────────────────────────────────────────────


async def sync_beautiful_message_to_chatwoot(
    *,
    chatwoot_base_url: str,
    chatwoot_account_id: int,
    chatwoot_api_token: str,
    conversation_id: int,
    wamid: str,
    formatted_text: str,
) -> bool:
    """Заменяет сырое шаблонное сообщение в Chatwoot на красиво отформатированный текст.

    Полный цикл:
      1. Ждём 2–3 секунды, чтобы Meta успела доставить вебхук в Chatwoot.
      2. GET всех сообщений беседы.
      3. Ищем сообщение с source_id == wamid (с повторными попытками).
      4. DELETE найденного сырого сообщения (best-effort).
      5. POST нового исходящего сообщения с formatted_text.

    Аргументы:
        chatwoot_base_url:    Базовый URL Chatwoot, напр. "https://chatwoot.example.com"
        chatwoot_account_id:  ID аккаунта в Chatwoot.
        chatwoot_api_token:   API-токен агента/бота.
        conversation_id:      ID беседы в Chatwoot.
        wamid:                ID сообщения Meta (messages[0].id), напр. "wamid.xxx".
        formatted_text:       Готовый красивый текст для замены.

    Возвращает:
        True  — всё прошло успешно (удалили + создали).
        False — что-то пошло не так (подробности в логах).

    Никогда не бросает исключений — функция предназначена для вызова через
    asyncio.create_task() без await на уровне воркера.
    """
    log_ctx = f"[беседа={conversation_id}, wamid={wamid}]"

    try:
        # ── Шаг 1: ждём, пока Chatwoot обработает входящий вебхук от Meta ──
        delay = random.uniform(_SLEEP_MIN_SEC, _SLEEP_MAX_SEC)
        logger.info("%s Ждём %.1f сек перед синхронизацией с Chatwoot...", log_ctx, delay)
        await asyncio.sleep(delay)

        base = _build_base_url(chatwoot_base_url, chatwoot_account_id)
        headers = _auth_headers(chatwoot_api_token)

        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT_SEC) as client:
            # ── Шаг 2 + 3: получаем список сообщений и ищем наш wamid ──
            raw_message: dict[str, Any] | None = None

            for attempt in range(1, _MAX_FIND_RETRIES + 1):
                try:
                    messages = await _get_messages(client, base, conversation_id, headers)
                except httpx.HTTPStatusError as exc:
                    logger.error(
                        "%s Ошибка HTTP при получении сообщений (попытка %d/%d): %s %s",
                        log_ctx,
                        attempt,
                        _MAX_FIND_RETRIES,
                        exc.response.status_code,
                        exc.response.text[:200],
                    )
                    # Если Chatwoot вернул 4xx (не 429) — повтор бессмысленен
                    if exc.response.status_code < 500 and exc.response.status_code != 429:
                        return False
                except httpx.HTTPError as exc:
                    logger.error(
                        "%s Сетевая ошибка при получении сообщений (попытка %d/%d): %s",
                        log_ctx,
                        attempt,
                        _MAX_FIND_RETRIES,
                        exc,
                    )
                else:
                    raw_message = _find_message_by_wamid(messages, wamid)
                    if raw_message is not None:
                        logger.info(
                            "%s Найдено сырое сообщение с wamid '%s' (id=%s) на попытке %d.",
                            log_ctx,
                            wamid,
                            raw_message.get("id"),
                            attempt,
                        )
                        break

                if attempt < _MAX_FIND_RETRIES:
                    logger.warning(
                        "%s Сообщение с wamid '%s' ещё не найдено, ждём %.1f сек...",
                        log_ctx,
                        wamid,
                        _RETRY_SLEEP_SEC,
                    )
                    await asyncio.sleep(_RETRY_SLEEP_SEC)

            if raw_message is None:
                logger.error(
                    "%s Сообщение с wamid '%s' не найдено после %d попыток. "
                    "Публикуем красивое сообщение без удаления сырого.",
                    log_ctx,
                    wamid,
                    _MAX_FIND_RETRIES,
                )
                # Даже если не нашли — публикуем красивый текст,
                # чтобы агент в любом случае увидел нужную информацию.
                await _post_beautiful_message(client, base, conversation_id, formatted_text, headers)
                return False

            raw_msg_id: int = int(raw_message["id"])

            # ── Шаг 4: удаляем сырое сообщение (best-effort) ──
            deleted = await _delete_message(client, base, conversation_id, raw_msg_id, headers)
            if not deleted:
                logger.warning(
                    "%s Не удалось удалить сырое сообщение %d — всё равно публикуем красивое сообщение.",
                    log_ctx,
                    raw_msg_id,
                )

            # ── Шаг 5: публикуем красивое исходящее сообщение ──
            new_msg_id = await _post_beautiful_message(client, base, conversation_id, formatted_text, headers)
            if new_msg_id is None:
                return False

            return deleted  # True только если и удалили, и создали успешно

    except Exception:
        # Верхний предохранитель: функция никогда не должна падать наружу,
        # т.к. вызывается через asyncio.create_task() без обработки исключений.
        logger.exception("%s Непредвиденная ошибка в sync_beautiful_message_to_chatwoot.", log_ctx)
        return False
