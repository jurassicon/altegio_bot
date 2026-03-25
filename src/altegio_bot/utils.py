from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Глобальное хранилище фоновых задач.
# Без него CPython GC может уничтожить task до завершения,
# если на него нет других ссылок — это реальный баг в prod.
_background_tasks: set[asyncio.Task[Any]] = set()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def fire_and_forget(coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
    """Запускает корутину фоном, не блокируя вызывающий код.

    - Держит ссылку на task в _background_tasks, пока он не завершится
      (защита от GC).
    - Логирует необработанные исключения через done-callback.
    - Возвращает Task — при желании можно отменить: task.cancel().

    Использование:
        fire_and_forget(
            chatwoot_client.sync_template_message(conv_id, wamid, text)
        )
    """
    task = asyncio.create_task(coro)
    _background_tasks.add(task)

    def _on_done(t: asyncio.Task[Any]) -> None:
        _background_tasks.discard(t)
        if not t.cancelled() and (exc := t.exception()) is not None:
            logger.error(
                "Фоновая задача завершилась с ошибкой: %s",
                exc,
                exc_info=exc,
            )

    task.add_done_callback(_on_done)
    return task
