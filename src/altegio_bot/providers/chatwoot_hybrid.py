"""Dual-write WhatsApp provider.

PRIMARY:   MetaCloudProvider  – blocking, must succeed
SECONDARY: ChatwootClient     – async, best-effort (never fails the send)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from altegio_bot.chatwoot_client import ChatwootClient
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.providers.meta_cloud import MetaCloudProvider

logger = logging.getLogger(__name__)

_ACLOSE_TIMEOUT = 3.0


class ChatwootHybridProvider:
    """Wraps MetaCloudProvider and mirrors outbound messages to Chatwoot."""

    # Signals to safe_send / safe_send_template that this provider accepts
    # company_id and staff_id keyword arguments.
    _supports_mirror_kwargs: bool = True

    def __init__(
        self,
        *,
        primary: MetaCloudProvider | None = None,
        chatwoot: ChatwootClient | None = None,
    ) -> None:
        self._primary: WhatsAppProvider = primary or MetaCloudProvider()
        self._chatwoot: ChatwootClient = chatwoot or ChatwootClient()
        self._background_tasks: set[asyncio.Task[None]] = set()

    async def aclose(self) -> None:
        if self._background_tasks:
            _, pending = await asyncio.wait(self._background_tasks, timeout=_ACLOSE_TIMEOUT)
            if pending:
                logger.warning(
                    "aclose: %d background mirror task(s) did not finish within %.1fs",
                    len(pending),
                    _ACLOSE_TIMEOUT,
                )

        aclose_primary = getattr(self._primary, "aclose", None)
        if callable(aclose_primary):
            await aclose_primary()
        await self._chatwoot.aclose()

    def _schedule_mirror(self, coro: Any) -> None:
        """Schedule a mirror coroutine as a tracked background task."""
        task: asyncio.Task[None] = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
        *,
        company_id: int = 0,
        staff_id: int | None = None,
        contact_name: str | None = None,
    ) -> str:
        # PRIMARY – must succeed (Отправка напрямую в Meta)
        msg_id = await self._primary.send(sender_id, phone_e164, text)

        # SECONDARY – best-effort (Логируем в Chatwoot как ПРИВАТНУЮ ЗАМЕТКУ)
        self._schedule_mirror(
            self._log_to_chatwoot(phone_e164, text, contact_name=contact_name, meta={"msg_id": msg_id})
        )

        return msg_id

    async def send_template(
        self,
        sender_id: int,
        phone_e164: str,
        template_name: str,
        language: str,
        params: list[str],
        fallback_text: str = "",
        *,
        company_id: int = 0,
        staff_id: int | None = None,
        contact_name: str | None = None,
    ) -> str:
        # PRIMARY – must succeed (отправка в Meta игнорирует fallback_text)
        msg_id = await self._primary.send_template(
            sender_id, phone_e164, template_name, language, params, fallback_text
        )

        # SECONDARY – best-effort (Отправляем в Chatwoot красивый сгенерированный текст)
        content = fallback_text if fallback_text else (f"[{template_name}] " + " | ".join(params))
        self._schedule_mirror(
            self._log_to_chatwoot(phone_e164, content, contact_name=contact_name, meta={"msg_id": msg_id})
        )

        return msg_id

    async def _log_to_chatwoot(
        self,
        phone_e164: str,
        content: str,
        *,
        contact_name: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        try:
            await self._chatwoot.mirror_outbound_as_note(phone_e164, content, contact_name=contact_name)
            logger.debug(
                "Chatwoot mirror ok phone=%s extra=%s",
                phone_e164,
                meta,
            )
        except Exception as exc:
            logger.warning(
                "Chatwoot log failed phone=%s err=%s extra=%s",
                phone_e164,
                exc,
                meta,
            )
