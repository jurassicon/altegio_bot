"""Dual-write WhatsApp provider.

PRIMARY:   MetaCloudProvider  – blocking, must succeed
SECONDARY: ChatwootClient     – async, best-effort (never fails the send)
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from altegio_bot.chatwoot_client import ChatwootClient
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.providers.meta_cloud import MetaCloudProvider
from altegio_bot.settings import settings
from altegio_bot.webhooks.common import InboxCompanyMap, parse_chatwoot_inbox_company_map, positive_int

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
        chatwoot_factory: Callable[[int], ChatwootClient] | None = None,
    ) -> None:
        self._primary: WhatsAppProvider = primary or MetaCloudProvider()
        # The legacy client targets the same global General/Unassigned inbox as
        # inbound code (which owns a separate client outside this provider), and
        # preserves single-inbox behavior when the map is empty.
        self._chatwoot: ChatwootClient = chatwoot or ChatwootClient()
        self._chatwoot_factory = chatwoot_factory or (lambda inbox_id: ChatwootClient(inbox_id=inbox_id))
        self._inbox_company_map: InboxCompanyMap = parse_chatwoot_inbox_company_map(settings.chatwoot_inbox_company_map)
        # Lazy and bounded: at most one client per validated configured inbox.
        # The configuration snapshot is immutable for this provider lifetime;
        # env changes require the documented worker recreation.
        self._chatwoot_clients: dict[int, ChatwootClient] = {}
        self._background_tasks: set[asyncio.Task[None]] = set()

    async def aclose(self) -> None:
        if self._background_tasks:
            _, pending = await asyncio.wait(self._background_tasks, timeout=_ACLOSE_TIMEOUT)
            if pending:
                logger.warning(
                    "aclose: %d background mirror task(s) did not finish within %.1fs; cancelling",
                    len(pending),
                    _ACLOSE_TIMEOUT,
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        aclose_primary = getattr(self._primary, "aclose", None)
        if callable(aclose_primary):
            await aclose_primary()

        # A test factory may deliberately return the same object for multiple
        # inboxes. Close by identity so no underlying AsyncClient is closed
        # twice; the legacy client follows the same rule.
        seen: set[int] = set()
        for client in (self._chatwoot, *self._chatwoot_clients.values()):
            identity = id(client)
            if identity in seen:
                continue
            seen.add(identity)
            await client.aclose()

    def _schedule_mirror(self, coro: Any) -> None:
        """Schedule a mirror coroutine as a tracked background task."""
        task: asyncio.Task[None] = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def check_metadata(self, phone_number_id: str, *, timeout: float | None = None) -> None:
        check = getattr(self._primary, "check_metadata", None)
        if check is None:
            raise RuntimeError(f"primary provider {type(self._primary).__name__} does not support check_metadata")
        await check(phone_number_id, timeout=timeout)

    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
        *,
        company_id: int = 0,
        staff_id: int | None = None,
        contact_name: str | None = None,
        reply_to_provider_message_id: str | None = None,
    ) -> str:
        # PRIMARY – must succeed (Отправка напрямую в Meta)
        kwargs: dict[str, object] = {}
        if isinstance(reply_to_provider_message_id, str) and reply_to_provider_message_id.strip():
            kwargs["reply_to_provider_message_id"] = reply_to_provider_message_id.strip()
        msg_id = await self._primary.send(
            sender_id,
            phone_e164,
            text,
            **kwargs,
        )

        # SECONDARY – best-effort (Логируем в Chatwoot как ПРИВАТНУЮ ЗАМЕТКУ)
        self._schedule_mirror(
            self._log_to_chatwoot(
                phone_e164,
                text,
                company_id=company_id,
                contact_name=contact_name,
                meta={"msg_id": msg_id},
            )
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
        header_image_url: str | None = None,
    ) -> str:
        # PRIMARY – must succeed (отправка в Meta игнорирует fallback_text)
        msg_id = await self._primary.send_template(
            sender_id,
            phone_e164,
            template_name,
            language,
            params,
            fallback_text,
            header_image_url=header_image_url,
        )

        # SECONDARY – best-effort (Отправляем в Chatwoot красивый сгенерированный текст)
        content = fallback_text if fallback_text else (f"[{template_name}] " + " | ".join(params))
        self._schedule_mirror(
            self._log_to_chatwoot(
                phone_e164,
                content,
                company_id=company_id,
                contact_name=contact_name,
                meta={"msg_id": msg_id},
            )
        )

        return msg_id

    async def _log_to_chatwoot(
        self,
        phone_e164: str,
        content: str,
        *,
        company_id: int = 0,
        contact_name: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        chatwoot, inbox_id, routing_error = self._chatwoot_for_company(company_id)
        if chatwoot is None:
            # Stable reason only: never log the raw map, phone, text, contact,
            # provider response or any other customer data.
            logger.warning("Chatwoot mirror skipped routing_reason=%s", routing_error)
            return

        try:
            await chatwoot.mirror_outbound_as_note(phone_e164, content, contact_name=contact_name)
            logger.debug(
                "Chatwoot mirror ok company_id=%s inbox_id=%s",
                company_id,
                inbox_id,
            )
        except Exception as exc:
            logger.warning(
                "Chatwoot log failed company_id=%s inbox_id=%s error_type=%s",
                company_id,
                inbox_id,
                type(exc).__name__,
            )

    def _chatwoot_for_company(self, company_id: object) -> tuple[ChatwootClient | None, int | None, str | None]:
        """Resolve the outbound mirror client without guessing or mutable state."""
        parsed = self._inbox_company_map
        if not parsed.configured:
            return self._chatwoot, getattr(self._chatwoot, "_inbox_id", None), None
        if not parsed.valid:
            return None, None, "invalid_inbox_company_map"

        canonical_company_id = positive_int(company_id)
        if canonical_company_id is None:
            return None, None, "invalid_company_id"
        inbox_id = parsed.inverse_mapping.get(canonical_company_id)
        if inbox_id is None:
            return None, None, "company_mapping_missing"

        client = self._chatwoot_clients.get(inbox_id)
        if client is None:
            try:
                client = self._chatwoot_factory(inbox_id)
            except Exception:
                return None, inbox_id, "chatwoot_client_init_failed"
            self._chatwoot_clients[inbox_id] = client
        return client, inbox_id, None
