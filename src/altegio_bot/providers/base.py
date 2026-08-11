from __future__ import annotations

from enum import StrEnum
from typing import Protocol


class ChatwootRoute(StrEnum):
    """Explicit outbound Chatwoot mirror routing intent."""

    TENANT = "tenant"
    GENERAL = "general"


class WhatsAppProvider(Protocol):
    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
        *,
        contact_name: str | None = None,
        reply_to_provider_message_id: str | None = None,
    ) -> str:
        pass

    async def send_template(
        self,
        sender_id: int,
        phone_e164: str,
        template_name: str,
        language: str,
        params: list[str],
        fallback_text: str = "",
        *,
        contact_name: str | None = None,
        header_image_url: str | None = None,
    ) -> str:
        pass
