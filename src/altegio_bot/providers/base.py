from __future__ import annotations

from typing import Protocol


class WhatsAppProvider(Protocol):
    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
        *,
        contact_name: str | None = None,
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
    ) -> str:
        pass
