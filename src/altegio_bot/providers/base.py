from __future__ import annotations

from typing import Protocol


class WhatsAppProvider(Protocol):
    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
    ) -> str:
        pass

    async def send_template(
        self,
        sender_id: int,
        phone_e164: str,
        template_name: str,
        language: str,
        params: list[str],
    ) -> str:
        pass
