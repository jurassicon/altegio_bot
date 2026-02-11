from __future__ import annotations

import logging
from uuid import uuid4

from altegio_bot.providers import meta_cloud
from altegio_bot.providers.base import WhatsAppProvider

logger = logging.getLogger(__name__)


class DummyProvider(WhatsAppProvider):
    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
    ) -> tuple[None, str] | str:
        provider_message_id = f'dummy-{uuid4()}'
        if WhatsAppProvider == meta_cloud and ALLOW_REAL_SEND != '1':
            return None, str('Real send disabled')
        logger.info(
            'Dummy send sender_id=%s phone=%s text_len=%s msg_id=%s',
            sender_id,
            phone_e164,
            len(text),
            provider_message_id,
        )
        return provider_message_id


async def safe_send(
    provider: WhatsAppProvider,
    sender_id: int,
    phone: str,
    text: str,
) -> tuple[str | None, str | None]:
    try:
        msg_id = await provider.send(sender_id, phone, text)
        return msg_id, None
    except Exception as exc:
        logger.exception('send failed: %s', exc)
        return None, str(exc)