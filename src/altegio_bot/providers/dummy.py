from __future__ import annotations

import logging
import os
from uuid import uuid4

from altegio_bot.providers.base import WhatsAppProvider

logger = logging.getLogger(__name__)

ALLOW_REAL_SEND_ENV = 'ALLOW_REAL_SEND'
WHATSAPP_PROVIDER_ENV = 'WHATSAPP_PROVIDER'
META_PROVIDER_KEY = 'meta_cloud'


def _provider_key(provider: WhatsAppProvider) -> str:
    key = os.getenv(WHATSAPP_PROVIDER_ENV, '').strip().lower()
    if key:
        return key

    module_name = provider.__class__.__module__.rsplit('.', 1)[-1]
    return module_name.strip().lower()


def _real_send_allowed(provider: WhatsAppProvider) -> bool:
    if _provider_key(provider) != META_PROVIDER_KEY:
        return True
    return os.getenv(ALLOW_REAL_SEND_ENV, '0').strip() == '1'


class DummyProvider(WhatsAppProvider):
    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
    ) -> str:
        provider_message_id = f'dummy-{uuid4()}'
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
    if not _real_send_allowed(provider):
        return None, 'Real send disabled'

    try:
        msg_id = await provider.send(sender_id, phone, text)
        return msg_id, None
    except Exception as exc:
        logger.exception('send failed: %s', exc)
        return None, str(exc)
