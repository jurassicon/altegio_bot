from __future__ import annotations

import os
from typing import Any

import aiohttp

from altegio_bot.providers.base import WhatsAppProvider


class MetaCloudProvider(WhatsAppProvider):
    def __init__(self) -> None:
        self._token = os.getenv('META_WA_TOKEN', '').strip()
        self._phone_number_id = os.getenv(
            'META_WA_PHONE_NUMBER_ID', ''
        ).strip()
        self._base = os.getenv(
            'META_GRAPH_API_BASE', 'https://graph.facebook.com'
        ).strip()
        self._ver = os.getenv('META_GRAPH_API_VERSION', 'v20.0').strip()

        if not self._token or not self._phone_number_id:
            raise RuntimeError(
                'META_WA_TOKEN / META_WA_PHONE_NUMBER_ID is not set'
            )

    async def send(self, sender_id: int, phone: str, text: str) -> str:
        url = f'{self._base}/{self._ver}/{self._phone_number_id}/messages'
        payload: dict[str, Any] = {
            'messaging_product': 'whatsapp',
            'to': phone,
            'type': 'text',
            'text': {'body': text, 'preview_url': False},
        }
        headers = {
            'Authorization': f'Bearer {self._token}',
            'Content-Type': 'application/json',
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as r:
                data = await r.json(content_type=None)
                if r.status >= 400:
                    raise RuntimeError(f'Meta send failed: {r.status} {data}')

        # обычно id лежит в data['messages'][0]['id'], но оставим безопасно:
        msgs = data.get('messages') or []
        if msgs and isinstance(msgs, list) and 'id' in msgs[0]:
            return str(msgs[0]['id'])
        return 'meta-unknown'
