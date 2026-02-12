from __future__ import annotations

import logging
import os
from typing import Any

import httpx

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import WhatsAppSender
from altegio_bot.providers.base import WhatsAppProvider

logger = logging.getLogger(__name__)


def _strip_plus(phone_e164: str) -> str:
    return phone_e164.lstrip('+').strip()


class MetaCloudProvider(WhatsAppProvider):
    def __init__(
        self,
        *,
        access_token: str | None = None,
        api_version: str | None = None,
        graph_url: str | None = None,
        timeout_sec: float = 20.0,
    ) -> None:
        self._access_token = (
            access_token
            if access_token is not None
            else os.getenv('WHATSAPP_ACCESS_TOKEN', '').strip()
        )
        self._api_version = (
            api_version
            if api_version is not None
            else os.getenv('WHATSAPP_API_VERSION', 'v21.0').strip()
        )
        self._graph_url = (
            graph_url
            if graph_url is not None
            else os.getenv('WHATSAPP_GRAPH_URL', 'https://graph.facebook.com')
            .strip()
        )

        self._allow_real_send = os.getenv('ALLOW_REAL_SEND', '0').strip() == '1'
        self._sender_cache: dict[int, str] = {}
        self._client = httpx.AsyncClient(timeout=timeout_sec)

        if not self._access_token:
            raise RuntimeError('WHATSAPP_ACCESS_TOKEN is not set')

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _get_phone_number_id(self, sender_id: int) -> str:
        cached = self._sender_cache.get(sender_id)
        if cached:
            return cached

        async with SessionLocal() as session:
            sender = await session.get(WhatsAppSender, sender_id)

        if sender is None:
            raise RuntimeError(f'WhatsAppSender not found: id={sender_id}')

        phone_number_id = (sender.phone_number_id or '').strip()
        if not phone_number_id:
            raise RuntimeError(
                f'phone_number_id is empty for sender_id={sender_id}'
            )

        self._sender_cache[sender_id] = phone_number_id
        return phone_number_id

    def _headers(self) -> dict[str, str]:
        return {'Authorization': f'Bearer {self._access_token}'}

    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
    ) -> str:
        if not self._allow_real_send:
            raise RuntimeError('Real send disabled (set ALLOW_REAL_SEND=1)')

        phone_number_id = await self._get_phone_number_id(sender_id)
        to_number = _strip_plus(phone_e164)

        url = f'{self._graph_url}/{self._api_version}/{phone_number_id}/messages'
        payload: dict[str, Any] = {
            'messaging_product': 'whatsapp',
            'to': to_number,
            'type': 'text',
            'text': {
                'body': text,
                'preview_url': False,
            },
        }

        res = await self._client.post(url, headers=self._headers(), json=payload)

        data: dict[str, Any] = {}
        try:
            data = res.json()
        except Exception:
            data = {}

        if res.status_code >= 400:
            err = data.get('error') if isinstance(data, dict) else None
            msg = None
            if isinstance(err, dict):
                msg = err.get('message')
            raise RuntimeError(
                f'Meta send failed status={res.status_code} '
                f'body={msg or data}'
            )

        messages = data.get('messages') if isinstance(data, dict) else None
        if isinstance(messages, list) and messages:
            first = messages[0]
            if isinstance(first, dict) and first.get('id'):
                msg_id = str(first['id'])
                logger.info(
                    'Meta send ok sender_id=%s phone=%s msg_id=%s',
                    sender_id,
                    phone_e164,
                    msg_id,
                )
                return msg_id

        raise RuntimeError(f'Unexpected Meta response: {data}')
