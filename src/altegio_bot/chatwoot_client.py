"""Chatwoot API client – thin async wrapper around the Chatwoot REST API.

Only the methods required for the dual-write integration are implemented:
- get_or_create_contact  – upsert a contact by phone number
- get_or_create_conversation – open/reuse a conversation for a contact
- send_message           – post an outbound message to a conversation
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from altegio_bot.settings import settings

logger = logging.getLogger(__name__)


class ChatwootClient:
    """Async Chatwoot API client."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        api_token: str | None = None,
        account_id: int | None = None,
        inbox_id: int | None = None,
        timeout_sec: float = 15.0,
    ) -> None:
        self._base_url = (base_url or settings.chatwoot_base_url).rstrip("/")
        self._api_token = api_token or settings.chatwoot_api_token
        self._account_id = account_id if account_id is not None else settings.chatwoot_account_id
        self._inbox_id = inbox_id if inbox_id is not None else settings.chatwoot_inbox_id
        self._client = httpx.AsyncClient(timeout=timeout_sec)

    async def aclose(self) -> None:
        await self._client.aclose()

    def _headers(self) -> dict[str, str]:
        return {
            "api_access_token": self._api_token,
            "Content-Type": "application/json",
        }

    def _api(self, path: str) -> str:
        return f"{self._base_url}/api/v1/accounts/{self._account_id}{path}"

    async def get_or_create_contact(
        self,
        phone_e164: str,
        *,
        name: str | None = None,
    ) -> int:
        """Return Chatwoot contact ID, creating one if necessary."""
        # Try to find existing contact by phone
        search_url = self._api("/contacts/search")
        res = await self._client.get(
            search_url,
            headers=self._headers(),
            params={"q": phone_e164, "include_contacts": "true"},
        )
        if res.status_code == 200:
            data: dict[str, Any] = res.json()
            payload_list = data.get("payload") or []
            if isinstance(payload_list, list):
                for contact in payload_list:
                    if isinstance(contact, dict):
                        phone = (contact.get("phone_number") or "").strip()
                        if phone == phone_e164:
                            cid = contact.get("id")
                            if cid is not None:
                                return int(cid)

        # Create new contact
        create_url = self._api("/contacts")
        body: dict[str, Any] = {"phone_number": phone_e164}
        if name:
            body["name"] = name
        res = await self._client.post(
            create_url,
            headers=self._headers(),
            json=body,
        )
        res.raise_for_status()
        data = res.json()
        contact_id = data.get("id") or (data.get("payload") or {}).get("contact", {}).get("id")
        if contact_id is None:
            raise RuntimeError(f"Failed to create Chatwoot contact: {data}")
        return int(contact_id)

    async def get_or_create_conversation(
        self,
        contact_id: int,
    ) -> int:
        """Return an open conversation ID for this contact, creating one if needed."""
        # List existing conversations for the contact
        list_url = self._api(f"/contacts/{contact_id}/conversations")
        res = await self._client.get(list_url, headers=self._headers())
        if res.status_code == 200:
            data = res.json()
            conversations = (data.get("payload") or []) if isinstance(data, dict) else (data or [])
            if isinstance(conversations, list):
                for conv in conversations:
                    if not isinstance(conv, dict):
                        continue
                    # Prefer open conversations on our inbox
                    inbox_id = conv.get("inbox_id")
                    status = conv.get("status", "")
                    if inbox_id == self._inbox_id and status == "open":
                        cid = conv.get("id")
                        if cid is not None:
                            return int(cid)

        # Create a new conversation
        create_url = self._api("/conversations")
        body = {
            "inbox_id": self._inbox_id,
            "contact_id": contact_id,
        }
        res = await self._client.post(
            create_url,
            headers=self._headers(),
            json=body,
        )
        res.raise_for_status()
        data = res.json()
        conv_id = data.get("id")
        if conv_id is None:
            raise RuntimeError(f"Failed to create Chatwoot conversation: {data}")
        return int(conv_id)

    async def send_message(
        self,
        conversation_id: int,
        content: str,
        *,
        message_type: str = "outgoing",
        private: bool = False,
    ) -> int:
        """Post a message to a conversation. Returns the message ID."""
        url = self._api(f"/conversations/{conversation_id}/messages")
        body: dict[str, Any] = {
            "content": content,
            "message_type": message_type,
            "private": private,
        }
        res = await self._client.post(url, headers=self._headers(), json=body)
        res.raise_for_status()
        data: dict[str, Any] = res.json()
        msg_id = data.get("id")
        if msg_id is None:
            raise RuntimeError(f"Chatwoot send_message returned no id: {data}")
        return int(msg_id)

    async def log_incoming_message(
        self,
        phone_e164: str,
        content: str,
        *,
        contact_name: str | None = None,
    ) -> tuple[int, int]:
        """Log an incoming message from a customer.

        Returns (conversation_id, chatwoot_message_id).
        Best-effort: callers should catch all exceptions.
        """
        contact_id = await self.get_or_create_contact(
            phone_e164,
            name=contact_name,
        )
        conversation_id = await self.get_or_create_conversation(contact_id)
        note_content = f"👤 [ВХОДЯЩЕЕ ОТ КЛИЕНТА]:\n{content}"
        message_id = await self.send_message(
            conversation_id,
            note_content,
            message_type="outgoing",
            private=True,
        )
        return conversation_id, message_id
