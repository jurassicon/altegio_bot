"""Chatwoot API client – thin async wrapper around the Chatwoot REST API.

Only the methods required for the dual-write integration are implemented:
- get_or_create_contact      – upsert a contact by phone number
- get_or_create_conversation – open/reuse a conversation for a contact
- send_message               – post an outbound message to a conversation
- mirror_outbound_as_note    – mirror outbound message as a private agent note
"""

from __future__ import annotations

import logging
import re
from typing import Any

import httpx

from altegio_bot.settings import settings

logger = logging.getLogger(__name__)

_ALLOWED_FORWARDED_PROTOS = ("http", "https")


def normalize_forwarded_proto(value: str | None) -> str | None:
    """Validate a CHATWOOT_API_FORWARDED_PROTO value.

    Returns "http"/"https" (trimmed, lower-cased) or None when the header
    must not be sent. Invalid values are ignored with a warning so a typo
    can never silently change request semantics.
    """
    if value is None:
        return None
    cleaned = value.strip().lower()
    if not cleaned:
        return None
    if cleaned in _ALLOWED_FORWARDED_PROTOS:
        return cleaned
    logger.warning(
        "chatwoot: ignoring invalid CHATWOOT_API_FORWARDED_PROTO=%r (expected 'http' or 'https')",
        value,
    )
    return None


def forwarded_proto_header(value: str | None = None) -> dict[str, str]:
    """Optional X-Forwarded-Proto header for Chatwoot API requests.

    Reads settings.chatwoot_api_forwarded_proto unless an explicit value is
    given. Returns {} when the feature is off — callers can always merge the
    result into their existing headers.
    """
    raw = value if value is not None else settings.chatwoot_api_forwarded_proto
    proto = normalize_forwarded_proto(raw)
    if proto:
        return {"X-Forwarded-Proto": proto}
    return {}


def append_wa_deeplink(text: str, phone_e164: str | None) -> str:
    """Append a WhatsApp deeplink footer to a Chatwoot message body.

    Idempotent: skipped when the deeplink is already present or when
    phone_e164 contains no digits.
    """
    if not phone_e164:
        return text
    digits = re.sub(r"\D", "", phone_e164)
    if not digits:
        return text
    wa_url = f"https://wa.me/{digits}"
    if wa_url in text:
        return text
    return f"{text}\n\n---\n\U0001f4ac Написать в WhatsApp: {wa_url}"


def _log_and_raise(res: httpx.Response, ctx: str) -> None:
    """Log response body on error, then raise via raise_for_status."""
    if res.is_error:
        logger.warning(
            "chatwoot: %s failed status=%s body=%.300s",
            ctx,
            res.status_code,
            res.text,
        )
        res.raise_for_status()


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
        forwarded_proto: str | None = None,
    ) -> None:
        self._base_url = (base_url or settings.chatwoot_base_url).rstrip("/")
        self._api_token = api_token or settings.chatwoot_api_token
        self._account_id = account_id if account_id is not None else settings.chatwoot_account_id
        self._inbox_id = inbox_id if inbox_id is not None else settings.chatwoot_inbox_id
        # Normalize once: an invalid value warns a single time per client,
        # not on every request.
        self._forwarded_proto = normalize_forwarded_proto(
            forwarded_proto if forwarded_proto is not None else settings.chatwoot_api_forwarded_proto
        )
        self._client = httpx.AsyncClient(timeout=timeout_sec)

    async def aclose(self) -> None:
        await self._client.aclose()

    def _headers(self) -> dict[str, str]:
        headers = {
            "api_access_token": self._api_token,
            "Content-Type": "application/json",
        }
        if self._forwarded_proto:
            headers["X-Forwarded-Proto"] = self._forwarded_proto
        return headers

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
                                current_name = contact.get("name")
                                if name and current_name != name:
                                    update_url = self._api(f"/contacts/{cid}")
                                    # Отправляем PUT-запрос на обновление.
                                    await self._client.put(update_url, headers=self._headers(), json={"name": name})
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
        _log_and_raise(res, "create_contact")
        data = res.json()
        contact_id = data.get("id") or (data.get("payload") or {}).get("contact", {}).get("id")
        if contact_id is None:
            raise RuntimeError(f"Failed to create Chatwoot contact: {data}")
        return int(contact_id)

    async def get_or_create_conversation(self, contact_id: int) -> int:
        """Return a single persistent conversation for this contact.

        Strategy (WhatsApp-style single thread):
        1. Fetch all conversations for the contact in our inbox.
        2. Prefer an already-open one — return it immediately.
        3. If only resolved/pending ones exist — reopen the most recent one
           via PATCH /conversations/{id}/toggle_status instead of creating
           a new conversation. This keeps the full history in one thread.
        4. Only create a brand-new conversation when none exist at all.
        """
        list_url = self._api(f"/contacts/{contact_id}/conversations")
        res = await self._client.get(list_url, headers=self._headers())

        best_conv_id: int | None = None  # самый свежий resolved/pending
        best_conv_created: int = -1  # unix timestamp для сравнения

        if res.status_code == 200:
            data = res.json()
            conversations = data.get("payload") or [] if isinstance(data, dict) else (data or [])

            if isinstance(conversations, list):
                for conv in conversations:
                    if not isinstance(conv, dict):
                        continue

                    # Только наш inbox
                    if conv.get("inbox_id") != self._inbox_id:
                        continue

                    cid = conv.get("id")
                    if cid is None:
                        continue

                    status = conv.get("status", "")

                    # ── Шаг 2: уже открытая — берём сразу ──────────────
                    if status == "open":
                        logger.debug(
                            "Chatwoot: reusing open conversation_id=%s for contact_id=%s",
                            cid,
                            contact_id,
                        )
                        return int(cid)

                    # ── Шаг 3: resolved/pending — запоминаем самую свежую
                    created_at = conv.get("created_at") or 0
                    if isinstance(created_at, str):
                        # Chatwoot может вернуть ISO-строку
                        try:
                            from datetime import datetime as _dt

                            created_at = int(_dt.fromisoformat(created_at.replace("Z", "+00:00")).timestamp())
                        except Exception:
                            created_at = 0

                    if int(created_at) > best_conv_created:
                        best_conv_created = int(created_at)
                        best_conv_id = int(cid)

        # ── Шаг 3: реоткрываем самую свежую resolved/pending беседу ────
        if best_conv_id is not None:
            reopen_url = self._api(f"/conversations/{best_conv_id}/toggle_status")
            patch_res = await self._client.post(
                reopen_url,
                headers=self._headers(),
                json={"status": "open"},
            )
            if patch_res.status_code in (200, 201):
                logger.info(
                    "Chatwoot: reopened conversation_id=%s for contact_id=%s (WhatsApp-style single thread)",
                    best_conv_id,
                    contact_id,
                )
                return best_conv_id

            # Если реоткрытие не удалось — логируем и падаем в создание новой
            logger.warning(
                "Chatwoot: failed to reopen conversation_id=%s (status=%s), will create new",
                best_conv_id,
                patch_res.status_code,
            )

        # ── Шаг 4: создаём новую беседу (только если нет ни одной) ─────
        create_url = self._api("/conversations")
        create_res = await self._client.post(
            create_url,
            headers=self._headers(),
            json={
                "inbox_id": self._inbox_id,
                "contact_id": contact_id,
                "status": "open",
            },
        )
        _log_and_raise(create_res, "create_conversation")
        data = create_res.json()
        conv_id = data.get("id")
        if conv_id is None:
            raise RuntimeError(f"Failed to create Chatwoot conversation: {data}")
        logger.info(
            "Chatwoot: created new conversation_id=%s for contact_id=%s",
            conv_id,
            contact_id,
        )
        return int(conv_id)

    async def send_message(
        self,
        conversation_id: int,
        content: str,
        *,
        message_type: str = "outgoing",
        private: bool = False,
        content_attributes: dict[str, Any] | None = None,
    ) -> int:
        """Post a message to a conversation. Returns the message ID.

        ``content_attributes`` is forwarded verbatim to Chatwoot when provided
        (used for native reply rendering via ``in_reply_to`` /
        ``in_reply_to_external_id``).  It is omitted entirely when ``None`` so
        existing behavior is unchanged.
        """
        url = self._api(f"/conversations/{conversation_id}/messages")

        # Формируем тело без поля private
        body: dict[str, Any] = {
            "content": content,
            "message_type": message_type,
        }

        if content_attributes is not None:
            body["content_attributes"] = content_attributes

        # Chatwoot выдает 422, если отправить поле private для входящих сообщений,
        # поэтому добавляем его ТОЛЬКО для исходящих/заметок.
        if message_type == "outgoing":
            body["private"] = private

        res = await self._client.post(url, headers=self._headers(), json=body)
        _log_and_raise(res, "send_message")
        data: dict[str, Any] = res.json()
        msg_id = data.get("id")
        if msg_id is None:
            raise RuntimeError(f"Chatwoot send_message returned no id: {data}")
        return int(msg_id)

    async def _conversation_has_inbound(self, conversation_id: int) -> bool:
        """Return True if the conversation has any incoming message from client.

        Used to decide whether to attach a wa.me deeplink to mirror notes:
        deeplink is only useful when the client has never written themselves,
        so a master can initiate contact via personal WhatsApp.  Once the
        client has written in, the deeplink is noise.

        Returns False on any API error so deeplink is kept as the safe default.
        """
        url = self._api(f"/conversations/{conversation_id}/messages")
        try:
            res = await self._client.get(url, headers=self._headers())
            if res.status_code != 200:
                return False
            data = res.json()
            messages: list = []
            if isinstance(data, list):
                messages = data
            elif isinstance(data, dict):
                payload = data.get("payload", [])
                if isinstance(payload, list):
                    messages = payload
                elif isinstance(payload, dict):
                    messages = payload.get("messages", [])
            return any(m.get("message_type") in (0, "incoming") for m in messages if isinstance(m, dict))
        except Exception:
            return False

    async def get_or_create_incoming_conversation(
        self,
        phone_e164: str,
        *,
        contact_name: str | None = None,
    ) -> int:
        """Resolve the conversation an inbound message would land in.

        Returns the conversation id WITHOUT posting a message, so the caller
        can decide whether a native ``in_reply_to`` target lives in this same
        conversation before sending.  Mirrors the contact/conversation
        resolution that :meth:`log_incoming_message` performs.
        """
        contact_id = await self.get_or_create_contact(
            phone_e164,
            name=contact_name,
        )
        return await self.get_or_create_conversation(contact_id)

    async def log_incoming_message(
        self,
        phone_e164: str,
        content: str,
        *,
        contact_name: str | None = None,
        content_attributes: dict[str, Any] | None = None,
    ) -> tuple[int, int]:
        """Log an incoming message from a customer.

        Returns (conversation_id, chatwoot_message_id).
        Best-effort: callers should catch all exceptions.

        ``content_attributes`` (when provided) is forwarded so a WhatsApp
        reply can render as a native Chatwoot reply (``in_reply_to``).

        No wa.me deeplink is appended — the client already has WhatsApp open
        and the link would only add noise to the conversation view.
        """
        conversation_id = await self.get_or_create_incoming_conversation(
            phone_e164,
            contact_name=contact_name,
        )

        message_id = await self.send_message(
            conversation_id,
            content,
            message_type="incoming",
            content_attributes=content_attributes,
        )
        logger.info(
            "chatwoot: incoming logged phone=%s conversation_id=%s message_id=%s",
            phone_e164,
            conversation_id,
            message_id,
        )
        return conversation_id, message_id

    async def mirror_outbound_as_note(
        self,
        phone_e164: str,
        text: str,
        *,
        contact_name: str | None = None,
    ) -> None:
        """Mirror an outbound message to Chatwoot as a private agent note.

        Pattern from irida_whisper/_send_private_note:
          private=True → yellow speech bubble, visible to agents only,
          never delivered to the customer, no conflict with Meta webhook.

        Deeplink policy: attach a wa.me link only when the conversation has
        no prior inbound from the client.  Once the client has written in,
        the deeplink is redundant and pollutes the conversation view.

        Never raises — best-effort.
        """
        try:
            contact_id = await self.get_or_create_contact(phone_e164, name=contact_name)
            conversation_id = await self.get_or_create_conversation(contact_id)
            has_inbound = await self._conversation_has_inbound(conversation_id)
            body = text if has_inbound else append_wa_deeplink(text, phone_e164)
            msg_id = await self.send_message(
                conversation_id,
                body,
                message_type="outgoing",
                private=True,
            )
            logger.info(
                "Chatwoot mirror note posted msg_id=%s conv=%s phone=%s",
                msg_id,
                conversation_id,
                phone_e164,
            )
        except Exception:
            logger.exception("Chatwoot mirror failed (best-effort, ignored) phone=%s", phone_e164)
