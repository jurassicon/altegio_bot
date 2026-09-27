"""Chatwoot API client – thin async wrapper around the Chatwoot REST API.

Only the methods required for the dual-write integration are implemented:
- get_or_create_contact      – upsert a contact by phone number
- get_or_create_conversation – open/reuse a conversation for a contact
- send_message               – post an outbound message to a conversation
- mirror_outbound_as_note    – mirror outbound message as a private agent note
- find_outbound_mirror_note  – legacy bounded scan for one outbound wamid
"""

from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx

from altegio_bot.chatwoot_headers import normalize_forwarded_proto
from altegio_bot.settings import settings

logger = logging.getLogger(__name__)


# Technical marker written on a bot/automation private mirror note so a later
# inbound WhatsApp reaction can prove WHICH Chatwoot message a Meta wamid became.
# The version suffix is part of the contract: an unknown or older value is never
# accepted as a native reply target.
OUTBOUND_MIRROR_MESSAGE_KIND = "whatsapp_outbound_mirror_v1"

# Conservative INTERNAL compatibility ceiling for a finished Click-to-Chat URL —
# not an official Meta limit. Meta publishes no maximum length for the wa.me
# prefill text. Above this the prefill is dropped entirely; the operator's text
# is never truncated or partially inserted.
WA_CLICK_TO_CHAT_MAX_URL_CHARS = 2000

# Chatwoot returns message_type either as its numeric enum or as a string.
_OUTGOING_MESSAGE_TYPES: frozenset[object] = frozenset({1, "outgoing"})

# Chatwoot (4.17) serves a conversation's messages one page at a time and pages
# backwards through ``before=<message id>``. Its query filters ``id < before``,
# orders by ``created_at DESC``, takes a page, and then REVERSES it — so the array
# that comes back is in ASCENDING chronological order and ``page[0]`` is the
# oldest message on the page. It is NOT newest-first, and the numerically
# smallest id on the page is NOT necessarily its chronological boundary.
#
# A full page is this many messages; a shorter page therefore proves the walked
# history ended there.
_MIRROR_NOTE_PAGE_SIZE = 20

# Conservative page budget for one legacy marker scan, bounding it to
# ~_MIRROR_NOTE_PAGE_SIZE * _MIRROR_NOTE_MAX_PAGES ≈ 200 messages. The scan stops
# the moment it has proof, so a note on the first page costs exactly one request
# no matter how much older history exists; this budget only limits how far back a
# scan will look for a note it has not found yet. Exhausting it is a fail-closed
# miss. New mirror notes do not depend on this at all — they are resolved through
# the durable registry in ``chatwoot_mirror_registry`` in one indexed read.
_MIRROR_NOTE_MAX_PAGES = 10

# One wall-clock ceiling for the WHOLE scan, not a per-request timeout and not
# the sum of ten of them. The scan runs inline while the WhatsAppEvent row is
# locked and events are processed serially, so the entire proof must be cheap in
# latency terms: ten independent 15s client timeouts would be 150 seconds of held
# lock, which is not an acceptable cost for a best-effort cosmetic improvement.
_MIRROR_NOTE_TOTAL_DEADLINE_SEC = 5.0

# Per-page ceiling, so one stalled page cannot eat the whole budget on its own.
# The effective timeout of each request is the smaller of this and the deadline
# that remains, which is what makes the total bound strict rather than nominal.
_MIRROR_NOTE_PAGE_TIMEOUT_SEC = 2.0


def _monotonic() -> float:
    """Monotonic clock for the scan deadline (patched in tests, never mocked in prod)."""
    return time.monotonic()


@dataclass(frozen=True)
class MirroredNote:
    """The Chatwoot message a private mirror note actually became.

    Returned by :meth:`ChatwootClient.mirror_outbound_as_note` so the caller can
    record a durable WAMID → Message.id link. ``None`` is returned instead
    whenever Chatwoot did not answer with a usable id, and then no link is
    recorded and the reaction path degrades to its visible quote.
    """

    conversation_id: int
    message_id: int


def _wa_phone_digits(phone_e164: str | None) -> str | None:
    """Return the international number as bare digits, or None when unusable.

    wa.me accepts digits only: no ``+``, spaces, brackets or hyphens.
    """
    if not phone_e164:
        return None
    digits = re.sub(r"\D", "", phone_e164)
    return digits or None


def append_wa_deeplink(text: str, phone_e164: str | None) -> str:
    """Append a WhatsApp deeplink footer to a Chatwoot message body.

    Idempotent: skipped when the deeplink is already present or when
    phone_e164 contains no digits.
    """
    digits = _wa_phone_digits(phone_e164)
    if digits is None:
        return text
    wa_url = f"https://wa.me/{digits}"
    if wa_url in text:
        return text
    return f"{text}\n\n---\n\U0001f4ac Написать в WhatsApp: {wa_url}"


def build_wa_click_to_chat_url(phone_e164: str | None, text: str | None = None) -> str | None:
    """Build a wa.me Click-to-Chat URL, optionally prefilling the composer.

    ``https://wa.me/<digits>`` without ``text``, ``https://wa.me/<digits>?text=
    <percent-encoded>`` with it. Encoding uses ``quote(text, safe="")``, so
    spaces, newlines, ``&``, ``?``, ``#``, ``%``, quotes, brackets, Unicode and
    emoji all survive a round trip through ``unquote``.

    The length test is applied to the FINISHED ASCII URL after percent-encoding.
    Over :data:`WA_CLICK_TO_CHAT_MAX_URL_CHARS` the prefill is dropped and the
    plain wa.me URL is returned — the text is never truncated or partially
    inserted. An unusable phone returns ``None``.

    The link only opens WhatsApp and fills the composer. It sends nothing, and it
    does not bypass Meta's 24h customer service window: whatever the operator
    then sends comes from their own WhatsApp account.
    """
    digits = _wa_phone_digits(phone_e164)
    if digits is None:
        return None
    base_url = f"https://wa.me/{digits}"
    if not text:
        return base_url
    prefilled_url = f"{base_url}?text={quote(text, safe='')}"
    if len(prefilled_url) > WA_CLICK_TO_CHAT_MAX_URL_CHARS:
        return base_url
    return prefilled_url


def outbound_mirror_content_attributes(provider_message_id: str | None) -> dict[str, Any] | None:
    """Narrow technical ``content_attributes`` for an outbound mirror note.

    Exactly two keys — the versioned marker and the exact Meta wamid. No internal
    meta dict, no PII, no template/job/record fields are forwarded to Chatwoot.
    Returns ``None`` when there is no wamid to bind, so the note is posted
    unchanged and stays a plain historical note that can never be proven native.
    """
    wamid = (provider_message_id or "").strip()
    if not wamid:
        return None
    return {
        "altegio_bot_message_kind": OUTBOUND_MIRROR_MESSAGE_KIND,
        "whatsapp_provider_message_id": wamid,
    }


def _parse_conversation_messages(data: Any) -> list[Any] | None:
    """Normalize one page of the conversation messages endpoint.

    Returns the page as a list — possibly EMPTY, which is a legitimate final page
    and a valid proof that the walked history ended. Returns ``None`` only when
    the body is not a recognizable messages payload at all, so a malformed
    response can never be mistaken for "no more messages" and must fail closed.
    """
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return None
    payload = data.get("payload")
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        messages = payload.get("messages")
        if isinstance(messages, list):
            return messages
    return None


def _parse_returned_content_attributes(value: Any) -> dict[str, Any] | None:
    """Read ``content_attributes`` off an API response, or None when unusable.

    Accepts a JSON object and a JSON-object string (older Chatwoot versions
    serialize the column). Anything else is malformed for our purposes and must
    make the candidate fail closed instead of being guessed at.
    """
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def _positive_chatwoot_id(value: Any) -> int | None:
    """A usable Chatwoot id: a positive integer, and not a bool."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value > 0 else None


def _message_id(message: Any) -> int | None:
    """The usable Chatwoot message id, or None.

    Usable means a positive integer — the same rule for a proven native target
    and for a pagination cursor, so the two can never drift apart.
    """
    if not isinstance(message, dict):
        return None
    return _positive_chatwoot_id(message.get("id"))


def _page_boundary_message_id(page: list[Any]) -> int | None:
    """The id of the page's CHRONOLOGICAL boundary — the next ``before`` cursor.

    Chatwoot returns the page in ascending chronological order, so the boundary is
    ``page[0]``: the oldest message on the page. This is deliberately positional
    and deliberately NOT ``min(id)``. The two coincide only while ids happen to
    increase with ``created_at``; a backdated or imported message breaks that, and
    a cursor taken from the numerically smallest id would then name a message that
    is not the boundary and skip real history between the two.

    The page is never re-sorted here — reordering the server's answer would throw
    away the only ordering information the response carries.

    ``None`` when the page is empty or its boundary message has no usable id, in
    which case the walk cannot be continued and must fail closed rather than
    re-request the same page or guess a different boundary.
    """
    if not page:
        return None
    return _message_id(page[0])


def _mirror_note_message_id(
    message: Any,
    *,
    conversation_id: int,
    provider_message_id: str,
) -> int | None:
    """Return the message id when this message is a PROVEN mirror note.

    Every condition must hold at once; a single missing proof returns ``None``.
    Nothing here looks at the body, the template code, recency or result order.
    """
    if not isinstance(message, dict):
        return None
    own_conversation_id = message.get("conversation_id")
    if own_conversation_id is not None and own_conversation_id != conversation_id:
        return None
    message_type = message.get("message_type")
    if isinstance(message_type, bool) or message_type not in _OUTGOING_MESSAGE_TYPES:
        return None
    if message.get("private") is not True:
        return None
    attributes = _parse_returned_content_attributes(message.get("content_attributes"))
    if attributes is None:
        return None
    if attributes.get("altegio_bot_message_kind") != OUTBOUND_MIRROR_MESSAGE_KIND:
        return None
    if attributes.get("whatsapp_provider_message_id") != provider_message_id:
        return None
    return _message_id(message)


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


def _normalize_content_attributes(content_attributes: Any) -> dict[str, Any]:
    """Return content_attributes as a JSON object (dict) for the Chatwoot API body.

    Python-side normalization only — the Chatwoot REST API receives a real JSON
    object, never a JSON *string*. ``altegio_bot`` never touches Chatwoot's own
    database storage of ``content_attributes``.

    - str  → ``json.loads``; a valid JSON object returns a dict, anything else
      (invalid JSON, or valid JSON that is not an object) raises ValueError.
    - Mapping → a plain ``dict`` copy.
    - otherwise → TypeError.
    """
    if isinstance(content_attributes, str):
        try:
            parsed = json.loads(content_attributes)
        except json.JSONDecodeError as exc:
            raise ValueError("content_attributes_json_string_invalid") from exc
        if not isinstance(parsed, dict):
            raise ValueError("content_attributes_must_be_json_object")
        return parsed

    if isinstance(content_attributes, Mapping):
        return dict(content_attributes)

    raise TypeError("content_attributes_must_be_mapping")


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

        ``content_attributes`` is normalized to a JSON object (dict) before the
        API POST and forwarded to Chatwoot when provided (used for native reply
        rendering via ``in_reply_to`` / ``in_reply_to_external_id``).  It is
        omitted entirely when ``None`` so existing behavior is unchanged.

        ``altegio_bot`` only sends ``content_attributes`` through the Chatwoot
        REST API; it never connects to Chatwoot's database and never rewrites how
        Chatwoot stores ``content_attributes`` afterwards. Success is the created
        message id from the API response — not any particular DB storage shape.
        """
        url = self._api(f"/conversations/{conversation_id}/messages")

        # Формируем тело без поля private
        body: dict[str, Any] = {
            "content": content,
            "message_type": message_type,
        }

        normalized_attributes: dict[str, Any] | None = None
        if content_attributes is not None:
            normalized_attributes = _normalize_content_attributes(content_attributes)
            body["content_attributes"] = normalized_attributes

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
        message_id = int(msg_id)

        # content_attributes are sent through the Chatwoot REST API only. We do
        # NOT touch Chatwoot's DB afterwards: the current Chatwoot version expects
        # its own serialized storage format, and rewriting it can break Chatwoot
        # UI/API rendering. The created message id is the success criterion.
        return message_id

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
        # DEBUG, not INFO: normal per-message path, fires for every inbound
        # WhatsApp message (incl. native reply/reaction) — must not add noise.
        logger.debug(
            "chatwoot: incoming logged phone=%s conversation_id=%s message_id=%s",
            phone_e164,
            conversation_id,
            message_id,
        )
        return conversation_id, message_id

    async def _conversation_messages_page(
        self,
        conversation_id: int,
        *,
        before: int | None,
        timeout: float,
    ) -> list[Any] | None:
        """One page of a conversation's messages, or None when unusable.

        ``None`` is the single fail-closed signal covering a transport error, any
        status other than 200, a body that is not JSON, and a body that is not a
        recognizable messages payload. An empty list is a real, usable page.

        ``timeout`` is the remaining share of the scan's overall wall-clock
        deadline, so no single page can outlive the whole budget.

        Logs carry only the conversation id, the cursor presence and a stable
        reason — never a wamid, phone, message body, URL, token or response body.
        """
        url = self._api(f"/conversations/{conversation_id}/messages")
        params = {"before": str(before)} if before is not None else None
        try:
            res = await self._client.get(url, headers=self._headers(), params=params, timeout=timeout)
        except Exception as exc:
            logger.debug(
                "chatwoot: mirror note page conversation_id=%s paged=%s reason=transport_error error_type=%s",
                conversation_id,
                before is not None,
                type(exc).__name__,
            )
            return None
        if res.status_code != 200:
            logger.debug(
                "chatwoot: mirror note page conversation_id=%s paged=%s reason=http_status status=%s",
                conversation_id,
                before is not None,
                res.status_code,
            )
            return None
        try:
            data = res.json()
        except ValueError:
            logger.debug(
                "chatwoot: mirror note page conversation_id=%s paged=%s reason=malformed_json",
                conversation_id,
                before is not None,
            )
            return None
        messages = _parse_conversation_messages(data)
        if messages is None:
            logger.debug(
                "chatwoot: mirror note page conversation_id=%s paged=%s reason=malformed_payload",
                conversation_id,
                before is not None,
            )
        return messages

    async def find_outbound_mirror_note(
        self,
        conversation_id: int,
        provider_message_id: str,
    ) -> int | None:
        """Legacy bounded scan for the private mirror note of one outbound wamid.

        This is the FALLBACK path, for notes posted before the durable registry in
        ``chatwoot_mirror_registry`` existed. New notes are resolved from that
        registry in one indexed read and never reach this method.

        Trust model
        -----------
        The exact versioned marker carrying the exact wamid is accepted as
        sufficient proof, and the scan stops at the page that proves it. A
        candidate counts only when ALL of these hold at once:

        - it is listed by THIS conversation's messages endpoint, and its own
          ``conversation_id`` (when the payload carries one) is this conversation;
        - ``message_type`` is outgoing;
        - ``private`` is exactly ``True``;
        - ``content_attributes.altegio_bot_message_kind`` equals the expected
          marker version :data:`OUTBOUND_MIRROR_MESSAGE_KIND`;
        - ``content_attributes.whatsapp_provider_message_id`` equals the wamid
          exactly;
        - ``id`` is a positive integer.

        Nothing is ever matched by body, template code, ``created_at``, time
        proximity or "the last message", and a page is never re-sorted.

        Uniqueness is checked over the pages actually walked, which is the region
        this trust model is responsible for: two distinct proven ids seen before
        the scan stops are refused. Uniqueness is NOT claimed globally — the scan
        stops at its proof, so a duplicate marker further back is not looked for.
        Global uniqueness for new notes comes from the registry's unique key on the
        wamid instead.

        Cost and bounds
        ---------------
        A note on the first page costs exactly one request, regardless of how much
        older history the conversation has. Network cost scales with the distance
        to the target, not with the length of the conversation. The walk is capped
        by :data:`_MIRROR_NOTE_MAX_PAGES` pages AND by one overall wall-clock
        deadline of :data:`_MIRROR_NOTE_TOTAL_DEADLINE_SEC` seconds covering every
        page, with each request additionally capped by
        :data:`_MIRROR_NOTE_PAGE_TIMEOUT_SEC`.

        Pagination
        ----------
        Chatwoot returns each page in ascending chronological order, so the next
        cursor is the id of ``page[0]`` — the chronological boundary — and never
        ``min(id)``. The cursor must strictly decrease.

        Known upstream limitation: Chatwoot filters the next page by ``id <
        before`` while ordering by ``created_at``. When ids are not monotonic with
        ``created_at`` (backdated or imported messages) no id cursor can express
        that ordering, so this scan may not reach such a message. That defect is
        not papered over here; it is the reason new notes use the durable registry.

        Fail-closed
        -----------
        ``None`` — keep the caller's visible-quote fallback — for no match, for two
        distinct matches inside the walked region, for an HTTP/transport error, a
        malformed JSON body or an unrecognizable payload on any page, for a page
        whose boundary yields no usable cursor, for a cursor that would not
        strictly decrease (a replayed page), for the page budget running out, and
        for the wall-clock deadline expiring.

        Read-only through the REST API; Chatwoot's database is never touched.
        """
        wamid = (provider_message_id or "").strip()
        if not conversation_id or not wamid:
            return None

        matches: set[int] = set()
        cursor: int | None = None
        pages = 0
        scanned = 0
        deadline = _monotonic() + _MIRROR_NOTE_TOTAL_DEADLINE_SEC

        while True:
            remaining = deadline - _monotonic()
            if remaining <= 0:
                logger.debug(
                    "chatwoot: mirror note not proven conversation_id=%s reason=deadline_exceeded pages=%s messages=%s",
                    conversation_id,
                    pages,
                    scanned,
                )
                return None

            page = await self._conversation_messages_page(
                conversation_id,
                before=cursor,
                timeout=min(_MIRROR_NOTE_PAGE_TIMEOUT_SEC, remaining),
            )
            if page is None:
                return None
            pages += 1
            scanned += len(page)
            for message in page:
                candidate = _mirror_note_message_id(
                    message,
                    conversation_id=conversation_id,
                    provider_message_id=wamid,
                )
                if candidate is not None:
                    matches.add(candidate)

            if len(matches) > 1:
                # Two different Chatwoot messages claim the same wamid inside the
                # region this scan is responsible for. Never pick one.
                logger.debug(
                    "chatwoot: mirror note not proven conversation_id=%s reason=ambiguous_matches "
                    "pages=%s messages=%s match_count=%s",
                    conversation_id,
                    pages,
                    scanned,
                    len(matches),
                )
                return None
            if matches:
                # Proof in hand. Stop here: reading older pages could only cost
                # latency under a held lock, and a long history must not be able
                # to veto a target that was already proven.
                return next(iter(matches))

            if len(page) < _MIRROR_NOTE_PAGE_SIZE:
                # Short (or empty) page: the walked history is proven to end here
                # and the note is not in it.
                logger.debug(
                    "chatwoot: mirror note not proven conversation_id=%s reason=not_in_history pages=%s messages=%s",
                    conversation_id,
                    pages,
                    scanned,
                )
                return None

            if pages >= _MIRROR_NOTE_MAX_PAGES:
                logger.debug(
                    "chatwoot: mirror note not proven conversation_id=%s reason=page_budget_exhausted "
                    "pages=%s messages=%s",
                    conversation_id,
                    pages,
                    scanned,
                )
                return None

            next_cursor = _page_boundary_message_id(page)
            if next_cursor is None:
                logger.debug(
                    "chatwoot: mirror note not proven conversation_id=%s reason=no_pagination_cursor "
                    "pages=%s messages=%s",
                    conversation_id,
                    pages,
                    scanned,
                )
                return None
            if cursor is not None and next_cursor >= cursor:
                logger.debug(
                    "chatwoot: mirror note not proven conversation_id=%s reason=cursor_not_advancing "
                    "pages=%s messages=%s",
                    conversation_id,
                    pages,
                    scanned,
                )
                return None
            cursor = next_cursor

    async def mirror_outbound_as_note(
        self,
        phone_e164: str,
        text: str,
        *,
        contact_name: str | None = None,
        provider_message_id: str | None = None,
    ) -> MirroredNote | None:
        """Mirror an outbound message to Chatwoot as a private agent note.

        Pattern from irida_whisper/_send_private_note:
          private=True → yellow speech bubble, visible to agents only,
          never delivered to the customer, no conflict with Meta webhook.

        Deeplink policy: attach a wa.me link only when the conversation has
        no prior inbound from the client.  Once the client has written in,
        the deeplink is redundant and pollutes the conversation view.

        ``provider_message_id`` is the exact Meta wamid of the message this note
        mirrors. When present it is written as the narrow versioned marker from
        :func:`outbound_mirror_content_attributes`.

        Returns the :class:`MirroredNote` Chatwoot created, so the caller can
        record a durable wamid → Message.id link; that link is what a later
        inbound reaction resolves its native ``in_reply_to`` from. Returns ``None``
        when the note could not be posted or Chatwoot gave no usable ids — there is
        then nothing to record, and the reaction path keeps its visible quote.

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
                content_attributes=outbound_mirror_content_attributes(provider_message_id),
            )
            # DEBUG, not INFO: normal per-message mirroring. Failures below stay
            # at logger.exception.
            logger.debug(
                "Chatwoot mirror note posted msg_id=%s conv=%s phone=%s",
                msg_id,
                conversation_id,
                phone_e164,
            )
            message_id = _positive_chatwoot_id(msg_id)
            resolved_conversation_id = _positive_chatwoot_id(conversation_id)
            if message_id is None or resolved_conversation_id is None:
                return None
            return MirroredNote(conversation_id=resolved_conversation_id, message_id=message_id)
        except Exception:
            logger.exception("Chatwoot mirror failed (best-effort, ignored) phone=%s", phone_e164)
            return None
