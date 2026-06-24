"""Chatwoot API client – thin async wrapper around the Chatwoot REST API.

Only the methods required for the dual-write integration are implemented:
- get_or_create_contact      – upsert a contact by phone number
- get_or_create_conversation – open/reuse a conversation for a contact
- send_message               – post an outbound message to a conversation
- mirror_outbound_as_note    – mirror outbound message as a private agent note
"""

from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Mapping
from typing import Any

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from altegio_bot.chatwoot_headers import normalize_forwarded_proto
from altegio_bot.settings import settings

logger = logging.getLogger(__name__)


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


# ---------------------------------------------------------------------------
# Post-create content_attributes persistence (Chatwoot DB)
#
# Chatwoot's REST create-message endpoint persists ``content_attributes`` as a
# JSON *string* even when the HTTP body is a nested JSON object, which leaves
# native reply/reaction context unusable (``content_attributes ->> 'in_reply_to'``
# is NULL). After a successful create + returned message id, a best-effort,
# idempotent UPDATE rewrites that single message's column to a real JSON object.
#
# Module-level globals (not @lru_cache) so tests can reset state via monkeypatch
# and runtime can rebuild the engine when ``settings.chatwoot_db_url`` changes.
# ---------------------------------------------------------------------------

# Lazily-created async engine for the Chatwoot DB, keyed by URL. Stays None until
# first use with a configured settings.chatwoot_db_url. NEVER log these values
# directly: they contain a DSN with credentials.
_chatwoot_db_engine: AsyncEngine | None = None
_chatwoot_db_engine_url: str | None = None
_chatwoot_db_engine_error_url: str | None = None
_chatwoot_db_engine_error_type: str | None = None

# Runtime failure state: a syntactically valid URL can build an engine that then
# fails later on engine.begin()/execute (dead/unreachable DB). Track consecutive
# failures per exact URL and arm a short cooldown once a threshold is reached.
_chatwoot_db_runtime_error_url: str | None = None
_chatwoot_db_runtime_error_until: float = 0.0
_chatwoot_db_runtime_error_type: str | None = None
_chatwoot_db_runtime_error_count: int = 0


# Idempotent normalization for a single just-created Chatwoot message: when the
# create-message endpoint stored content_attributes as a JSON *string*, unwrap it
# back to a real JSON object so ``content_attributes ->> 'in_reply_to'`` resolves.
# The ``jsonb_typeof(...) = 'string'`` guard makes re-runs a no-op and leaves real
# objects and NULLs untouched. The ``::jsonb`` casts make it work whether the
# Chatwoot column type is json or jsonb (in Irida it was json, so a plain
# jsonb_typeof(content_attributes) failed — the cast is required).
_NORMALIZE_CHATWOOT_CONTENT_ATTRIBUTES_SQL = text(
    "UPDATE messages "
    "SET content_attributes = ((content_attributes::jsonb) #>> '{}')::jsonb "
    "WHERE id = :message_id "
    "AND content_attributes IS NOT NULL "
    "AND jsonb_typeof(content_attributes::jsonb) = 'string'"
)


def _chatwoot_db_url() -> str:
    return (settings.chatwoot_db_url or "").strip()


def _chatwoot_db_runtime_failure_threshold() -> int:
    return max(1, int(settings.chatwoot_db_runtime_failure_threshold))


def _chatwoot_db_runtime_failure_cooldown_seconds() -> float:
    return max(0.0, float(settings.chatwoot_db_runtime_failure_cooldown_seconds))


def _chatwoot_db_runtime_failure_active(url: str) -> bool:
    return bool(url and _chatwoot_db_runtime_error_url == url and time.monotonic() < _chatwoot_db_runtime_error_until)


def _record_chatwoot_db_runtime_failure(url: str, error_type: str) -> tuple[int, bool]:
    """Record a consecutive runtime failure for *url*; arm cooldown at threshold.

    Returns (failure_count, cooldown_armed). Failures are counted per exact URL;
    a URL change resets the counter so the new URL is retried immediately.
    """
    global _chatwoot_db_runtime_error_count, _chatwoot_db_runtime_error_type
    global _chatwoot_db_runtime_error_until, _chatwoot_db_runtime_error_url
    if not url:
        return 0, False
    if _chatwoot_db_runtime_error_url != url:
        _chatwoot_db_runtime_error_url = url
        _chatwoot_db_runtime_error_until = 0.0
        _chatwoot_db_runtime_error_count = 0
    _chatwoot_db_runtime_error_count += 1
    _chatwoot_db_runtime_error_type = error_type
    threshold = _chatwoot_db_runtime_failure_threshold()
    cooldown_armed = _chatwoot_db_runtime_error_count >= threshold
    if cooldown_armed:
        _chatwoot_db_runtime_error_until = time.monotonic() + _chatwoot_db_runtime_failure_cooldown_seconds()
    return _chatwoot_db_runtime_error_count, cooldown_armed


def _clear_chatwoot_db_runtime_failure(url: str) -> None:
    """Clear runtime failure/cooldown state after a successful normalization."""
    global _chatwoot_db_runtime_error_count, _chatwoot_db_runtime_error_type
    global _chatwoot_db_runtime_error_until, _chatwoot_db_runtime_error_url
    if _chatwoot_db_runtime_error_url != url:
        return
    _chatwoot_db_runtime_error_url = None
    _chatwoot_db_runtime_error_until = 0.0
    _chatwoot_db_runtime_error_type = None
    _chatwoot_db_runtime_error_count = 0


def _get_chatwoot_db_engine() -> AsyncEngine | None:
    """Return a cached async engine for the Chatwoot DB, or None when unconfigured.

    Lazy and cached: the engine is created on first use with a configured
    ``settings.chatwoot_db_url`` and reused while the URL is unchanged. Returns
    None when the URL is empty (safe no-op) or when it previously failed to build
    (warned once, then silent until the URL changes). A malformed URL can raise
    immediately from ``create_async_engine``; that is caught and disables the
    path without ever logging the URL/credentials.
    """
    global _chatwoot_db_engine, _chatwoot_db_engine_url
    global _chatwoot_db_engine_error_type, _chatwoot_db_engine_error_url
    url = _chatwoot_db_url()
    if not url:
        return None
    if _chatwoot_db_engine is not None and _chatwoot_db_engine_url == url:
        return _chatwoot_db_engine
    if _chatwoot_db_engine_error_url == url:
        return None
    # URL is new or changed: (re)build the engine. We intentionally do not dispose
    # a previous engine here — best-effort fix; URL changes are rare (mainly tests).
    # TODO: dispose the superseded engine if this ever rotates URLs at runtime.
    try:
        _chatwoot_db_engine = create_async_engine(
            url,
            pool_pre_ping=True,
            pool_size=2,
            max_overflow=2,
            pool_timeout=settings.chatwoot_db_pool_timeout_seconds,
            connect_args={"timeout": settings.chatwoot_db_connect_timeout_seconds},
        )
    except Exception as exc:  # noqa: BLE001 - best-effort normalization must never break sends
        _chatwoot_db_engine = None
        _chatwoot_db_engine_url = None
        _chatwoot_db_engine_error_url = url
        _chatwoot_db_engine_error_type = type(exc).__name__
        logger.warning(
            "chatwoot: content_attributes normalization disabled: invalid chatwoot_db_url error=%s",
            _chatwoot_db_engine_error_type,
        )
        return None
    _chatwoot_db_engine_url = url
    _chatwoot_db_engine_error_url = None
    _chatwoot_db_engine_error_type = None
    return _chatwoot_db_engine


def _normalize_content_attributes(content_attributes: Any) -> dict[str, Any]:
    """Return content_attributes as a JSON object (dict) payload for Chatwoot.

    Ensures the Python client never sends a JSON *string* and that the post-create
    persistence hook always receives a dict.

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

        ``content_attributes`` is normalized to a JSON object and forwarded to
        Chatwoot when provided (used for native reply rendering via
        ``in_reply_to`` / ``in_reply_to_external_id``).  It is omitted entirely
        when ``None`` so existing behavior is unchanged.

        When ``content_attributes`` is provided, a best-effort post-create
        normalization (:meth:`_persist_native_content_attributes`) rewrites the
        just-created message's column in the Chatwoot DB to a real JSON object,
        because the create endpoint otherwise persists it as a JSON *string*.
        That step never raises and never blocks delivery.
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

        if normalized_attributes is not None:
            # The REST create endpoint persists content_attributes as a JSON
            # string; rewrite this one message's column to a real JSON object so
            # native reply/reaction context (in_reply_to) is usable. Best-effort:
            # never raises, never blocks the send.
            await self._persist_native_content_attributes(
                message_id,
                conversation_id,
                normalized_attributes,
            )

        return message_id

    async def _persist_native_content_attributes(
        self,
        message_id: int,
        conversation_id: int,
        content_attributes: dict[str, Any],
    ) -> None:
        """Best-effort: normalize a just-created message's content_attributes.

        Chatwoot's create-message endpoint persists ``content_attributes`` as a
        JSON *string* even though we POST a nested JSON object, which leaves
        native reply context unusable (``content_attributes ->> 'in_reply_to'``
        is NULL). This rewrites that single message's column to a real JSON
        object via a direct, idempotent UPDATE against the Chatwoot database.

        Guarantees:
        - Silent DEBUG no-op when ``settings.chatwoot_db_url`` is empty: an
          unconfigured URL is the documented "disabled" state, so it must never
          emit WARNING/INFO (would spam on every native reply/reaction in
          Meta-direct / local environments).
        - A *malformed* configured URL still surfaces a single WARNING (emitted
          once per URL by :func:`_get_chatwoot_db_engine`), never the URL itself.
        - Idempotent: only rewrites rows still stored as a JSON string; real
          objects and NULLs are untouched, so re-runs do nothing.
        - Never raises: any failure is logged and swallowed so message creation
          and WhatsApp delivery are never affected.
        - Scoped to exactly one message id — never bulk/historical.

        Uses the *Chatwoot* DB via ``settings.chatwoot_db_url`` — never the
        altegio_bot application DB session. Never logs the DB URL/credentials or
        the content_attributes values (keys only).
        """
        url = _chatwoot_db_url()
        if not url:
            # Documented safe no-op: DB normalization is disabled. DEBUG only so
            # Meta-direct / local / not-yet-configured deploys do not spam a
            # WARNING on every native reply/reaction send.
            logger.debug("chatwoot: content_attributes normalization disabled: chatwoot_db_url not configured")
            return
        try:
            if _chatwoot_db_runtime_failure_active(url):
                logger.debug(
                    "chatwoot: content_attributes normalization skipped during temporary DB cooldown "
                    "message_id=%s conversation_id=%s error=%s",
                    message_id,
                    conversation_id,
                    _chatwoot_db_runtime_error_type or "unknown",
                )
                return
            engine = _get_chatwoot_db_engine()
            if engine is None:
                # Malformed/error URL: _get_chatwoot_db_engine already logged a
                # single WARNING for this URL (and stays silent on repeats), so
                # keep this per-send line at DEBUG to avoid warning spam.
                logger.debug(
                    "chatwoot: content_attributes normalization skipped: chatwoot_db_url unavailable "
                    "message_id=%s conversation_id=%s",
                    message_id,
                    conversation_id,
                )
                return
            async with engine.begin() as conn:
                await conn.execute(
                    _NORMALIZE_CHATWOOT_CONTENT_ATTRIBUTES_SQL,
                    {"message_id": int(message_id)},
                )
        except Exception as exc:  # noqa: BLE001 - best-effort, must never break send
            error_type = type(exc).__name__
            failure_count, cooldown_armed = _record_chatwoot_db_runtime_failure(url, error_type)
            if cooldown_armed:
                logger.warning(
                    "chatwoot: content_attributes normalization temporarily disabled after repeated DB errors "
                    "message_id=%s conversation_id=%s error=%s failure_count=%s cooldown_seconds=%s",
                    message_id,
                    conversation_id,
                    error_type,
                    failure_count,
                    _chatwoot_db_runtime_failure_cooldown_seconds(),
                )
            else:
                logger.warning(
                    "chatwoot: content_attributes normalization failed (ignored) "
                    "message_id=%s conversation_id=%s error=%s failure_count=%s threshold=%s",
                    message_id,
                    conversation_id,
                    error_type,
                    failure_count,
                    _chatwoot_db_runtime_failure_threshold(),
                )
            return
        _clear_chatwoot_db_runtime_failure(url)
        # DEBUG, not INFO: a configured + healthy Chatwoot DB normalizes on every
        # native reply/reaction, so successful normal operation must not add log
        # noise. Keys only — never the content_attributes values, never the DSN.
        logger.debug(
            "chatwoot: content_attributes normalized to JSON object message_id=%s conversation_id=%s keys=%s",
            message_id,
            conversation_id,
            sorted(content_attributes.keys()),
        )

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
