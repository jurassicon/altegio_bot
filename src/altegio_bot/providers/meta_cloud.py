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
    return phone_e164.lstrip("+").strip()


class MetaCloudError(RuntimeError):
    """Safe Meta Cloud API error with structured, non-PII fields."""

    def __init__(
        self,
        action: str,
        *,
        status_code: int | None,
        meta_code: str | None,
        is_transient: bool | None,
        safe_message: str | None = None,
        fbtrace_id: str | None = None,
    ) -> None:
        self.action = action
        self.status_code = status_code
        self.meta_code = meta_code
        self.is_transient = is_transient
        self.safe_message = safe_message
        self.fbtrace_id = fbtrace_id
        super().__init__(self.__str__())

    def __str__(self) -> str:
        bits = [
            f"Meta {self.action} failed",
            f"status={self.status_code}" if self.status_code is not None else "status=None",
            f"code={self.meta_code}" if self.meta_code is not None else "code=None",
            f"is_transient={str(self.is_transient).lower()}" if self.is_transient is not None else "is_transient=None",
        ]
        if self.safe_message:
            bits.append(f"message={self.safe_message}")
        if self.fbtrace_id:
            bits.append(f"fbtrace_id={self.fbtrace_id}")
        return " ".join(bits)


def _safe_meta_message_marker(err: dict[str, Any]) -> str | None:
    message = str(err.get("message") or "").lower()
    title = str(err.get("title") or "").strip()
    error_type = str(err.get("type") or "").strip()

    if "access token" in message and "expired" in message:
        return "access token expired"
    if any(
        marker in message
        for marker in (
            "number of parameters does not match",
            "does not match the expected number of params",
            "required parameter is missing",
            "template does not exist",
            "template name does not exist",
            "does not exist in the translation",
        )
    ):
        return "template validation error"
    if title:
        return title[:80]
    if error_type:
        return error_type[:80]
    return None


def _meta_error_from_response(action: str, response: Any, data: dict[str, Any]) -> MetaCloudError:
    err = data.get("error") if isinstance(data, dict) else None
    if not isinstance(err, dict):
        err = {}
    meta_code = err.get("code")
    is_transient = err.get("is_transient")
    fbtrace_id = err.get("fbtrace_id")
    return MetaCloudError(
        action,
        status_code=getattr(response, "status_code", None),
        meta_code=str(meta_code) if meta_code is not None else None,
        is_transient=bool(is_transient) if isinstance(is_transient, bool) else None,
        safe_message=_safe_meta_message_marker(err),
        fbtrace_id=str(fbtrace_id)[:80] if fbtrace_id else None,
    )


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
            access_token if access_token is not None else os.getenv("WHATSAPP_ACCESS_TOKEN", "").strip()
        )
        self._api_version = (
            api_version if api_version is not None else os.getenv("WHATSAPP_API_VERSION", "v21.0").strip()
        )
        self._graph_url = (
            graph_url
            if graph_url is not None
            else os.getenv("WHATSAPP_GRAPH_URL", "https://graph.facebook.com").strip()
        )

        self._allow_real_send = os.getenv("ALLOW_REAL_SEND", "0").strip() == "1"
        self._sender_cache: dict[int, str] = {}
        self._client = httpx.AsyncClient(timeout=timeout_sec)

        if not self._access_token:
            raise RuntimeError("WHATSAPP_ACCESS_TOKEN is not set")

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _get_phone_number_id(self, sender_id: int) -> str:
        cached = self._sender_cache.get(sender_id)
        if cached:
            return cached

        async with SessionLocal() as session:
            sender = await session.get(WhatsAppSender, sender_id)

        if sender is None:
            raise RuntimeError(f"WhatsAppSender not found: id={sender_id}")

        phone_number_id = (sender.phone_number_id or "").strip()
        if not phone_number_id:
            raise RuntimeError(f"phone_number_id is empty for sender_id={sender_id}")

        self._sender_cache[sender_id] = phone_number_id
        return phone_number_id

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._access_token}"}

    async def check_metadata(self, phone_number_id: str, *, timeout: float | None = None) -> None:
        """Read phone-number metadata as a safe Meta availability probe."""
        url = f"{self._graph_url}/{self._api_version}/{phone_number_id}"
        params = {"fields": "id,display_phone_number,verified_name"}
        res = await self._client.get(
            url,
            headers=self._headers(),
            params=params,
            timeout=timeout,
        )

        if res.status_code >= 400:
            data: dict[str, Any] = {}
            try:
                data = res.json()
            except Exception:
                data = {}
            raise _meta_error_from_response("metadata probe", res, data)

    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
        *,
        contact_name: str | None = None,
        reply_to_provider_message_id: str | None = None,
    ) -> str:
        if not self._allow_real_send:
            raise RuntimeError("Real send disabled (set ALLOW_REAL_SEND=1)")

        phone_number_id = await self._get_phone_number_id(sender_id)
        to_number = _strip_plus(phone_e164)

        url = f"{self._graph_url}/{self._api_version}/{phone_number_id}/messages"
        payload: dict[str, Any] = {
            "messaging_product": "whatsapp",
            "to": to_number,
            "type": "text",
            "text": {
                "body": text,
                "preview_url": False,
            },
        }
        if isinstance(reply_to_provider_message_id, str) and reply_to_provider_message_id.strip():
            payload["context"] = {
                "message_id": reply_to_provider_message_id.strip(),
            }

        res = await self._client.post(url, headers=self._headers(), json=payload)

        data: dict[str, Any] = {}
        try:
            data = res.json()
        except Exception:
            data = {}

        if res.status_code >= 400:
            raise _meta_error_from_response("send", res, data)

        messages = data.get("messages") if isinstance(data, dict) else None
        if isinstance(messages, list) and messages:
            first = messages[0]
            if isinstance(first, dict) and first.get("id"):
                msg_id = str(first["id"])
                logger.info(
                    "Meta send ok sender_id=%s phone=%s msg_id=%s",
                    sender_id,
                    phone_e164,
                    msg_id,
                )
                return msg_id

        raise RuntimeError(f"Unexpected Meta response: {data}")

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
        header_image_url: str | None = None,
    ) -> str:
        """Send an approved Meta template message."""
        if not self._allow_real_send:
            raise RuntimeError("Real send disabled (set ALLOW_REAL_SEND=1)")

        phone_number_id = await self._get_phone_number_id(sender_id)
        to_number = _strip_plus(phone_e164)

        components: list[dict[str, Any]] = []
        if header_image_url:
            components.append(
                {
                    "type": "header",
                    "parameters": [{"type": "image", "image": {"link": header_image_url}}],
                }
            )
        if params:
            components.append(
                {
                    "type": "body",
                    "parameters": [{"type": "text", "text": p} for p in params],
                }
            )

        url = f"{self._graph_url}/{self._api_version}/{phone_number_id}/messages"
        template_payload: dict[str, Any] = {
            "name": template_name,
            "language": {"code": language},
        }
        if components:
            template_payload["components"] = components
        payload: dict[str, Any] = {
            "messaging_product": "whatsapp",
            "to": to_number,
            "type": "template",
            "template": template_payload,
        }

        res = await self._client.post(url, headers=self._headers(), json=payload)

        data: dict[str, Any] = {}
        try:
            data = res.json()
        except Exception:
            data = {}

        if res.status_code >= 400:
            raise _meta_error_from_response("send_template", res, data)

        messages = data.get("messages") if isinstance(data, dict) else None
        if isinstance(messages, list) and messages:
            first = messages[0]
            if isinstance(first, dict) and first.get("id"):
                msg_id = str(first["id"])
                logger.info(
                    "Meta template sent sender_id=%s phone=%s template=%s msg_id=%s",
                    sender_id,
                    phone_e164,
                    template_name,
                    msg_id,
                )
                return msg_id

        raise RuntimeError(f"Unexpected Meta response for template: {data}")
