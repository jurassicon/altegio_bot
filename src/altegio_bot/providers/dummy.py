from __future__ import annotations

import logging
import os
from uuid import uuid4

from altegio_bot.providers.base import ChatwootRoute, WhatsAppProvider

logger = logging.getLogger(__name__)

ALLOW_REAL_SEND_ENV = "ALLOW_REAL_SEND"
WHATSAPP_PROVIDER_ENV = "WHATSAPP_PROVIDER"
META_PROVIDER_KEY = "meta_cloud"


def _provider_key(provider: WhatsAppProvider) -> str:
    key = os.getenv(WHATSAPP_PROVIDER_ENV, "").strip().lower()
    if key:
        return key

    module_name = provider.__class__.__module__.rsplit(".", 1)[-1]
    return module_name.strip().lower()


def _real_send_allowed(provider: WhatsAppProvider) -> bool:
    if _provider_key(provider) != META_PROVIDER_KEY:
        return True
    return os.getenv(ALLOW_REAL_SEND_ENV, "0").strip() == "1"


def _supports_mirror(provider: WhatsAppProvider) -> bool:
    """Return True if the provider accepts company_id / staff_id kwargs."""
    return bool(getattr(provider, "_supports_mirror_kwargs", False))


class DummyProvider(WhatsAppProvider):
    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
        *,
        contact_name: str | None = None,
        reply_to_provider_message_id: str | None = None,
    ) -> str:
        provider_message_id = f"dummy-{uuid4()}"
        logger.info(
            "Dummy send sender_id=%s phone=%s text_len=%s msg_id=%s reply_context=%s",
            sender_id,
            phone_e164,
            len(text),
            provider_message_id,
            bool(reply_to_provider_message_id),
        )
        return provider_message_id

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
        provider_message_id = f"dummy-tpl-{uuid4()}"
        logger.info(
            "Dummy send_template sender_id=%s phone=%s template=%s lang=%s params=%s header=%s msg_id=%s",
            sender_id,
            phone_e164,
            template_name,
            language,
            params,
            header_image_url,
            provider_message_id,
        )
        return provider_message_id


async def safe_send(
    provider: WhatsAppProvider,
    sender_id: int,
    phone: str,
    text: str,
    *,
    tenant_provider: str | None = None,
    company_id: int = 0,
    staff_id: int | None = None,
    contact_name: str | None = None,
    reply_to_provider_message_id: str | None = None,
    chatwoot_route: ChatwootRoute = ChatwootRoute.TENANT,
) -> tuple[str | None, str | None]:
    if not _real_send_allowed(provider):
        return None, "Real send disabled"

    try:
        kwargs: dict[str, object] = {"contact_name": contact_name}
        if isinstance(reply_to_provider_message_id, str) and reply_to_provider_message_id.strip():
            kwargs["reply_to_provider_message_id"] = reply_to_provider_message_id.strip()
        if _supports_mirror(provider):
            kwargs["tenant_provider"] = tenant_provider
            kwargs["company_id"] = company_id
            kwargs["staff_id"] = staff_id
            kwargs["chatwoot_route"] = chatwoot_route
        msg_id = await provider.send(sender_id, phone, text, **kwargs)  # type: ignore[call-arg]
        return msg_id, None
    except Exception as exc:
        # The raw exception may carry tokens, URLs, response bodies, phone/message
        # fragments, or injection/control characters — log only its class name.
        # The raw text is still returned to the caller for in-memory
        # classification (it must never be logged or persisted downstream).
        logger.warning("provider send failed error_type=%s", type(exc).__name__)
        return None, str(exc)


async def safe_send_template(
    provider: WhatsAppProvider,
    sender_id: int,
    phone: str,
    template_name: str,
    language: str,
    params: list[str],
    fallback_text: str = "",
    *,
    tenant_provider: str | None = None,
    company_id: int = 0,
    staff_id: int | None = None,
    contact_name: str | None = None,
    header_image_url: str | None = None,
) -> tuple[str | None, str | None]:
    if not _real_send_allowed(provider):
        return None, "Real send disabled"

    try:
        kwargs: dict[str, object] = {
            "fallback_text": fallback_text,
            "contact_name": contact_name,
            "header_image_url": header_image_url,
        }
        if _supports_mirror(provider):
            kwargs["tenant_provider"] = tenant_provider
            kwargs["company_id"] = company_id
            kwargs["staff_id"] = staff_id
        msg_id = await provider.send_template(  # type: ignore[call-arg]
            sender_id, phone, template_name, language, params, **kwargs
        )
        return msg_id, None
    except Exception as exc:
        # Class name only — the raw exception may leak tokens/URLs/PII/injection.
        # The raw text is returned for in-memory classification, never logged.
        logger.warning("provider template send failed error_type=%s", type(exc).__name__)
        return None, str(exc)
