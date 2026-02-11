from __future__ import annotations

import os

from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.providers.dummy import DummyProvider
from altegio_bot.providers.meta_cloud import MetaCloudProvider


def get_provider() -> WhatsAppProvider:
    name = os.getenv("WHATSAPP_PROVIDER", "dummy").strip().lower()
    if name == "meta_cloud":
        return MetaCloudProvider()
    return DummyProvider()
