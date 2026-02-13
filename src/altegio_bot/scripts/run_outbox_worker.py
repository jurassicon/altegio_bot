from __future__ import annotations

import asyncio
import os

from altegio_bot.providers.dummy import DummyProvider
from altegio_bot.providers.meta_cloud import MetaCloudProvider
from altegio_bot.workers.outbox_worker import run_loop


def _provider_from_env():
    key = os.getenv('WHATSAPP_PROVIDER', 'dummy').strip().lower()
    if key == 'meta_cloud':
        return MetaCloudProvider()
    return DummyProvider()


async def main() -> None:
    provider = _provider_from_env()
    try:
        await run_loop(provider=provider)
    finally:
        aclose = getattr(provider, 'aclose', None)
        if callable(aclose):
            await aclose()


if __name__ == '__main__':
    asyncio.run(main())
