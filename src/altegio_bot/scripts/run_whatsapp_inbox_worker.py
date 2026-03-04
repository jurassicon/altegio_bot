from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from altegio_bot.providers.dummy import DummyProvider
from altegio_bot.providers.meta_cloud import MetaCloudProvider
from altegio_bot.workers.whatsapp_inbox_worker import run_loop


def _build_provider() -> Any:
    key = os.getenv("WHATSAPP_PROVIDER", "dummy").strip().lower()

    if key == "meta_cloud":
        return MetaCloudProvider()

    if key == "chatwoot_hybrid":
        from altegio_bot.providers.chatwoot_hybrid import ChatwootHybridProvider

        return ChatwootHybridProvider()

    return DummyProvider()


async def _amain() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    provider = _build_provider()

    try:
        await run_loop(provider=provider)
    finally:
        aclose = getattr(provider, "aclose", None)
        if callable(aclose):
            await aclose()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
