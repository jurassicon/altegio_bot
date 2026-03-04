from __future__ import annotations

import asyncio
import logging

from altegio_bot.providers.factory import get_provider
from altegio_bot.workers.outbox_worker import run_loop


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    provider = get_provider()

    try:
        await run_loop(provider=provider)
    finally:
        aclose = getattr(provider, "aclose", None)
        if callable(aclose):
            await aclose()


if __name__ == "__main__":
    asyncio.run(main())
