"""Manual cleanup script for expired promo loyalty cards.

Usage (local):
    uv run python -m altegio_bot.scripts.cleanup_expired_promo_cards

Usage (Docker):
    docker compose exec -T altegio-api python -m altegio_bot.scripts.cleanup_expired_promo_cards

Exit codes:
    0  — all eligible cards deleted (or nothing to do)
    1  — one or more card deletions failed
"""

from __future__ import annotations

import asyncio
import logging
import sys


async def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    from altegio_bot.db import SessionLocal
    from altegio_bot.promo_loyalty_cleanup import cleanup_expired_promo_loyalty_cards

    async with SessionLocal() as session:
        async with session.begin():
            result = await cleanup_expired_promo_loyalty_cards(session)

    print(f"found={result.found}")
    print(f"deleted={result.deleted}")
    print(f"failed={result.failed}")
    print(f"skipped={result.skipped}")

    return 1 if result.failed > 0 else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
