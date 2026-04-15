"""Entrypoint: campaign execution worker.

Запускает бесконечный цикл, который забирает и исполняет
campaign_execute_new_clients_monthly jobs из message_jobs.

Запуск:
  python -m altegio_bot.scripts.run_campaign_worker
или через docker-compose:
  command: ["/app/.venv/bin/python", "-m", "altegio_bot.scripts.run_campaign_worker"]
"""

from __future__ import annotations

import asyncio
import logging

from altegio_bot.workers.campaign_worker import run_loop


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    await run_loop()


if __name__ == "__main__":
    asyncio.run(main())
