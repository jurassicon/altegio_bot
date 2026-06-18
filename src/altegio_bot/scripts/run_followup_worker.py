"""Entrypoint: follow-up auto-run worker.

Запускает бесконечный цикл, который находит completed send-real campaign run'ы
с истёкшим followup_delay_days и автоматически выполняет plan_followup +
execute_followup.

Запуск:
  python -m altegio_bot.scripts.run_followup_worker
или через docker-compose:
  command: ["/app/.venv/bin/python", "-m", "altegio_bot.scripts.run_followup_worker"]
"""

from __future__ import annotations

import asyncio
import logging

from altegio_bot.workers.followup_worker import run_loop


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    await run_loop()


if __name__ == "__main__":
    asyncio.run(main())
