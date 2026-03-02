"""Migrate existing contacts to Chatwoot.

Creates a Chatwoot contact for every Client in the database that has a
phone number, skipping any that already exist in Chatwoot.

Usage::

    python -m altegio_bot.scripts.migrate_contacts_to_chatwoot

Environment variables required (same as the main app):
    CHATWOOT_BASE_URL, CHATWOOT_API_TOKEN, CHATWOOT_ACCOUNT_ID,
    CHATWOOT_INBOX_ID, DATABASE_URL
"""
from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select

from altegio_bot.chatwoot_client import ChatwootClient
from altegio_bot.db import SessionLocal
from altegio_bot.models.models import Client

logger = logging.getLogger(__name__)


async def migrate(batch_size: int = 100) -> None:
    client = ChatwootClient()
    try:
        offset = 0
        total = 0
        skipped = 0
        errors = 0

        while True:
            async with SessionLocal() as session:
                stmt = (
                    select(Client)
                    .where(Client.phone_e164.is_not(None))
                    .order_by(Client.id.asc())
                    .offset(offset)
                    .limit(batch_size)
                )
                res = await session.execute(stmt)
                clients = list(res.scalars().all())

            if not clients:
                break

            for c in clients:
                phone = (c.phone_e164 or '').strip()
                if not phone:
                    skipped += 1
                    continue
                try:
                    cw_id = await client.get_or_create_contact(
                        phone,
                        name=c.display_name,
                    )
                    logger.info(
                        'Migrated client_id=%s phone=%s chatwoot_id=%s',
                        c.id,
                        phone,
                        cw_id,
                    )
                    total += 1
                except Exception as exc:
                    logger.warning(
                        'Failed to migrate client_id=%s phone=%s err=%s',
                        c.id,
                        phone,
                        exc,
                    )
                    errors += 1

            offset += batch_size

        logger.info(
            'Migration complete: migrated=%s skipped=%s errors=%s',
            total,
            skipped,
            errors,
        )
    finally:
        await client.aclose()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )
    asyncio.run(migrate())


if __name__ == '__main__':
    main()
