"""
Migrate existing clients from altegio_bot DB to Chatwoot contacts.

Usage:
  # Dry run (no changes)
  python -m altegio_bot.scripts.migrate_contacts_to_chatwoot --dry-run --limit 10

  # Migrate first 50 contacts
  python -m altegio_bot.scripts.migrate_contacts_to_chatwoot --limit 50

  # Migrate all contacts with last appointment in last 12 months
  python -m altegio_bot.scripts.migrate_contacts_to_chatwoot --months 12
"""

import argparse
import asyncio
import logging
from datetime import timedelta
from typing import Any

import httpx
from sqlalchemy import select
from altegio_bot.db import SessionLocal
from altegio_bot.models.models import Client, Record
from altegio_bot.settings import settings
from altegio_bot.workers.outbox_worker import utcnow

logger = logging.getLogger(__name__)


class ChatwootClientFixed:
    """Chatwoot API client with retry logic."""

    def __init__(self, base_url: str, account_id: str, inbox_id: str, api_key: str):
        self.base_url = base_url.rstrip('/')
        self.account_id = account_id
        self.inbox_id = inbox_id
        self.api_key = api_key
        self.client = httpx.AsyncClient(timeout=30.0)
        self.request_count = 0

    def _headers(self) -> dict[str, str]:
        return {'api_access_token': self.api_key}

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        max_retries: int = 3,
        **kwargs
    ) -> httpx.Response:
        """Make HTTP request with exponential backoff retry."""
        for attempt in range(max_retries):
            try:
                self.request_count += 1

                if method == 'GET':
                    response = await self.client.get(url, **kwargs)
                elif method == 'POST':
                    response = await self.client.post(url, **kwargs)
                elif method == 'PUT':
                    response = await self.client.put(url, **kwargs)
                else:
                    raise ValueError(f'Unsupported method: {method}')

                response.raise_for_status()
                return response

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:  # Rate limit
                    wait_time = 2 ** attempt  # 1s, 2s, 4s
                    logger.warning(
                        f'Rate limit hit, waiting {wait_time}s (attempt {attempt+1}/{max_retries})'
                    )
                    await asyncio.sleep(wait_time)
                elif e.response.status_code >= 500:  # Server error
                    wait_time = 2 ** attempt
                    logger.warning(
                        f'Server error {e.response.status_code}, retrying in {wait_time}s'
                    )
                    await asyncio.sleep(wait_time)
                else:
                    raise

            except (httpx.TimeoutException, httpx.ConnectError) as e:
                wait_time = 2 ** attempt
                logger.warning(
                    f'Connection error: {e}, retrying in {wait_time}s'
                )
                await asyncio.sleep(wait_time)

        raise Exception(f'Failed after {max_retries} attempts')

    async def find_contact_by_phone(self, phone_e164: str) -> dict | None:
        """Find contact by phone number."""
        search_phone = phone_e164.lstrip('+')

        url = f'{self.base_url}/api/v1/accounts/{self.account_id}/contacts/search'
        params = {'q': search_phone}

        response = await self._request_with_retry(
            'GET', url, headers=self._headers(), params=params
        )

        data = response.json()
        payload = data.get('payload', [])

        if payload and isinstance(payload, list):
            return payload[0]

        return None

    async def create_contact(
        self,
        phone_e164: str,
        name: str | None = None,
        custom_attributes: dict | None = None
    ) -> dict:
        """Create new contact."""
        url = f'{self.base_url}/api/v1/accounts/{self.account_id}/contacts'

        payload: dict[str, Any] = {
            'inbox_id': self.inbox_id,
            'phone_number': phone_e164,
        }

        if name:
            payload['name'] = name

        if custom_attributes:
            payload['custom_attributes'] = custom_attributes

        response = await self._request_with_retry(
            'POST', url, headers=self._headers(), json=payload
        )

        data = response.json()
        return data.get('payload', {}).get('contact', {})

    async def update_contact_attributes(
        self,
        contact_id: int,
        custom_attributes: dict
    ) -> dict:
        """Update contact custom attributes."""
        url = f'{self.base_url}/api/v1/accounts/{self.account_id}/contacts/{contact_id}'

        payload = {'custom_attributes': custom_attributes}

        response = await self._request_with_retry(
            'PUT', url, headers=self._headers(), json=payload
        )

        data = response.json()
        return data.get('payload', {}).get('contact', {})

    async def aclose(self):
        await self.client.aclose()


async def get_clients_with_recent_records(
    months: int = 12,
    limit: int | None = None
) -> list[Client]:
    """Get clients with appointments in last N months."""
    cutoff_date = utcnow() - timedelta(days=30 * months)

    async with SessionLocal() as session:
        stmt = (
            select(Client)
            .join(Record, Record.client_id == Client.id)
            .where(Record.starts_at >= cutoff_date)
            .where(Client.phone_e164.isnot(None))
            .where(Client.phone_e164 != '')
            .group_by(Client.id)
            .order_by(Client.id)
        )

        if limit:
            stmt = stmt.limit(limit)

        result = await session.execute(stmt)
        clients = list(result.scalars().all())

    return clients


async def get_last_appointment_date(client_id: int) -> str | None:
    """Get last appointment date for client."""
    async with SessionLocal() as session:
        stmt = (
            select(Record.starts_at)
            .where(Record.client_id == client_id)
            .order_by(Record.starts_at.desc())
            .limit(1)
        )
        result = await session.execute(stmt)
        last_appt = result.scalar_one_or_none()

        if last_appt:
            return last_appt.strftime('%Y-%m-%d')

        return None


async def migrate_one_client(
    client: Client,
    chatwoot: ChatwootClientFixed,
    dry_run: bool = False
) -> tuple[str, str]:
    """Migrate single client to Chatwoot."""

    if not client.phone_e164:
        return ('skipped', 'No phone number')

    try:
        existing = await chatwoot.find_contact_by_phone(client.phone_e164)

        custom_attrs = {
            'altegio_client_id': client.altegio_client_id or 0,
            'opted_out_status': 'yes' if client.wa_opted_out else 'no',
        }

        last_appt = await get_last_appointment_date(client.id)
        if last_appt:
            custom_attrs['last_appointment_date'] = last_appt

        if dry_run:
            if existing:
                return ('updated', f'Would update contact (existing id={existing["id"]})')
            else:
                return ('created', 'Would create new contact')

        if existing:
            await chatwoot.update_contact_attributes(
                existing['id'], custom_attrs
            )
            return ('updated', f'Updated contact id={existing["id"]}')
        else:
            contact = await chatwoot.create_contact(
                client.phone_e164,
                name=client.display_name,
                custom_attributes=custom_attrs
            )
            return ('created', f'Created contact id={contact.get("id")}')

        await asyncio.sleep(0.6)

    except Exception as e:
        return ('error', str(e))


async def migrate_contacts(
    dry_run: bool = False,
    limit: int | None = None,
    months: int = 12,
    delay_sec: float = 0.6,
) -> dict[str, int]:
    """Migrate clients to Chatwoot with rate limiting and retry."""

    if not settings.chatwoot_enabled:
        logger.error('CHATWOOT_ENABLED is false. Enable Chatwoot first.')
        logger.error('Set CHATWOOT_ENABLED=true in .env and restart containers')
        return {}

    logger.info('=== Migration Start ===')
    logger.info(f'Mode: {"DRY RUN" if dry_run else "REAL"}')
    logger.info(f'Limit: {limit or "None (all)"}')
    logger.info(f'Period: Last {months} months')
    logger.info(f'Delay: {delay_sec}s between requests')
    logger.info(f'Chatwoot URL: {settings.chatwoot_url}')
    logger.info(f'Account ID: {settings.chatwoot_account_id}')
    logger.info(f'Inbox ID: {settings.chatwoot_inbox_id}')

    chatwoot = ChatwootClientFixed(
        base_url=settings.chatwoot_url,
        account_id=settings.chatwoot_account_id,
        inbox_id=settings.chatwoot_inbox_id,
        api_key=settings.chatwoot_api_token,
    )

    logger.info('Fetching clients from database...')
    clients = await get_clients_with_recent_records(months=months, limit=limit)
    logger.info(f'Found {len(clients)} clients to migrate')

    if not clients:
        logger.warning('No clients found!')
        await chatwoot.aclose()
        return {}

    stats = {
        'total': len(clients),
        'created': 0,
        'updated': 0,
        'skipped': 0,
        'error': 0,
    }

    for i, client in enumerate(clients, 1):
        status, message = await migrate_one_client(client, chatwoot, dry_run)
        stats[status] += 1

        if i % 10 == 0 or i == len(clients):
            logger.info(
                f'Progress: {i}/{len(clients)} | '
                f'Created: {stats["created"]} | '
                f'Updated: {stats["updated"]} | '
                f'Errors: {stats["error"]} | '
                f'API calls: {chatwoot.request_count}'
            )

        log_func = logger.info if status != 'error' else logger.error
        log_func(
            f'[{i}/{len(clients)}] {client.phone_e164} ({client.display_name}): '
            f'{status} - {message}'
        )

        if i < len(clients) and not dry_run:
            await asyncio.sleep(delay_sec)

    logger.info('=== Migration Complete ===')
    logger.info(f'Total: {stats["total"]}')
    logger.info(f'Created: {stats["created"]}')
    logger.info(f'Updated: {stats["updated"]}')
    logger.info(f'Skipped: {stats["skipped"]}')
    logger.info(f'Errors: {stats["error"]}')
    logger.info(f'Total API calls: {chatwoot.request_count}')

    await chatwoot.aclose()
    return stats


async def main() -> None:
    parser = argparse.ArgumentParser(
        description='Migrate contacts to Chatwoot with retry logic'
    )
    parser.add_argument('--dry-run', action='store_true', help='Preview without changes')
    parser.add_argument('--limit', type=int, help='Max contacts to process')
    parser.add_argument('--months', type=int, default=12,
                       help='Include clients with appointments in last N months')
    parser.add_argument('--delay', type=float, default=0.6,
                       help='Delay between API requests in seconds')

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
    )

    await migrate_contacts(
        dry_run=args.dry_run,
        limit=args.limit,
        months=args.months,
        delay_sec=args.delay,
    )


if __name__ == '__main__':
    asyncio.run(main())