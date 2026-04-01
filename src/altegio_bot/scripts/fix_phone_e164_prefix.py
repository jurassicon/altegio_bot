from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from sqlalchemy import func, select, update

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import (
    Client,
    ContactRateLimit,
    OutboxMessage,
)
from altegio_bot.settings import settings

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def _sleep(delay: float) -> None:
    if delay > 0:
        await asyncio.sleep(delay)


async def _cw_request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: dict[str, str],
    max_retries: int = 3,
    **kwargs: Any,
) -> httpx.Response:
    last_exc: Exception | None = None

    for attempt in range(max_retries):
        try:
            response = await client.request(
                method,
                url,
                headers=headers,
                **kwargs,
            )
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as exc:
            last_exc = exc
            status = exc.response.status_code
            if status == 429 or status >= 500:
                wait = 2**attempt
                retry_after = exc.response.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait = max(wait, int(float(retry_after)))
                    except ValueError:
                        pass
                logger.warning(
                    "Chatwoot %s – retrying in %ss (attempt %d/%d)",
                    status,
                    wait,
                    attempt + 1,
                    max_retries,
                )
                await asyncio.sleep(wait)
            else:
                raise
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            last_exc = exc
            wait = 2**attempt
            logger.warning(
                "Chatwoot connection error: %s – retrying in %ss",
                exc,
                wait,
            )
            await asyncio.sleep(wait)

    raise RuntimeError(f"Chatwoot request failed after {max_retries} attempts: {method} {url}") from last_exc


async def _find_contact(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    account_id: int,
    base_url: str,
    phone: str,
) -> dict[str, Any] | None:
    url = f"{base_url}/api/v1/accounts/{account_id}/contacts/search"
    resp = await _cw_request(
        client,
        "GET",
        url,
        headers,
        params={"q": phone},
    )
    payload = resp.json().get("payload", [])
    if not isinstance(payload, list):
        return None

    for contact in payload:
        if contact.get("phone_number") == phone:
            return contact

    return None


async def _merge_contacts(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    account_id: int,
    base_url: str,
    parent_id: int,
    child_id: int,
) -> None:
    url = f"{base_url}/api/v1/accounts/{account_id}/contacts/{parent_id}/merge"
    await _cw_request(
        client,
        "POST",
        url,
        headers,
        json={"child_id": child_id},
    )


async def _update_contact_phone(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    account_id: int,
    base_url: str,
    contact_id: int,
    phone: str,
) -> None:
    url = f"{base_url}/api/v1/accounts/{account_id}/contacts/{contact_id}"
    await _cw_request(
        client,
        "PUT",
        url,
        headers,
        json={"phone_number": phone},
    )


async def _get_contact_conversations(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    account_id: int,
    base_url: str,
    contact_id: int,
) -> list[dict[str, Any]]:
    url = f"{base_url}/api/v1/accounts/{account_id}/contacts/{contact_id}/conversations"
    resp = await _cw_request(client, "GET", url, headers)
    payload = resp.json().get("payload", [])
    if not isinstance(payload, list):
        return []
    return payload


async def _fetch_bad_phone_values_for_model(
    column: Any,
) -> list[str]:
    async with SessionLocal() as session:
        stmt = (
            select(column).where(column.isnot(None)).where(column != "").where(column.not_like("+%")).order_by(column)
        )
        result = await session.execute(stmt)
        return [value for value in result.scalars().all() if value]


async def _fetch_bad_phones(limit: int | None) -> list[str]:
    phones: list[str] = []

    phones.extend(await _fetch_bad_phone_values_for_model(Client.phone_e164))
    phones.extend(await _fetch_bad_phone_values_for_model(OutboxMessage.phone_e164))
    phones.extend(await _fetch_bad_phone_values_for_model(ContactRateLimit.phone_e164))

    unique_phones = list(dict.fromkeys(phones))
    if limit is not None:
        return unique_phones[:limit]
    return unique_phones


async def _fetch_bad_clients(
    phones: list[str] | None,
) -> list[tuple[int, str]]:
    if phones is not None and not phones:
        return []

    async with SessionLocal() as session:
        stmt = (
            select(Client.id, Client.phone_e164)
            .where(Client.phone_e164.isnot(None))
            .where(Client.phone_e164 != "")
            .where(Client.phone_e164.not_like("+%"))
            .order_by(Client.id)
        )
        if phones is not None:
            stmt = stmt.where(Client.phone_e164.in_(phones))

        result = await session.execute(stmt)
        return list(result.all())


async def _count_bad_outbox(phones: list[str] | None) -> int:
    async with SessionLocal() as session:
        stmt = (
            select(func.count())
            .select_from(OutboxMessage)
            .where(OutboxMessage.phone_e164.isnot(None))
            .where(OutboxMessage.phone_e164 != "")
            .where(OutboxMessage.phone_e164.not_like("+%"))
        )
        if phones is not None:
            if not phones:
                return 0
            stmt = stmt.where(OutboxMessage.phone_e164.in_(phones))

        result = await session.execute(stmt)
        return int(result.scalar_one())


async def _count_bad_rate_limits(phones: list[str] | None) -> int:
    async with SessionLocal() as session:
        stmt = select(func.count()).select_from(ContactRateLimit).where(ContactRateLimit.phone_e164.not_like("+%"))
        if phones is not None:
            if not phones:
                return 0
            stmt = stmt.where(ContactRateLimit.phone_e164.in_(phones))

        result = await session.execute(stmt)
        return int(result.scalar_one())


async def _apply_db_fix_clients(phones: list[str] | None) -> int:
    if phones is not None and not phones:
        return 0

    async with SessionLocal() as session:
        stmt = (
            update(Client)
            .where(Client.phone_e164.isnot(None))
            .where(Client.phone_e164 != "")
            .where(Client.phone_e164.not_like("+%"))
        )
        if phones is not None:
            stmt = stmt.where(Client.phone_e164.in_(phones))

        stmt = stmt.values(
            phone_e164=func.concat("+", Client.phone_e164),
        )
        result = await session.execute(stmt)
        await session.commit()
        return int(result.rowcount or 0)


async def _apply_db_fix_outbox(phones: list[str] | None) -> int:
    if phones is not None and not phones:
        return 0

    async with SessionLocal() as session:
        stmt = (
            update(OutboxMessage)
            .where(OutboxMessage.phone_e164.isnot(None))
            .where(OutboxMessage.phone_e164 != "")
            .where(OutboxMessage.phone_e164.not_like("+%"))
        )
        if phones is not None:
            stmt = stmt.where(OutboxMessage.phone_e164.in_(phones))

        stmt = stmt.values(
            phone_e164=func.concat("+", OutboxMessage.phone_e164),
        )
        result = await session.execute(stmt)
        await session.commit()
        return int(result.rowcount or 0)


async def _apply_db_fix_rate_limits(phones: list[str] | None) -> int:
    if phones is not None and not phones:
        return 0

    async with SessionLocal() as session:
        stmt = (
            select(ContactRateLimit.phone_e164)
            .where(ContactRateLimit.phone_e164.not_like("+%"))
            .order_by(ContactRateLimit.phone_e164)
        )
        if phones is not None:
            stmt = stmt.where(ContactRateLimit.phone_e164.in_(phones))

        result = await session.execute(stmt)
        bad_phones = list(result.scalars().all())

        fixed = 0
        for phone_no_plus in bad_phones:
            legacy = await session.get(ContactRateLimit, phone_no_plus)
            if legacy is None:
                continue

            phone_with_plus = f"+{phone_no_plus}"
            current = await session.get(ContactRateLimit, phone_with_plus)

            if current is None:
                legacy.phone_e164 = phone_with_plus
                legacy.updated_at = _utcnow()
            else:
                current.next_allowed_at = max(
                    current.next_allowed_at,
                    legacy.next_allowed_at,
                )
                current.updated_at = _utcnow()
                await session.delete(legacy)

            fixed += 1

        await session.commit()
        return fixed


async def _process_chatwoot(
    phones_without_plus: list[str],
    dry_run: bool,
    delay: float,
) -> dict[str, int]:
    stats = {
        "merged": 0,
        "updated": 0,
        "already_correct": 0,
        "manual_review": 0,
        "errors": 0,
    }

    if not settings.chatwoot_enabled:
        logger.warning("chatwoot_enabled=False — skipping Chatwoot step.")
        return stats

    account_id = settings.chatwoot_account_id
    if not account_id:
        logger.warning("chatwoot_account_id is not set — skipping Chatwoot step.")
        return stats

    base_url = settings.chatwoot_base_url.rstrip("/")
    headers = {"api_access_token": settings.chatwoot_api_token}

    async with httpx.AsyncClient(timeout=30.0) as http:
        for phone_no_plus in phones_without_plus:
            phone_with_plus = f"+{phone_no_plus}"

            try:
                contact_with = await _find_contact(
                    http,
                    headers,
                    account_id,
                    base_url,
                    phone_with_plus,
                )
                await _sleep(delay)

                contact_without = await _find_contact(
                    http,
                    headers,
                    account_id,
                    base_url,
                    phone_no_plus,
                )
                await _sleep(delay)

                if contact_with and contact_without:
                    conversations = await _get_contact_conversations(
                        http,
                        headers,
                        account_id,
                        base_url,
                        contact_without["id"],
                    )
                    await _sleep(delay)

                    open_conversations = [item for item in conversations if item.get("status") in ("open", "pending")]

                    if open_conversations:
                        logger.warning(
                            "[MANUAL REVIEW] %s (contact_id=%d) has "
                            "%d open/pending conversation(s) — "
                            "skipping automatic merge with "
                            "contact_id=%d. Verify manually before "
                            "merging.",
                            phone_no_plus,
                            contact_without["id"],
                            len(open_conversations),
                            contact_with["id"],
                        )
                        stats["manual_review"] += 1
                        continue

                    mode = "[DRY-RUN]" if dry_run else "[APPLY]"
                    logger.info(
                        "%s %s → %s: merge (child=%d → parent=%d)",
                        mode,
                        phone_no_plus,
                        phone_with_plus,
                        contact_without["id"],
                        contact_with["id"],
                    )
                    if not dry_run:
                        await _merge_contacts(
                            http,
                            headers,
                            account_id,
                            base_url,
                            parent_id=contact_with["id"],
                            child_id=contact_without["id"],
                        )
                        await _sleep(delay)
                    stats["merged"] += 1
                    continue

                if contact_without and not contact_with:
                    mode = "[DRY-RUN]" if dry_run else "[APPLY]"
                    logger.info(
                        "%s %s → %s: update phone (contact_id=%d)",
                        mode,
                        phone_no_plus,
                        phone_with_plus,
                        contact_without["id"],
                    )
                    if not dry_run:
                        await _update_contact_phone(
                            http,
                            headers,
                            account_id,
                            base_url,
                            contact_id=contact_without["id"],
                            phone=phone_with_plus,
                        )
                        await _sleep(delay)
                    stats["updated"] += 1
                    continue

                logger.debug(
                    "%s: already correct or no Chatwoot contact",
                    phone_with_plus,
                )
                stats["already_correct"] += 1

            except Exception as exc:
                logger.error(
                    "Error processing %s in Chatwoot: %s",
                    phone_no_plus,
                    exc,
                )
                stats["errors"] += 1

    return stats


def _print_verification_checklist() -> None:
    print(
        """
=== POST-RUN VERIFICATION CHECKLIST ===
Run these SQL queries to verify the fix:

1. Check clients without +:
   SELECT COUNT(*) FROM clients
   WHERE phone_e164 IS NOT NULL
     AND phone_e164 != ''
     AND phone_e164 NOT LIKE '+%';

2. Check outbox_messages without +:
   SELECT COUNT(*) FROM outbox_messages
   WHERE phone_e164 IS NOT NULL
     AND phone_e164 != ''
     AND phone_e164 NOT LIKE '+%';

3. Check contact_rate_limits without +:
   SELECT COUNT(*) FROM contact_rate_limits
   WHERE phone_e164 NOT LIKE '+%';

4. Check duplicate rate-limit pairs:
   SELECT COUNT(*)
   FROM contact_rate_limits old
   JOIN contact_rate_limits good
     ON good.phone_e164 = '+' || old.phone_e164
   WHERE old.phone_e164 NOT LIKE '+%';
"""
    )


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=("Fix phone_e164 values missing the leading + and deduplicate corresponding Chatwoot contacts."),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Preview only. Use --apply to make changes.",
    )
    parser.add_argument(
        "--apply",
        dest="dry_run",
        action="store_false",
        help="Apply DB and Chatwoot changes.",
    )
    parser.add_argument(
        "--skip-chatwoot",
        action="store_true",
        help="Skip Chatwoot deduplication.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process at most N distinct bad phones.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=3.0,
        help="Delay between Chatwoot requests in seconds.",
    )

    args = parser.parse_args()
    dry_run = args.dry_run

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )

    mode_label = "DRY-RUN" if dry_run else "APPLY"
    logger.info(
        "=== fix_phone_e164_prefix starting (mode=%s) ===",
        mode_label,
    )

    scoped_phones = await _fetch_bad_phones(args.limit)
    bad_clients = await _fetch_bad_clients(scoped_phones)
    bad_outbox_count = await _count_bad_outbox(scoped_phones)
    bad_rate_limit_count = await _count_bad_rate_limits(scoped_phones)

    logger.info(
        "Batch scope: phones=%d, clients=%d, outbox_messages=%d, contact_rate_limits=%d",
        len(scoped_phones),
        len(bad_clients),
        bad_outbox_count,
        bad_rate_limit_count,
    )

    clients_fixed = 0
    outbox_fixed = 0
    rate_limits_fixed = 0

    if dry_run:
        for client_id, phone in bad_clients:
            logger.info(
                "[DRY-RUN] client_id=%d: %s → +%s",
                client_id,
                phone,
                phone,
            )
    else:
        clients_fixed = await _apply_db_fix_clients(scoped_phones)
        outbox_fixed = await _apply_db_fix_outbox(scoped_phones)
        rate_limits_fixed = await _apply_db_fix_rate_limits(scoped_phones)

        logger.info(
            "[APPLY] DB fixed: clients=%d, outbox_messages=%d, contact_rate_limits=%d",
            clients_fixed,
            outbox_fixed,
            rate_limits_fixed,
        )

    cw_stats = {
        "merged": 0,
        "updated": 0,
        "already_correct": 0,
        "manual_review": 0,
        "errors": 0,
    }
    if not args.skip_chatwoot and scoped_phones:
        logger.info(
            "--- Step 2: Chatwoot deduplication (%d phones) ---",
            len(scoped_phones),
        )
        cw_stats = await _process_chatwoot(
            scoped_phones,
            dry_run=dry_run,
            delay=args.delay,
        )
    elif args.skip_chatwoot:
        logger.info("Chatwoot step skipped (--skip-chatwoot).")
    else:
        logger.info("No bad phones left to process.")

    print()
    print("=== SUMMARY ===")
    if dry_run:
        print(f"  [DRY-RUN] phones in batch:         {len(scoped_phones)}")
        print(f"  [DRY-RUN] clients to fix:          {len(bad_clients)}")
        print(f"  [DRY-RUN] outbox_messages to fix:  {bad_outbox_count}")
        print(f"  [DRY-RUN] contact_rate_limits to fix:  {bad_rate_limit_count}")
    else:
        print(f"  phones in batch:           {len(scoped_phones)}")
        print(f"  clients fixed:             {clients_fixed}")
        print(f"  outbox_messages fixed:     {outbox_fixed}")
        print(f"  contact_rate_limits fixed: {rate_limits_fixed}")

    print(f"  Chatwoot merged:           {cw_stats['merged']}")
    print(f"  Chatwoot phone updated:    {cw_stats['updated']}")
    print(f"  Chatwoot already correct:  {cw_stats['already_correct']}")
    print(f"  Chatwoot manual review:    {cw_stats['manual_review']}")
    print(f"  Errors:                    {cw_stats['errors']}")

    _print_verification_checklist()


if __name__ == "__main__":
    asyncio.run(main())
