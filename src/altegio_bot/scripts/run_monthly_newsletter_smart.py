"""Monthly newsletter smart runner.

Modes:
  list      - display candidates (no DB writes, no sends)
  dry-run   - display candidates + show what would happen
  send-test - display candidates + send test message to --test-phone
  send-real - run real newsletter for all eligible candidates

'arrived' is determined by ``Record.attendance == 1 OR
Record.visit_attendance == 1`` which corresponds to the Altegio
'"пришёл"' (arrival confirmed) status.

Usage::

    python -m altegio_bot.scripts.run_monthly_newsletter_smart \\
      [--month YYYY-MM | --from YYYY-MM-DD --to YYYY-MM-DD] \\
      [--company-id 758285 | --company-id 758285,1271200 | --company-id all] \\
      --mode list|dry-run|send-test|send-real \\
      [--test-phone 381638400431] \\
      [--limit 200] \\
      [--booking-link https://n813709.alteg.io/] \\
      [--template kitilash_ka_newsletter_new_clients_monthly_v2] \\
      [--expect-status sent|delivered] \\
      [--timeout 180] \\
      [--cleanup] \\
      [--force] \\
      [--format table|json] \\
      [--out /tmp/candidates.json]

Exit codes:
  0 = success / PASS
  2 = failure / FAIL
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import string
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.altegio_loyalty import AltegioLoyaltyClient
from altegio_bot.db import SessionLocal
from altegio_bot.message_planner import add_job
from altegio_bot.models.models import (
    CampaignRecipient,
    CampaignRun,
    Client,
    Record,
)
from altegio_bot.scripts.run_newsletter_new_clients_monthly import (
    NEWSLETTER_JOB_TYPE,
    _prev_month_range,
)
from altegio_bot.scripts.run_test_newsletter_smart import (
    DEFAULT_CARD_TYPE_TITLE,
    run_smart_test,
)
from altegio_bot.settings import settings
from altegio_bot.utils import utcnow

logger = logging.getLogger("monthly_newsletter_smart")

CAMPAIGN_CODE = "newsletter_new_clients_monthly"
DEFAULT_TEMPLATE = "kitilash_ka_newsletter_new_clients_monthly_v2"
_LOYALTY_CARD_PREFIX = "Kundenkarte #"

# 'arrived' status documented:
# Record.attendance == 1 (Altegio "пришёл") OR
# Record.visit_attendance == 1 (visit confirmed).
# Both fields are synced from the Altegio API record payload.
ARRIVED_STATUSES_DOC = "attendance=1 OR visit_attendance=1 (Altegio arrived)"


# ---------------------------------------------------------------------------
# Candidate data type
# ---------------------------------------------------------------------------


@dataclass
class CandidateInfo:
    company_id: int
    client_id: int | None
    altegio_client_id: int | None
    display_name: str | None
    phone_e164: str | None
    total_records_in_period: int
    arrived_records_in_period: int
    is_opted_out: bool
    is_eligible: bool
    excluded_reason: str | None


# ---------------------------------------------------------------------------
# Period helpers
# ---------------------------------------------------------------------------


def _parse_period(
    month: str | None,
    from_date: str | None,
    to_date: str | None,
) -> tuple[datetime, datetime]:
    """Return (start_inclusive, end_exclusive) UTC datetimes.

    Priority: --month > --from/--to > previous calendar month.
    When --to is provided it is treated as an *inclusive* end day.
    """
    if month is not None:
        parts = month.split("-")
        year, mon = int(parts[0]), int(parts[1])
        start = datetime(year, mon, 1, tzinfo=timezone.utc)
        next_mon = mon + 1 if mon < 12 else 1
        next_yr = year if mon < 12 else year + 1
        end = datetime(next_yr, next_mon, 1, tzinfo=timezone.utc)
        return start, end

    if from_date is not None and to_date is not None:
        start = datetime.fromisoformat(from_date).replace(tzinfo=timezone.utc)
        to_dt = datetime.fromisoformat(to_date).replace(tzinfo=timezone.utc)
        end = to_dt + timedelta(days=1)
        return start, end

    return _prev_month_range()


# ---------------------------------------------------------------------------
# Company ID helpers
# ---------------------------------------------------------------------------


def _parse_company_ids(value: str) -> list[int] | None:
    """Parse --company-id value.  Returns None for 'all'."""
    v = value.strip().lower()
    if v == "all":
        return None
    return [int(x.strip()) for x in v.split(",") if x.strip()]


async def _resolve_all_company_ids(session: AsyncSession) -> list[int]:
    res = await session.execute(select(Client.company_id).distinct().order_by(Client.company_id))
    return [row[0] for row in res.all()]


# ---------------------------------------------------------------------------
# Candidate computation
# ---------------------------------------------------------------------------


async def _compute_candidates(
    session: AsyncSession,
    *,
    company_id: int,
    period_start: datetime,
    period_end: datetime,
) -> list[CandidateInfo]:
    """Compute all clients with records in the period plus their eligibility.

    'arrived' = attendance==1 OR visit_attendance==1.
    See ARRIVED_STATUSES_DOC for the authoritative definition.
    """
    logger.debug(
        "Computing candidates company=%s period=[%s, %s)",
        company_id,
        period_start.date(),
        period_end.date(),
    )

    arrived_expr = case(
        (
            (Record.attendance == 1) | (Record.visit_attendance == 1),
            1,
        ),
        else_=0,
    )

    stats_subq = (
        select(
            Record.client_id.label("client_id"),
            func.count(Record.id).label("total_records"),
            func.sum(arrived_expr).label("arrived_records"),
        )
        .where(Record.company_id == company_id)
        .where(Record.client_id.is_not(None))
        .where(Record.starts_at >= period_start)
        .where(Record.starts_at < period_end)
        .group_by(Record.client_id)
        .subquery()
    )

    stmt = (
        select(
            Client,
            func.coalesce(stats_subq.c.total_records, 0).label("total_records"),
            func.coalesce(stats_subq.c.arrived_records, 0).label("arrived_records"),
        )
        .join(stats_subq, stats_subq.c.client_id == Client.id)
        .where(Client.company_id == company_id)
        .order_by(
            Client.phone_e164.asc().nullsfirst(),
            Client.display_name.asc().nullsfirst(),
        )
    )

    res = await session.execute(stmt)
    rows = res.all()

    result: list[CandidateInfo] = []
    for row in rows:
        client = row[0]
        total = int(row.total_records or 0)
        arrived = int(row.arrived_records or 0)

        excluded_reason: str | None = None
        if client.wa_opted_out:
            excluded_reason = "opted_out"
        elif arrived > 0:
            excluded_reason = "has_arrived"
        elif total > 1:
            excluded_reason = "more_than_one_record"
        elif not client.phone_e164:
            excluded_reason = "no_phone"

        result.append(
            CandidateInfo(
                company_id=company_id,
                client_id=client.id,
                altegio_client_id=client.altegio_client_id,
                display_name=client.display_name,
                phone_e164=client.phone_e164,
                total_records_in_period=total,
                arrived_records_in_period=arrived,
                is_opted_out=client.wa_opted_out,
                is_eligible=excluded_reason is None,
                excluded_reason=excluded_reason,
            )
        )

    return result


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


def _compute_summary(
    candidates: list[CandidateInfo],
) -> dict[str, int]:
    return {
        "total_clients_seen": len(candidates),
        "candidates_count": sum(1 for c in candidates if c.is_eligible),
        "excluded_opted_out": sum(1 for c in candidates if c.excluded_reason == "opted_out"),
        "excluded_more_than_one_record": sum(1 for c in candidates if c.excluded_reason == "more_than_one_record"),
        "excluded_has_arrived": sum(1 for c in candidates if c.excluded_reason == "has_arrived"),
        "excluded_no_phone": sum(1 for c in candidates if c.excluded_reason == "no_phone"),
    }


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def _format_table(candidates: list[CandidateInfo]) -> str:
    """Return an ASCII table string for *candidates*."""
    if not candidates:
        return "  (no clients with records in this period)\n"

    headers = [
        "company_id",
        "altegio_id",
        "name",
        "phone",
        "records",
        "arrived",
        "optout",
        "eligible",
        "reason",
    ]
    rows = [
        [
            str(c.company_id),
            str(c.altegio_client_id or ""),
            (c.display_name or "")[:30],
            c.phone_e164 or "",
            str(c.total_records_in_period),
            str(c.arrived_records_in_period),
            "yes" if c.is_opted_out else "no",
            "yes" if c.is_eligible else "no",
            c.excluded_reason or "",
        ]
        for c in candidates
    ]

    widths = [max(len(headers[i]), max((len(r[i]) for r in rows), default=0)) for i in range(len(headers))]

    def _fmt(row: list[str]) -> str:
        return "  " + "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row))

    sep = "  " + "  ".join("-" * w for w in widths)
    lines = [_fmt(headers), sep] + [_fmt(r) for r in rows]
    return "\n".join(lines) + "\n"


def _print_candidates(
    all_candidates: list[CandidateInfo],
    *,
    period_start: datetime,
    period_end: datetime,
    fmt: str = "table",
    out_file: str | None = None,
) -> None:
    """Print candidates to stdout and optionally export to a file."""
    print(f"\n=== CANDIDATES period={period_start.date()}..{period_end.date()} ===")
    print(f"  arrived check: {ARRIVED_STATUSES_DOC}\n")

    if fmt == "json":
        content = json.dumps([asdict(c) for c in all_candidates], indent=2, default=str)
        print(content)
    else:
        by_company: dict[int, list[CandidateInfo]] = {}
        for c in all_candidates:
            by_company.setdefault(c.company_id, []).append(c)

        for cid in sorted(by_company):
            print(f"--- company_id={cid} ---")
            print(_format_table(by_company[cid]))

    print("=== SUMMARY ===")
    companies = sorted({c.company_id for c in all_candidates})
    for cid in companies:
        group = [c for c in all_candidates if c.company_id == cid]
        s = _compute_summary(group)
        print(f"  company_id={cid}:")
        for k, v in s.items():
            print(f"    {k:<42}: {v}")

    if len(companies) > 1:
        total = _compute_summary(all_candidates)
        print("  TOTAL across all companies:")
        for k, v in total.items():
            print(f"    {k:<42}: {v}")

    if out_file:
        with open(out_file, "w", encoding="utf-8") as fh:
            json.dump(
                [asdict(c) for c in all_candidates],
                fh,
                indent=2,
                default=str,
            )
        logger.info("Candidates written to %s", out_file)


# ---------------------------------------------------------------------------
# Campaign DB helpers
# ---------------------------------------------------------------------------


async def _create_campaign_run(
    *,
    mode: str,
    company_ids: list[int],
    period_start: datetime,
    period_end: datetime,
    all_candidates: list[CandidateInfo],
) -> int:
    """Insert a CampaignRun and return its id."""
    summary = _compute_summary(all_candidates)
    async with SessionLocal() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code=CAMPAIGN_CODE,
                mode=mode,
                company_ids=company_ids,
                period_start=period_start,
                period_end=period_end,
                status="running",
                total_clients_seen=summary["total_clients_seen"],
                candidates_count=summary["candidates_count"],
                excluded_opted_out=summary["excluded_opted_out"],
                excluded_more_than_one_record=summary["excluded_more_than_one_record"],
                excluded_has_arrived=summary["excluded_has_arrived"],
                excluded_no_phone=summary["excluded_no_phone"],
                meta={
                    "period_start": period_start.isoformat(),
                    "period_end": period_end.isoformat(),
                },
            )
            session.add(run)
            await session.flush()
            run_id = run.id
    return run_id


async def _save_recipients(
    run_id: int,
    candidates: list[CandidateInfo],
) -> None:
    """Bulk-insert CampaignRecipient rows."""
    async with SessionLocal() as session:
        async with session.begin():
            for c in candidates:
                session.add(
                    CampaignRecipient(
                        campaign_run_id=run_id,
                        company_id=c.company_id,
                        client_id=c.client_id,
                        altegio_client_id=c.altegio_client_id,
                        phone_e164=c.phone_e164,
                        display_name=c.display_name,
                        status="candidate" if c.is_eligible else "skipped",
                        excluded_reason=c.excluded_reason,
                        total_records_in_period=c.total_records_in_period,
                        arrived_records_in_period=c.arrived_records_in_period,
                        is_opted_out=c.is_opted_out,
                        meta={},
                    )
                )


async def _complete_campaign_run(
    run_id: int,
    *,
    sent: int,
    failed: int,
    status: str = "done",
) -> None:
    async with SessionLocal() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            if run is not None:
                run.status = status
                run.sent_count = sent
                run.failed_count = failed
                run.completed_at = utcnow()


# ---------------------------------------------------------------------------
# send-real helpers
# ---------------------------------------------------------------------------


async def _resolve_card_type_id(
    loyalty: AltegioLoyaltyClient,
    location_id: int,
    *,
    card_type_id: str | None,
) -> str | None:
    """Return resolved loyalty_card_type_id or None if unavailable."""
    resolved = card_type_id or settings.loyalty_card_type_id or None
    if resolved:
        return resolved
    try:
        card_types = await loyalty.get_card_types(location_id)
    except Exception as exc:
        logger.warning("Could not fetch card types: %s", exc)
        return None
    match = next(
        (t for t in card_types if isinstance(t, dict) and t.get("title") == DEFAULT_CARD_TYPE_TITLE),
        None,
    )
    if match is None and card_types:
        match = card_types[0]
    if match is None:
        return None
    return str(match.get("id") or match.get("loyalty_card_type_id") or "") or None


async def _gen_real_card_number() -> str:
    """Generate a 16-digit production card number.

    Format: ``00`` + ``YYMMDD`` + 8 random digits using
    ``secrets`` to minimise collision risk.
    (Test cards use ``99`` prefix; production cards use ``00``.)
    """
    now = utcnow()
    yymmdd = now.strftime("%y%m%d")
    random_part = "".join(secrets.choice(string.digits) for _ in range(8))
    return f"00{yymmdd}{random_part}"


# ---------------------------------------------------------------------------
# Mode implementations
# ---------------------------------------------------------------------------


async def _run_mode_list(
    all_candidates: list[CandidateInfo],
    *,
    period_start: datetime,
    period_end: datetime,
    fmt: str,
    out_file: str | None,
) -> int:
    _print_candidates(
        all_candidates,
        period_start=period_start,
        period_end=period_end,
        fmt=fmt,
        out_file=out_file,
    )
    return 0


async def _run_mode_dry_run(
    all_candidates: list[CandidateInfo],
    *,
    period_start: datetime,
    period_end: datetime,
    fmt: str,
    out_file: str | None,
    template_name: str,
    booking_link: str,
) -> int:
    _print_candidates(
        all_candidates,
        period_start=period_start,
        period_end=period_end,
        fmt=fmt,
        out_file=out_file,
    )
    eligible = [c for c in all_candidates if c.is_eligible]
    print(f"\n[dry-run] Would send {len(eligible)} message(s) via template {template_name!r}.")
    print(f"[dry-run] booking_link={booking_link!r}")
    print("[dry-run] No cards issued, no messages sent, no DB writes.\n")
    return 0


async def _run_mode_send_test(
    all_candidates: list[CandidateInfo],
    *,
    period_start: datetime,
    period_end: datetime,
    fmt: str,
    out_file: str | None,
    test_phone: str,
    company_id: int,
    booking_link: str,
    template_name: str,
    expect_status: str,
    timeout_sec: int,
    cleanup: bool,
    force: bool,
    card_type_id: str | None,
    client_name: str,
) -> int:
    # Print candidate list first
    _print_candidates(
        all_candidates,
        period_start=period_start,
        period_end=period_end,
        fmt=fmt,
        out_file=out_file,
    )
    eligible_count = sum(1 for c in all_candidates if c.is_eligible)
    print(f"\n[send-test] Eligible candidates: {eligible_count}. Sending test message to {test_phone} only.\n")

    exit_code = await run_smart_test(
        phone=test_phone,
        company_id=company_id,
        location_id=company_id,
        booking_link=booking_link,
        template_name=template_name,
        expect_status=expect_status,
        timeout_sec=timeout_sec,
        cleanup=cleanup,
        cleanup_on_fail=False,
        force=force,
        card_type_id=card_type_id,
        client_name=client_name,
    )
    return exit_code


async def _run_mode_send_real(
    all_candidates: list[CandidateInfo],
    *,
    period_start: datetime,
    period_end: datetime,
    company_ids: list[int],
    fmt: str,
    out_file: str | None,
    booking_link: str,
    template_name: str,
    limit: int | None,
    card_type_id: str | None,
) -> int:
    _print_candidates(
        all_candidates,
        period_start=period_start,
        period_end=period_end,
        fmt=fmt,
        out_file=out_file,
    )

    eligible = [c for c in all_candidates if c.is_eligible]
    if limit is not None:
        eligible = eligible[:limit]

    run_id = await _create_campaign_run(
        mode="send-real",
        company_ids=company_ids,
        period_start=period_start,
        period_end=period_end,
        all_candidates=all_candidates,
    )
    logger.info("CampaignRun created id=%s", run_id)

    await _save_recipients(run_id, all_candidates)

    sent = 0
    errors = 0

    loyalty = AltegioLoyaltyClient()
    try:
        # Cache card_type_id per location (company_id == location_id here)
        card_type_cache: dict[int, str | None] = {}

        for candidate in eligible:
            cid = candidate.company_id
            phone_e164 = candidate.phone_e164 or ""
            phone_int = int(phone_e164.lstrip("+"))

            if cid not in card_type_cache:
                card_type_cache[cid] = await _resolve_card_type_id(loyalty, cid, card_type_id=card_type_id)
            resolved_type = card_type_cache[cid]
            if not resolved_type:
                logger.error(
                    "No card type for company=%s, skipping client=%s",
                    cid,
                    candidate.client_id,
                )
                errors += 1
                continue

            card_num = await _gen_real_card_number()
            loyalty_card_text = f"{_LOYALTY_CARD_PREFIX}{card_num}"

            try:
                card_resp = await loyalty.issue_card(
                    cid,
                    loyalty_card_number=card_num,
                    loyalty_card_type_id=resolved_type,
                    phone=phone_int,
                )
            except Exception as exc:
                logger.error(
                    "Card issue failed client=%s: %s",
                    candidate.client_id,
                    exc,
                )
                errors += 1
                continue

            issued_num = str(card_resp.get("loyalty_card_number") or card_num)
            card_id = str(card_resp.get("id") or card_resp.get("loyalty_card_id") or "")
            loyalty_card_text = f"{_LOYALTY_CARD_PREFIX}{issued_num}"

            display = candidate.display_name or "KitiLash Kunde"
            template_params = [display, booking_link, loyalty_card_text]

            try:
                async with SessionLocal() as session:
                    async with session.begin():
                        await add_job(
                            session,
                            company_id=cid,
                            record_id=None,
                            client_id=candidate.client_id,
                            job_type=NEWSLETTER_JOB_TYPE,
                            run_at=utcnow(),
                            payload={
                                "kind": NEWSLETTER_JOB_TYPE,
                                "loyalty_card_text": loyalty_card_text,
                                "campaign_run_id": run_id,
                            },
                        )
                sent += 1
                logger.info(
                    "Queued newsletter job client=%s card=%s",
                    candidate.client_id,
                    issued_num,
                )
            except Exception as exc:
                logger.error(
                    "Queue job failed client=%s: %s",
                    candidate.client_id,
                    exc,
                )
                errors += 1
                continue

            # Update recipient record
            async with SessionLocal() as session:
                async with session.begin():
                    res = await session.execute(
                        select(CampaignRecipient)
                        .where(CampaignRecipient.campaign_run_id == run_id)
                        .where(CampaignRecipient.client_id == candidate.client_id)
                        .limit(1)
                    )
                    recip = res.scalar_one_or_none()
                    if recip is not None:
                        recip.loyalty_card_id = card_id
                        recip.loyalty_card_number = issued_num
                        recip.status = "sent"

    finally:
        await loyalty.aclose()

    await _complete_campaign_run(run_id, sent=sent, failed=errors)

    logger.info(
        "send-real complete: run_id=%s sent=%s errors=%s",
        run_id,
        sent,
        errors,
    )
    print(f"\n[send-real] campaign_run_id={run_id} sent={sent} errors={errors}")
    return 0 if errors == 0 else 2


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def run_monthly_newsletter_smart(
    *,
    month: str | None,
    from_date: str | None,
    to_date: str | None,
    company_id_arg: str,
    mode: str,
    test_phone: str,
    booking_link: str,
    template_name: str,
    expect_status: str,
    timeout_sec: int,
    cleanup: bool,
    force: bool,
    limit: int | None,
    output_format: str,
    out_file: str | None,
    card_type_id: str | None,
    client_name: str,
) -> int:
    period_start, period_end = _parse_period(month, from_date, to_date)
    raw_company_ids = _parse_company_ids(company_id_arg)

    logger.info(
        "Monthly newsletter smart mode=%s period=[%s, %s) company_id_arg=%r",
        mode,
        period_start.date(),
        period_end.date(),
        company_id_arg,
    )

    async with SessionLocal() as session:
        if raw_company_ids is None:
            company_ids = await _resolve_all_company_ids(session)
            if not company_ids:
                logger.error("No companies found in DB.")
                return 2
        else:
            company_ids = raw_company_ids

        all_candidates: list[CandidateInfo] = []
        for cid in company_ids:
            cands = await _compute_candidates(
                session,
                company_id=cid,
                period_start=period_start,
                period_end=period_end,
            )
            all_candidates.extend(cands)
            logger.info(
                "company=%s: %d total clients (%d eligible)",
                cid,
                len(cands),
                sum(1 for c in cands if c.is_eligible),
            )

    if mode == "list":
        return await _run_mode_list(
            all_candidates,
            period_start=period_start,
            period_end=period_end,
            fmt=output_format,
            out_file=out_file,
        )

    if mode == "dry-run":
        return await _run_mode_dry_run(
            all_candidates,
            period_start=period_start,
            period_end=period_end,
            fmt=output_format,
            out_file=out_file,
            template_name=template_name,
            booking_link=booking_link,
        )

    if mode == "send-test":
        primary_company = company_ids[0]
        return await _run_mode_send_test(
            all_candidates,
            period_start=period_start,
            period_end=period_end,
            fmt=output_format,
            out_file=out_file,
            test_phone=test_phone,
            company_id=primary_company,
            booking_link=booking_link,
            template_name=template_name,
            expect_status=expect_status,
            timeout_sec=timeout_sec,
            cleanup=cleanup,
            force=force,
            card_type_id=card_type_id,
            client_name=client_name,
        )

    if mode == "send-real":
        return await _run_mode_send_real(
            all_candidates,
            period_start=period_start,
            period_end=period_end,
            company_ids=company_ids,
            fmt=output_format,
            out_file=out_file,
            booking_link=booking_link,
            template_name=template_name,
            limit=limit,
            card_type_id=card_type_id,
        )

    logger.error("Unknown mode: %r", mode)
    return 2


async def main() -> None:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Monthly newsletter smart runner (list/dry-run/send-test/send-real).")

    # Period
    parser.add_argument(
        "--month",
        default=None,
        help="Period as YYYY-MM (e.g. 2026-01). Overrides --from/--to.",
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        default=None,
        help="Period start as YYYY-MM-DD (inclusive).",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        default=None,
        help="Period end as YYYY-MM-DD (inclusive).",
    )

    # Company
    parser.add_argument(
        "--company-id",
        default="all",
        help='Company id(s): single int, comma-separated list, or "all".',
    )

    # Mode
    parser.add_argument(
        "--mode",
        choices=["list", "dry-run", "send-test", "send-real"],
        default="list",
    )

    # send-test
    parser.add_argument(
        "--test-phone",
        default="381638400431",
        help="Recipient phone for send-test mode.",
    )

    # send / template
    parser.add_argument(
        "--booking-link",
        default="https://n813709.alteg.io/",
    )
    parser.add_argument(
        "--template",
        default=DEFAULT_TEMPLATE,
    )
    parser.add_argument(
        "--expect-status",
        choices=["sent", "delivered"],
        default="delivered",
    )
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete loyalty card after PASS (send-test) or after send-real.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore idempotency check (send-test).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max eligible candidates to process (send-real).",
    )
    parser.add_argument(
        "--card-type-id",
        default=None,
        help="Altegio loyalty_card_type_id (overrides ENV).",
    )
    parser.add_argument(
        "--client-name",
        default="KitiLash Kunde",
    )

    # Output
    parser.add_argument(
        "--format",
        dest="output_format",
        choices=["table", "json"],
        default="table",
    )
    parser.add_argument(
        "--out",
        dest="out_file",
        default=None,
        help="Export candidates to JSON file.",
    )

    args = parser.parse_args()

    exit_code = await run_monthly_newsletter_smart(
        month=args.month,
        from_date=args.from_date,
        to_date=args.to_date,
        company_id_arg=args.company_id,
        mode=args.mode,
        test_phone=args.test_phone,
        booking_link=args.booking_link,
        template_name=args.template,
        expect_status=args.expect_status,
        timeout_sec=args.timeout,
        cleanup=args.cleanup,
        force=args.force,
        limit=args.limit,
        output_format=args.output_format,
        out_file=args.out_file,
        card_type_id=args.card_type_id,
        client_name=args.client_name,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    asyncio.run(main())
