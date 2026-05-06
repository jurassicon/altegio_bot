"""One-off repair script: schedule follow-up jobs for a specific campaign run.

Usage (dry-run, default — nothing is written to the DB):
    python -m altegio_bot.scripts.repair_schedule_campaign_followups --campaign-run-id 21 --dry-run

Usage (apply — creates MessageJob rows and updates CampaignRecipient):
    python -m altegio_bot.scripts.repair_schedule_campaign_followups --campaign-run-id 21 --apply
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.followup import (
    _READ_OR_LATER_STATUSES,
    check_followup_final_eligibility,
)
from altegio_bot.campaigns.runner import FOLLOWUP_JOB_TYPE
from altegio_bot.db import SessionLocal
from altegio_bot.models.models import CampaignRecipient, CampaignRun, MessageJob
from altegio_bot.utils import utcnow

_TERMINAL_FOLLOWUP_STATUSES: frozenset[str] = frozenset(
    {
        "followup_queued",
        "followup_sent",
        "followup_skipped",
        "followup_canceled",
        "skipped_read",
        "skipped_booked_after",
        "skipped_opted_out",
        "skipped_future_record",
    }
)


@dataclass
class _RecipientRow:
    recipient_id: int
    display_name: str | None
    phone_e164: str | None
    status: str
    sent_at: datetime | None
    read_at: datetime | None
    booked_after_at: datetime | None
    decision: str  # "create" or "skip"
    run_at: datetime | None
    reason: str | None


@dataclass
class RepairStats:
    campaign_run_id: int
    dry_run: bool
    total_recipients: int = 0
    sent_recipients: int = 0
    candidates: int = 0
    created: int = 0
    skipped_read: int = 0
    skipped_booked_after: int = 0
    skipped_future_record: int = 0
    skipped_opted_out: int = 0
    skipped_existing_job: int = 0
    skipped_missing_send: int = 0
    skipped_other: int = 0
    rows: list[_RecipientRow] = field(default_factory=list)


def _repair_dedupe_key(run_id: int, recipient_id: int) -> str:
    return f"campaign_followup:{run_id}:{recipient_id}"


def _make_row(
    recipient: CampaignRecipient,
    *,
    decision: str,
    run_at: datetime | None,
    reason: str | None,
) -> _RecipientRow:
    return _RecipientRow(
        recipient_id=recipient.id,
        display_name=recipient.display_name,
        phone_e164=recipient.phone_e164,
        status=recipient.status,
        sent_at=recipient.sent_at,
        read_at=recipient.read_at,
        booked_after_at=recipient.booked_after_at,
        decision=decision,
        run_at=run_at,
        reason=reason,
    )


async def schedule_followups(
    run_id: int,
    dry_run: bool,
    *,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> RepairStats:
    """Schedule follow-up jobs for all eligible recipients in a campaign run.

    Pass ``session_factory`` in tests to avoid touching the real database.
    """
    factory = session_factory or SessionLocal
    now = utcnow()

    async with factory() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            if run is None:
                print(f"ERROR: CampaignRun {run_id} not found", file=sys.stderr)
                sys.exit(2)

            errors: list[str] = []
            if run.mode != "send-real":
                errors.append(f"mode={run.mode!r} (expected 'send-real')")
            if not run.followup_enabled:
                errors.append("followup_enabled=False")
            if run.followup_delay_days is None:
                errors.append("followup_delay_days is None")
            if not run.followup_policy:
                errors.append("followup_policy is None/empty")
            if not run.followup_template_name:
                errors.append("followup_template_name is None/empty")
            if errors:
                for e in errors:
                    print(f"ERROR: run {run_id}: {e}", file=sys.stderr)
                sys.exit(2)

            delay: int = run.followup_delay_days  # type: ignore[assignment]  # validated above

            stmt = select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run_id)
            recipients = (await session.execute(stmt)).scalars().all()

            stats = RepairStats(campaign_run_id=run_id, dry_run=dry_run)
            stats.total_recipients = len(recipients)

            candidates: list[tuple[CampaignRecipient, datetime]] = []

            for recipient in recipients:
                # --- Fast skips (in-memory, no DB round-trip) ---

                if recipient.status == "skipped":
                    stats.skipped_other += 1
                    continue

                # Recipient was in the send pipeline.
                stats.sent_recipients += 1

                if recipient.status in _READ_OR_LATER_STATUSES:
                    stats.skipped_read += 1
                    stats.rows.append(_make_row(recipient, decision="skip", run_at=None, reason="read_or_later_status"))
                    continue

                if recipient.read_at is not None:
                    stats.skipped_read += 1
                    stats.rows.append(_make_row(recipient, decision="skip", run_at=None, reason="read_at_set"))
                    continue

                if recipient.booked_after_at is not None:
                    stats.skipped_booked_after += 1
                    stats.rows.append(_make_row(recipient, decision="skip", run_at=None, reason="booked_after_at_set"))
                    continue

                if recipient.provider_message_id is None:
                    stats.skipped_missing_send += 1
                    stats.rows.append(
                        _make_row(recipient, decision="skip", run_at=None, reason="no_provider_message_id")
                    )
                    continue

                if recipient.followup_message_job_id is not None:
                    stats.skipped_existing_job += 1
                    stats.rows.append(_make_row(recipient, decision="skip", run_at=None, reason="followup_job_id_set"))
                    continue

                if recipient.followup_status in _TERMINAL_FOLLOWUP_STATUSES:
                    stats.skipped_existing_job += 1
                    stats.rows.append(
                        _make_row(
                            recipient,
                            decision="skip",
                            run_at=None,
                            reason=f"terminal_followup_status:{recipient.followup_status}",
                        )
                    )
                    continue

                # --- Dedupe: check for an existing MessageJob by payload ---
                existing_job_id = await session.scalar(
                    select(MessageJob.id)
                    .where(MessageJob.job_type == FOLLOWUP_JOB_TYPE)
                    .where(MessageJob.payload["campaign_run_id"].as_integer() == run_id)
                    .where(MessageJob.payload["campaign_recipient_id"].as_integer() == recipient.id)
                    .limit(1)
                )
                if existing_job_id is not None:
                    stats.skipped_existing_job += 1
                    stats.rows.append(_make_row(recipient, decision="skip", run_at=None, reason="existing_job_in_db"))
                    continue

                # --- Calculate run_at ---
                if recipient.sent_at is not None:
                    run_at = recipient.sent_at + timedelta(days=delay)
                elif run.completed_at is not None:
                    run_at = run.completed_at + timedelta(days=delay)
                else:
                    print(
                        f"  SKIP recipient_id={recipient.id}: no sent_at and run has no completed_at",
                    )
                    stats.skipped_missing_send += 1
                    stats.rows.append(
                        _make_row(
                            recipient,
                            decision="skip",
                            run_at=None,
                            reason="no_sent_at_or_completed_at",
                        )
                    )
                    continue

                # --- Final eligibility guard ---
                eligibility = await check_followup_final_eligibility(session, recipient, run, now)
                if not eligibility.eligible:
                    reason = eligibility.skip_reason or "ineligible"
                    fs = eligibility.followup_status
                    if fs == "skipped_read":
                        stats.skipped_read += 1
                    elif fs == "skipped_booked_after":
                        stats.skipped_booked_after += 1
                    elif fs == "skipped_opted_out":
                        stats.skipped_opted_out += 1
                    elif fs == "skipped_future_record":
                        stats.skipped_future_record += 1
                    else:
                        stats.skipped_other += 1
                    print(f"  SKIP recipient_id={recipient.id} reason={reason!r}")

                    # Apply: persist eligibility outcome on recipient so that
                    # e.g. Anna gets followup_status='skipped_future_record'
                    # and booked_after_at is backfilled from the AltegioEvent.
                    if not dry_run:
                        recipient.followup_status = fs or "followup_skipped"
                        if eligibility.booked_after_at is not None and recipient.booked_after_at is None:
                            recipient.booked_after_at = eligibility.booked_after_at

                    stats.rows.append(_make_row(recipient, decision="skip", run_at=run_at, reason=reason))
                    continue

                stats.candidates += 1
                candidates.append((recipient, run_at))
                stats.rows.append(_make_row(recipient, decision="create", run_at=run_at, reason=None))

            # --- Write phase (--apply only) ---
            if not dry_run:
                for recipient, run_at in candidates:
                    payload: dict = {
                        "kind": FOLLOWUP_JOB_TYPE,
                        "campaign_run_id": run.id,
                        "campaign_recipient_id": recipient.id,
                        "template_name": run.followup_template_name,
                    }
                    if not recipient.client_id and recipient.phone_e164:
                        payload["phone_e164"] = recipient.phone_e164
                        if recipient.display_name:
                            payload["contact_name"] = recipient.display_name

                    dedupe_key = _repair_dedupe_key(run.id, recipient.id)

                    upsert = pg_insert(MessageJob).values(
                        company_id=recipient.company_id,
                        record_id=None,
                        client_id=recipient.client_id,
                        job_type=FOLLOWUP_JOB_TYPE,
                        run_at=run_at,
                        status="queued",
                        last_error=None,
                        dedupe_key=dedupe_key,
                        payload=payload,
                        locked_at=None,
                        max_attempts=5,
                    )
                    upsert = upsert.on_conflict_do_update(
                        index_elements=[MessageJob.dedupe_key],
                        set_={
                            "status": "queued",
                            "last_error": None,
                            "locked_at": None,
                            "payload": upsert.excluded.payload,
                        },
                        where=MessageJob.status.in_(("canceled", "failed")),
                    )
                    await session.execute(upsert)

                    job = await session.scalar(select(MessageJob).where(MessageJob.dedupe_key == dedupe_key))

                    recipient.followup_status = "followup_queued"
                    if job is not None:
                        recipient.followup_message_job_id = job.id

                    stats.created += 1

    return stats


def _print_summary(stats: RepairStats) -> None:
    mode_label = "DRY RUN" if stats.dry_run else "APPLY"
    width = 60
    print(f"\n{'=' * width}")
    print(f"Campaign Follow-up Repair — {mode_label}")
    print(f"{'=' * width}")
    print(f"  campaign_run_id       : {stats.campaign_run_id}")
    print(f"  total_recipients      : {stats.total_recipients}")
    print(f"  sent_recipients       : {stats.sent_recipients}")
    print(f"  candidates            : {stats.candidates}")
    print(f"  created               : {stats.created}")
    print(f"  skipped_read          : {stats.skipped_read}")
    print(f"  skipped_booked_after  : {stats.skipped_booked_after}")
    print(f"  skipped_future_record : {stats.skipped_future_record}")
    print(f"  skipped_opted_out     : {stats.skipped_opted_out}")
    print(f"  skipped_existing_job  : {stats.skipped_existing_job}")
    print(f"  skipped_missing_send  : {stats.skipped_missing_send}")
    print(f"  skipped_other         : {stats.skipped_other}")

    if stats.rows:
        cols = 150
        print(f"\n{'─' * cols}")
        print(
            f"{'id':>8}  {'display_name':<20}  {'phone':<15}"
            f"  {'status':<22}  {'sent_at':<19}  {'read_at':<10}"
            f"  {'booked':<10}  {'dec':<6}  {'run_at':<19}  reason"
        )
        print("─" * cols)
        for row in stats.rows:
            sent = str(row.sent_at)[:19] if row.sent_at else "—"
            read = str(row.read_at)[:10] if row.read_at else "—"
            booked = str(row.booked_after_at)[:10] if row.booked_after_at else "—"
            rat = str(row.run_at)[:19] if row.run_at else "—"
            name = (row.display_name or "")[:20]
            phone = (row.phone_e164 or "")[:15]
            reason = (row.reason or "")[:40]
            print(
                f"{row.recipient_id:>8}  {name:<20}  {phone:<15}"
                f"  {row.status:<22}  {sent:<19}  {read:<10}"
                f"  {booked:<10}  {row.decision:<6}  {rat:<19}  {reason}"
            )
        print("─" * cols)

    action = "Would create" if stats.dry_run else "Created"
    count = stats.candidates if stats.dry_run else stats.created
    print(f"\n{action} {count} follow-up job(s).")

    if stats.dry_run and stats.candidates > 0:
        print("\nRe-run with --apply to persist changes.")


async def _main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Repair: schedule follow-up jobs for a campaign run.",
    )
    parser.add_argument("--campaign-run-id", type=int, required=True, metavar="ID")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=True,
        help="Preview only — nothing is written to the database (default).",
    )
    group.add_argument(
        "--apply",
        dest="dry_run",
        action="store_false",
        help="Create MessageJob rows and update CampaignRecipient statuses.",
    )
    args = parser.parse_args(argv)

    stats = await schedule_followups(args.campaign_run_id, dry_run=args.dry_run)
    _print_summary(stats)


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
