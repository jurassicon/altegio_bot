"""PR-9: prove the queued review requests before opening the send fence.

``EASYWEEK_REVIEW_SEND_ENABLED`` starts closed, so real ``review_3d`` jobs
accumulate as ``queued`` without a single message going out. This command is
what earns the right to open it: it takes the REAL jobs production is holding
and asks whether each one would be provably sendable right now.

The verdict has to come from the runtime's own rules, not from a second
implementation that reads similarly. So the identity, tenancy, category,
opt-out, link and deadline checks are the very functions the outbox worker
calls — imported narrowly, the way the reminder preflight already does. A
preflight that judged any of this differently would bless a queue the worker
then refuses, or, far worse, the other way round.

Two things it checks that the send path reaches only later, and that are worth
knowing BEFORE the fence opens rather than one job at a time afterwards: that
exactly one active EasyWeek template row exists for this branch and code and
matches the source-controlled contract byte for byte, and that the branch has
its own active sender. Both are configuration, both are silent until a send,
and both are exactly what a rollout gets wrong.

Strictly read-only. It selects, and it never writes: no job, no event, no
record, no outbox row, no Meta call, no Chatwoot message, no EasyWeek API call
of any kind. The session is never committed.

Green is narrow on purpose: at least one candidate, nothing truncated, and every
candidate proven. Anything else exits non-zero, including an unexpected error —
"we could not check" must never read as "everything is fine".

Output is counts, stable reason codes and technical ids. No booking uuid, no
hash, no review URL, no dedupe key, no name, phone, e-mail, service text or
price ever reaches stdout: this output is read in a terminal and pasted into
tickets.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_branches import branch_template_contract
from altegio_bot.easyweek_policy import REVIEW_3D
from altegio_bot.easyweek_service_category import evaluate_service_category
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    Client,
    MessageJob,
    MessageTemplate,
    Record,
    WhatsAppSender,
)
from altegio_bot.settings import settings

DEFAULT_LIMIT: Final = 200
# Ids are for an operator to spot-check a row, not a data export.
MAX_REPORTED_IDS: Final = 25

# Everything that can still be waiting to send. `processing` is included on
# purpose rather than filtered away: with the fence shut nothing should ever be
# claimed, so a `processing` review is a fact the operator needs to see, not a
# row to quietly skip.
OPEN_STATUSES: Final = ("queued", "processing")

PROVEN: Final = "proven"
REASON_CLAIMED_WHILE_FENCED: Final = "claimed_while_fence_closed"
REASON_NOT_OWNED: Final = "branch_not_in_registry"
REASON_DOMAIN: Final = "domain_identity_unproven"
REASON_CATEGORY: Final = "category_not_allowed"
REASON_CATEGORY_CONFIG: Final = "category_configuration_unavailable"
REASON_DEADLINE: Final = "deadline_expired"
REASON_TEMPLATE_MISSING: Final = "template_row_missing"
REASON_TEMPLATE_DUPLICATE: Final = "template_row_duplicated"
REASON_TEMPLATE_CONTRACT: Final = "template_contract_mismatch"
REASON_SENDER_MISSING: Final = "sender_missing_or_inactive"


@dataclass
class ReviewPreflightReport:
    """Counts, stable reason codes and technical ids — nothing else."""

    candidate_count: int = 0
    checked_count: int = 0
    truncated: bool = False
    reasons: Counter = field(default_factory=Counter)
    blocked_job_ids: list[int] = field(default_factory=list)
    blocked_record_ids: list[int] = field(default_factory=list)
    company_ids: set[int] = field(default_factory=set)

    @property
    def green_count(self) -> int:
        return self.reasons.get(PROVEN, 0)

    @property
    def blocked_count(self) -> int:
        return self.checked_count - self.green_count

    @property
    def ready(self) -> bool:
        """Green, and only for the narrow case that actually proves something."""
        if self.truncated:
            return False
        if self.candidate_count == 0:
            # A fence opened on the strength of "no problems found" is opened
            # blind: there was no queue to find problems in.
            return False
        if self.checked_count != self.candidate_count:
            return False
        return self.green_count == self.candidate_count

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "read-only",
            "candidate_count": self.candidate_count,
            "checked_count": self.checked_count,
            "green_count": self.green_count,
            "blocked_count": self.blocked_count,
            "truncated": self.truncated,
            "reasons": dict(sorted(self.reasons.items())),
            "blocked_job_ids": sorted(self.blocked_job_ids)[:MAX_REPORTED_IDS],
            "blocked_record_ids": sorted(self.blocked_record_ids)[:MAX_REPORTED_IDS],
            "company_ids": sorted(self.company_ids)[:MAX_REPORTED_IDS],
            "ready": self.ready,
        }


async def select_open_review_jobs(session: AsyncSession, *, limit: int) -> tuple[list[MessageJob], bool]:
    """Open EasyWeek review jobs, and whether the queue was longer than *limit*.

    One extra row is fetched purely to detect truncation: a bounded look at a
    longer queue must be reported as bounded, not as a clean bill of health.
    """
    stmt = (
        select(MessageJob)
        .where(MessageJob.provider == PROVIDER_EASYWEEK)
        .where(MessageJob.job_type == REVIEW_3D)
        .where(MessageJob.status.in_(OPEN_STATUSES))
        .order_by(MessageJob.run_at.asc(), MessageJob.id.asc())
        .limit(limit + 1)
    )
    rows = list((await session.execute(stmt)).scalars().all())
    return rows[:limit], len(rows) > limit


async def _template_reason(session: AsyncSession, job: MessageJob, profile: Any) -> str | None:
    """Exactly one active row for this branch and code, matching the contract.

    Provider AND company are both in the predicate: `review_3d` is a universal
    code on the Altegio side, and an EasyWeek job must never resolve to another
    tenant's row just because the code matches.
    """
    language = (getattr(settings, "easyweek_default_language", "de") or "de").strip() or "de"
    rows = list(
        (
            await session.execute(
                select(MessageTemplate)
                .where(MessageTemplate.provider == PROVIDER_EASYWEEK)
                .where(MessageTemplate.company_id == job.company_id)
                .where(MessageTemplate.code == REVIEW_3D)
                .where(MessageTemplate.language == language)
                .where(MessageTemplate.is_active.is_(True))
            )
        )
        .scalars()
        .all()
    )
    if not rows:
        return REASON_TEMPLATE_MISSING
    if len(rows) > 1:
        # Two active rows means the send would pick one by chance.
        return REASON_TEMPLATE_DUPLICATE

    contract = branch_template_contract(profile, REVIEW_3D)
    if contract is None:
        return REASON_TEMPLATE_CONTRACT
    row = rows[0]
    if (row.meta_template_name or "").strip() != contract.meta_template_name:
        return REASON_TEMPLATE_CONTRACT
    if row.body != contract.raw_body:
        # The body carries the branch footer, so a mismatch here is a message
        # signed by the wrong salon.
        return REASON_TEMPLATE_CONTRACT
    return None


async def _sender_reason(session: AsyncSession, job: MessageJob) -> str | None:
    """An active EasyWeek sender owned by this branch, and no fallback."""
    sender = (
        (
            await session.execute(
                select(WhatsAppSender)
                .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
                .where(WhatsAppSender.company_id == job.company_id)
                .where(WhatsAppSender.is_active.is_(True))
            )
        )
        .scalars()
        .first()
    )
    return None if sender is not None else REASON_SENDER_MISSING


async def check_review_job(session: AsyncSession, job: MessageJob) -> str:
    """One job's verdict as a stable reason code. ``PROVEN`` means sendable."""
    # Imported here rather than at module scope: the outbox worker is a heavy
    # import, and this keeps the command usable as a plain script. Same narrow
    # borrowing the reminder preflight already does.
    from altegio_bot.workers.outbox_worker import (
        _easyweek_owned_branch,
        _easyweek_review_presend_error,
        easyweek_reminder_deadline_passed,
    )

    if job.status == "processing":
        # With the fence shut nothing should have been claimed at all.
        return REASON_CLAIMED_WHILE_FENCED

    location, profile, ownership_error = _easyweek_owned_branch(job.company_id)
    if ownership_error is not None or location is None or profile is None:
        return REASON_NOT_OWNED

    record = None
    if job.record_id is not None:
        record = (await session.execute(select(Record).where(Record.id == job.record_id))).scalars().one_or_none()
    client = None
    if job.client_id is not None:
        client = (await session.execute(select(Client).where(Client.id == job.client_id))).scalars().one_or_none()

    # THE runtime guard: identity, tenancy, booking uuid, planned start, deletion,
    # opt-out, services_count and the review link, all in one place.
    if _easyweek_review_presend_error(job, record, client) is not None:
        return REASON_DOMAIN
    assert record is not None  # proven by the guard above

    eligibility = evaluate_service_category(
        record_raw=record.raw,
        allowed_categories_raw=settings.easyweek_allowed_service_categories,
    )
    if not eligibility.allowed:
        # A broken allowlist is red rather than green: it is not a decision that
        # the review is fine, it is an inability to decide at all.
        return REASON_CATEGORY_CONFIG if eligibility.recoverable_configuration else REASON_CATEGORY

    if easyweek_reminder_deadline_passed(job, record):
        return REASON_DEADLINE

    template_reason = await _template_reason(session, job, profile)
    if template_reason is not None:
        return template_reason

    sender_reason = await _sender_reason(session, job)
    if sender_reason is not None:
        return sender_reason

    return PROVEN


async def run_review_preflight(
    session: AsyncSession,
    *,
    limit: int = DEFAULT_LIMIT,
) -> ReviewPreflightReport:
    """Check every open review job with the runtime rules. Writes nothing."""
    jobs, truncated = await select_open_review_jobs(session, limit=limit)
    report = ReviewPreflightReport(candidate_count=len(jobs), truncated=truncated)

    for job in jobs:
        try:
            reason = await check_review_job(session, job)
        except Exception:  # noqa: BLE001 — a failure to check is never a pass
            reason = "check_failed"

        report.checked_count += 1
        report.reasons[reason] += 1
        if job.company_id is not None:
            report.company_ids.add(job.company_id)
        if reason != PROVEN:
            report.blocked_job_ids.append(job.id)
            if job.record_id is not None:
                report.blocked_record_ids.append(job.record_id)

    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only preflight for EasyWeek review_3d jobs. Writes nothing, sends nothing."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum review jobs to check (default {DEFAULT_LIMIT}). A truncated queue is never green.",
    )
    args = parser.parse_args(argv)
    if args.limit < 1:
        parser.error("--limit must be at least 1")
    return args


async def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        async with SessionLocal() as session:
            report = await run_review_preflight(session, limit=args.limit)
    except Exception as exc:  # noqa: BLE001 — class name only, never the text
        # A database or configuration failure is red. It is not evidence that
        # the queue is fine; it is evidence that we could not look.
        print({"mode": "read-only", "ready": False, "error": type(exc).__name__})
        return 1

    print(report.as_safe_dict())
    return 0 if report.ready else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
