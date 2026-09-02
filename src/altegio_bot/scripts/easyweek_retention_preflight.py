"""PR-12: prove the queued retention messages before opening the send fence.

``EASYWEEK_RETENTION_SEND_ENABLED`` starts closed, so real ``repeat_10d`` and
``comeback_3d`` jobs accumulate as ``queued`` without a single message going
out. This command is what earns the right to open it: it takes the REAL jobs
production is holding and asks whether each one would be provably sendable right
now.

The verdict comes from the runtime's own rules, not from a second implementation
that reads similarly. The identity, counter, return, opt-out, category, link and
deadline checks are the very functions the outbox worker calls — imported
narrowly, exactly as the review preflight already does. A preflight that judged
any of this differently would bless a queue the worker then refuses or, far
worse, the other way round.

Two things it checks that the send path reaches only later, and that are worth
knowing BEFORE the fence opens rather than one job at a time afterwards: that
exactly one active EasyWeek template row exists for this branch and code and
matches the source-controlled contract byte for byte, and that the branch has
its own active sender.

Strictly read-only. It selects, and it never writes: no job, no event, no
record, no outbox row, no Meta call, no Chatwoot message, no EasyWeek API call
of any kind. The session is never committed.

Green is narrow on purpose: at least one candidate, nothing truncated, and every
candidate proven. Anything else exits non-zero, including an unexpected error —
"we could not check" must never read as "everything is fine".

Output is counts, stable reason codes and technical ids. No booking uuid, no
hash, no visit count, no dedupe key, no name, phone, e-mail, service text or
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
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_policy import COMEBACK_3D, EASYWEEK_RETENTION_JOB_TYPES, REPEAT_10D
from altegio_bot.easyweek_service_category import evaluate_service_category
from altegio_bot.meta_templates import build_lifecycle_template_params
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    Client,
    MessageJob,
    MessageTemplate,
    OutboxMessage,
    Record,
    RecordService,
    WhatsAppSender,
)
from altegio_bot.settings import settings
from altegio_bot.template_validation import validate_lifecycle_template_params

DEFAULT_LIMIT: Final = 200
# Ids are for an operator to spot-check a row, not a data export.
MAX_REPORTED_IDS: Final = 25

# EasyWeek never runs service-based sender routing; the outbox resolves this
# code and only this one.
EASYWEEK_SENDER_CODE: Final = "default"

# Everything that can still be waiting to send. `processing` is included on
# purpose rather than filtered away: with the fence shut nothing should ever be
# claimed, so a `processing` retention job is a fact the operator needs to see,
# not a row to quietly skip.
OPEN_STATUSES: Final = ("queued", "processing")

PROVEN: Final = "proven"
REASON_CLAIMED_WHILE_FENCED: Final = "claimed_while_fence_closed"
REASON_NOT_OWNED: Final = "branch_not_in_registry"
REASON_JOB_TYPE_NOT_ALLOWED: Final = "job_type_not_allowed"
REASON_ALREADY_SENT: Final = "already_sent"
REASON_CATEGORY: Final = "category_not_allowed"
REASON_CATEGORY_CONFIG: Final = "category_configuration_unavailable"
REASON_DEADLINE: Final = "deadline_expired"
REASON_TEMPLATE_MISSING: Final = "template_row_missing"
REASON_TEMPLATE_DUPLICATE: Final = "template_row_duplicated"
REASON_TEMPLATE_CONTRACT: Final = "template_contract_mismatch"
REASON_SENDER_MISSING: Final = "sender_missing_or_inactive"
REASON_SENDER_PHONE_ID_EMPTY: Final = "sender_phone_number_id_empty"
REASON_PHONE_MISSING: Final = "phone_missing"
REASON_TEMPLATE_PARAMS: Final = "template_params_unproven"
REASON_BOOKING_PAGE: Final = "booking_page_unproven"

# The location registry itself, which gates whether anything is claimed at all.
REASON_LOCATION_REGISTRY_UNCONFIGURED: Final = "location_registry_unconfigured"
REASON_LOCATION_REGISTRY_INVALID: Final = "location_registry_invalid"

# Rollout state, checked BEFORE the queue. A preflight is a statement about a
# specific configuration — "these jobs are safe to release" — and it is only
# meaningful in the state the rollout is actually supposed to be in.
REASON_NOTIFICATIONS_DISABLED: Final = "notifications_disabled"
REASON_PLANNING_DISABLED: Final = "retention_planning_disabled"
REASON_SEND_FENCE_OPEN: Final = "retention_send_fence_open"
# PR-11's counter is the input the whole feature reads. With it off no repeat is
# ever planned and no comeback can prove a baseline, so an empty queue would be
# reported without naming its cause.
REASON_VISIT_COUNTER_DISABLED: Final = "visit_counter_disabled"


@dataclass
class RetentionPreflightReport:
    """Counts, stable reason codes and technical ids — nothing else."""

    candidate_count: int = 0
    checked_count: int = 0
    truncated: bool = False
    # Set when the rollout state itself is wrong. The queue is then not read at
    # all: auditing a backlog while the fence is already open would describe a
    # world that no longer exists.
    config_error: str | None = None
    reasons: Counter = field(default_factory=Counter)
    job_types: Counter = field(default_factory=Counter)
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
        if self.config_error is not None:
            return False
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
            "read_only": True,
            # Stated explicitly rather than implied by the absence of writes: an
            # operator reading this in a ticket must be able to see that no
            # message and no migration was authorised by running it.
            "send_authorized": False,
            "migration_authorized": False,
            "config_error": self.config_error,
            "candidate_count": self.candidate_count,
            "checked_count": self.checked_count,
            "green_count": self.green_count,
            "blocked_count": self.blocked_count,
            "truncated": self.truncated,
            "job_types": dict(sorted(self.job_types.items())),
            "reasons": dict(sorted(self.reasons.items())),
            "blocked_job_ids": sorted(self.blocked_job_ids)[:MAX_REPORTED_IDS],
            "blocked_record_ids": sorted(self.blocked_record_ids)[:MAX_REPORTED_IDS],
            "company_ids": sorted(self.company_ids)[:MAX_REPORTED_IDS],
            "ready": self.ready,
        }


def rollout_state_error() -> str | None:
    """The one configuration this preflight is a statement about, or a reason.

    A green report means "every queued retention message would be sent correctly
    if the fence opened". That sentence is only true in one state: notifications
    on, planning on, the counter on, and the fence still shut. With notifications
    or planning off the queue is not being fed and the audit describes a frozen
    picture; with the fence already open there is nothing left to authorise.
    """
    if not bool(getattr(settings, "easyweek_notifications_enabled", False)):
        return REASON_NOTIFICATIONS_DISABLED
    if not bool(getattr(settings, "easyweek_retention_enabled", False)):
        return REASON_PLANNING_DISABLED
    if bool(getattr(settings, "easyweek_retention_send_enabled", False)):
        return REASON_SEND_FENCE_OPEN
    if not bool(getattr(settings, "easyweek_visit_counter_enabled", False)):
        # Reported here rather than per-row. Without the counter a repeat can
        # never prove its baseline, so the queue would simply be empty and the
        # first rollout would read "candidate_count=0, STOP" without ever
        # learning why.
        return REASON_VISIT_COUNTER_DISABLED

    registry = configured_easyweek_locations()
    if not registry.configured:
        return REASON_LOCATION_REGISTRY_UNCONFIGURED
    if not registry.valid:
        return REASON_LOCATION_REGISTRY_INVALID
    if not registry.locations:  # pragma: no cover - a valid parser result is non-empty
        return REASON_LOCATION_REGISTRY_UNCONFIGURED
    return None


async def select_open_retention_jobs(session: AsyncSession, *, limit: int) -> tuple[list[MessageJob], bool]:
    """Open EasyWeek retention jobs, and whether the queue was longer than *limit*.

    One extra row is fetched purely to detect truncation: a bounded look at a
    longer queue must be reported as bounded, not as a clean bill of health.

    Provider AND job type are both in the predicate. ``repeat_10d`` and
    ``comeback_3d`` exist on the Altegio side too, and auditing those here would
    report on a queue this fence does not govern.
    """
    stmt = (
        select(MessageJob)
        .where(MessageJob.provider == PROVIDER_EASYWEEK)
        .where(MessageJob.job_type.in_(sorted(EASYWEEK_RETENTION_JOB_TYPES)))
        .where(MessageJob.status.in_(OPEN_STATUSES))
        .order_by(MessageJob.run_at.asc(), MessageJob.id.asc())
        .limit(limit + 1)
    )
    rows = list((await session.execute(stmt)).scalars().all())
    return rows[:limit], len(rows) > limit


async def _template_reason(session: AsyncSession, job: MessageJob, profile: Any) -> str | None:
    """Exactly one active row for this branch and code, matching the contract.

    Provider AND company are both in the predicate: ``repeat_10d`` and
    ``comeback_3d`` are UNIVERSAL codes on the Altegio side — they are in
    ``UNIVERSAL_JOB_TYPES`` and reachable there by a cross-company fallback — so
    an EasyWeek job must never resolve to another tenant's row just because the
    code matches.
    """
    language = (getattr(settings, "easyweek_default_language", "de") or "de").strip() or "de"
    rows = list(
        (
            await session.execute(
                select(MessageTemplate)
                .where(MessageTemplate.provider == PROVIDER_EASYWEEK)
                .where(MessageTemplate.company_id == job.company_id)
                .where(MessageTemplate.code == job.job_type)
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

    contract = branch_template_contract(profile, job.job_type)
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
    """The sender runtime will actually route to, not merely one that exists."""
    sender = (
        (
            await session.execute(
                select(WhatsAppSender)
                .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
                .where(WhatsAppSender.company_id == job.company_id)
                .where(WhatsAppSender.sender_code == EASYWEEK_SENDER_CODE)
                .where(WhatsAppSender.is_active.is_(True))
            )
        )
        .scalars()
        .first()
    )
    if sender is None:
        return REASON_SENDER_MISSING
    if not (sender.phone_number_id or "").strip():
        # A row that exists but names no WhatsApp number is not a sender.
        return REASON_SENDER_PHONE_ID_EMPTY
    return None


async def _send_prerequisites_reason(
    session: AsyncSession,
    job: MessageJob,
    record: Record,
    client: Client,
    booking_link: str,
) -> str | None:
    """The values the send path itself requires, checked with its own builders.

    ``Client.phone_e164`` and ``display_name`` are both nullable, and neither is
    covered by the domain guard. The parameters are built and validated by the
    SAME functions the outbox calls, so this cannot become a third copy of the
    contract that drifts.
    """
    if not (getattr(client, "phone_e164", None) or "").strip():
        return REASON_PHONE_MISSING

    primary_service = ""
    if job.job_type == REPEAT_10D:
        services = list(
            (
                await session.execute(
                    select(RecordService)
                    .where(RecordService.record_id == record.id)
                    .order_by(RecordService.service_id.asc())
                )
            )
            .scalars()
            .all()
        )
        primary_service = (services[0].title or "") if services else ""

    params = build_lifecycle_template_params(
        job.job_type,
        {
            "client_name": getattr(client, "display_name", None) or "",
            "primary_service": primary_service,
            "booking_link": booking_link,
        },
    )
    if validate_lifecycle_template_params(job.job_type, params) is not None:
        return REASON_TEMPLATE_PARAMS
    return None


async def check_retention_job(session: AsyncSession, job: MessageJob) -> str:
    """One job's verdict as a stable reason code. ``PROVEN`` means sendable."""
    # Imported here rather than at module scope: the outbox worker is a heavy
    # import, and this keeps the command usable as a plain script. Same narrow
    # borrowing the review and reminder preflights already do.
    from altegio_bot.workers.outbox_worker import (
        SUCCESS_OUTBOX_STATUSES,
        _easyweek_owned_branch,
        _easyweek_retention_presend_error,
        easyweek_effective_booking_link,
        easyweek_reminder_deadline_passed,
    )

    if job.job_type not in EASYWEEK_RETENTION_JOB_TYPES:  # pragma: no cover - the query proves it
        return REASON_JOB_TYPE_NOT_ALLOWED
    if job.status == "processing":
        # With the fence shut nothing should have been claimed at all.
        return REASON_CLAIMED_WHILE_FENCED

    location, profile, ownership_error = _easyweek_owned_branch(job.company_id)
    if ownership_error is not None or location is None or profile is None:
        return REASON_NOT_OWNED

    # A job whose own message already went out is not a candidate to release.
    already_sent = (
        await session.execute(
            select(OutboxMessage.id)
            .where(OutboxMessage.job_id == job.id)
            .where(OutboxMessage.status.in_(SUCCESS_OUTBOX_STATUSES))
            .limit(1)
        )
    ).scalar_one_or_none()
    if already_sent is not None:
        return REASON_ALREADY_SENT

    record = None
    if job.record_id is not None:
        record = (await session.execute(select(Record).where(Record.id == job.record_id))).scalars().one_or_none()
    client = None
    if job.client_id is not None:
        client = (await session.execute(select(Client).where(Client.id == job.client_id))).scalars().one_or_none()

    # THE runtime guard: proof version, identity, tenancy, booking uuid, source
    # state, opt-out, the counter comparison, the future-booking check, the
    # comeback window and the booking page — all in one place, and all the very
    # function the worker calls.
    retention_error = await _easyweek_retention_presend_error(session, job, record, client)
    if retention_error is not None:
        return retention_error
    assert record is not None and client is not None  # proven by the guard above

    eligibility = evaluate_service_category(
        record_raw=record.raw,
        allowed_categories_raw=settings.easyweek_allowed_service_categories,
    )
    if not eligibility.allowed:
        # A broken allowlist is red rather than green: it is not a decision that
        # the message is fine, it is an inability to decide at all.
        return REASON_CATEGORY_CONFIG if eligibility.recoverable_configuration else REASON_CATEGORY

    if easyweek_reminder_deadline_passed(job, record):
        return REASON_DEADLINE

    template_reason = await _template_reason(session, job, profile)
    if template_reason is not None:
        return template_reason

    sender_reason = await _sender_reason(session, job)
    if sender_reason is not None:
        return sender_reason

    booking_link = easyweek_effective_booking_link(record, job.job_type, company_id=job.company_id)
    if not booking_link:  # pragma: no cover - the runtime guard proves it
        return REASON_BOOKING_PAGE

    return await _send_prerequisites_reason(session, job, record, client, booking_link) or PROVEN


async def run_retention_preflight(
    session: AsyncSession,
    *,
    limit: int = DEFAULT_LIMIT,
) -> RetentionPreflightReport:
    """Check every open retention job with the runtime rules. Writes nothing."""
    config_error = rollout_state_error()
    if config_error is not None:
        # The queue is deliberately not read: this is not an audit that failed,
        # it is an audit that does not apply.
        return RetentionPreflightReport(config_error=config_error)

    jobs, truncated = await select_open_retention_jobs(session, limit=limit)
    report = RetentionPreflightReport(candidate_count=len(jobs), truncated=truncated)

    for job in jobs:
        try:
            reason = await check_retention_job(session, job)
        except Exception:  # noqa: BLE001 — a failure to check is never a pass
            reason = "check_failed"

        report.checked_count += 1
        report.reasons[reason] += 1
        report.job_types[job.job_type] += 1
        if job.company_id is not None:
            report.company_ids.add(job.company_id)
        if reason != PROVEN:
            report.blocked_job_ids.append(job.id)
            if job.record_id is not None:
                report.blocked_record_ids.append(job.record_id)

    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            f"Read-only preflight for EasyWeek {REPEAT_10D} / {COMEBACK_3D} jobs. Writes nothing, sends nothing."
        )
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum retention jobs to check (default {DEFAULT_LIMIT}). A truncated queue is never green.",
    )
    args = parser.parse_args(argv)
    if args.limit < 1:
        parser.error("--limit must be at least 1")
    return args


async def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        async with SessionLocal() as session:
            report = await run_retention_preflight(session, limit=args.limit)
    except Exception as exc:  # noqa: BLE001 — class name only, never the text
        # A database or configuration failure is red. It is not evidence that
        # the queue is fine; it is evidence that we could not look.
        print(
            {
                "mode": "read-only",
                "read_only": True,
                "send_authorized": False,
                "ready": False,
                "error": type(exc).__name__,
            }
        )
        return 1

    print(report.as_safe_dict())
    return 0 if report.ready else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
