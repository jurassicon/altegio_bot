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
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_policy import REVIEW_3D
from altegio_bot.easyweek_review import (
    REVIEW_LINK_MISSING,
    REVIEW_LINKS_INVALID,
    REVIEW_LINKS_UNCONFIGURED,
    VISIT_COUNT_UNPROVEN,
    VISIT_COUNTER_DISABLED,
    VISIT_LIMIT_ELIGIBLE,
    VISIT_LIMIT_EXCEEDED,
    google_review_url_for_company,
    parse_google_review_links,
    validate_google_review_url,
    visit_limit_verdict,
)
from altegio_bot.easyweek_service_category import evaluate_service_category
from altegio_bot.message_planner import MAX_VISITS_FOR_REVIEW
from altegio_bot.meta_templates import build_lifecycle_template_params
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    Client,
    MessageJob,
    MessageTemplate,
    Record,
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
# claimed, so a `processing` review is a fact the operator needs to see, not a
# row to quietly skip.
OPEN_STATUSES: Final = ("queued", "processing")

PROVEN: Final = "proven"
REASON_CLAIMED_WHILE_FENCED: Final = "claimed_while_fence_closed"
REASON_NOT_OWNED: Final = "branch_not_in_registry"
REASON_DOMAIN: Final = "domain_identity_unproven"
# PR-10. The link is ours now, so its failure modes are ours to report: no
# entry for this branch, an entry that does not validate, and a link that
# changed after the job was planned (identity-bound, so never swapped in).
REASON_REVIEW_LINK_MISSING: Final = "review_link_missing"
REASON_REVIEW_LINK_INVALID: Final = "review_link_invalid"
REASON_REVIEW_LINK_CHANGED: Final = "review_link_changed"
# A valid map that does not cover every live branch.
REASON_REVIEW_LINKS_INCOMPLETE: Final = "review_links_incomplete"
# The location registry itself, which gates whether anything is claimed at all.
REASON_LOCATION_REGISTRY_UNCONFIGURED: Final = "location_registry_unconfigured"
REASON_LOCATION_REGISTRY_INVALID: Final = "location_registry_invalid"
REASON_CATEGORY: Final = "category_not_allowed"
# Plan §31.11. The three buckets an operator needs before opening the send
# fence: how much of the backlog is provably eligible, how much is over the
# limit and will be cancelled by the send guard, and how much cannot be proven
# either way. Imported rather than re-spelled so the preflight, the planner and
# the sender cannot drift apart.
REASON_VISIT_LIMIT_EXCEEDED: Final = VISIT_LIMIT_EXCEEDED
REASON_VISIT_COUNT_UNPROVEN: Final = VISIT_COUNT_UNPROVEN
REASON_VISIT_COUNTER_DISABLED: Final = VISIT_COUNTER_DISABLED
REASON_CATEGORY_CONFIG: Final = "category_configuration_unavailable"
REASON_DEADLINE: Final = "deadline_expired"
REASON_TEMPLATE_MISSING: Final = "template_row_missing"
REASON_TEMPLATE_DUPLICATE: Final = "template_row_duplicated"
REASON_TEMPLATE_CONTRACT: Final = "template_contract_mismatch"
REASON_SENDER_MISSING: Final = "sender_missing_or_inactive"
REASON_SENDER_PHONE_ID_EMPTY: Final = "sender_phone_number_id_empty"
REASON_PHONE_MISSING: Final = "phone_missing"
REASON_TEMPLATE_PARAMS: Final = "template_params_unproven"

# Rollout state, checked BEFORE the queue. A preflight is a statement about a
# specific configuration — "these jobs are safe to release" — and it is only
# meaningful in the state the rollout is actually supposed to be in.
REASON_NOTIFICATIONS_DISABLED: Final = "notifications_disabled"
REASON_PLANNING_DISABLED: Final = "review_planning_disabled"
REASON_SEND_FENCE_OPEN: Final = "review_send_fence_open"


@dataclass
class ReviewPreflightReport:
    """Counts, stable reason codes and technical ids — nothing else."""

    candidate_count: int = 0
    checked_count: int = 0
    truncated: bool = False
    # Set when the rollout state itself is wrong. The queue is then not read at
    # all: auditing a backlog while the fence is already open would describe a
    # world that no longer exists.
    config_error: str | None = None
    reasons: Counter = field(default_factory=Counter)
    # Plan §31.11, counts only. Kept apart from `reasons` because the visit
    # question has an answer for EVERY checked job, including the ones blocked
    # for an unrelated reason: an operator sizing the backlog needs to know how
    # much of it is provably eligible, not only which job failed first.
    visit_buckets: Counter = field(default_factory=Counter)
    blocked_job_ids: list[int] = field(default_factory=list)
    blocked_record_ids: list[int] = field(default_factory=list)
    company_ids: set[int] = field(default_factory=set)
    # Live branches absent from the review link map. Named, not merely counted:
    # otherwise the operator knows one is missing but has to diff the two
    # variables by hand to learn which.
    uncovered_company_ids: list[int] = field(default_factory=list)

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
            "config_error": self.config_error,
            "candidate_count": self.candidate_count,
            "checked_count": self.checked_count,
            "green_count": self.green_count,
            "blocked_count": self.blocked_count,
            "truncated": self.truncated,
            "reasons": dict(sorted(self.reasons.items())),
            "review_visit_buckets": dict(sorted(self.visit_buckets.items())),
            "blocked_job_ids": sorted(self.blocked_job_ids)[:MAX_REPORTED_IDS],
            "blocked_record_ids": sorted(self.blocked_record_ids)[:MAX_REPORTED_IDS],
            "company_ids": sorted(self.company_ids)[:MAX_REPORTED_IDS],
            "uncovered_company_ids": sorted(self.uncovered_company_ids)[:MAX_REPORTED_IDS],
            "ready": self.ready,
        }


def rollout_state_error() -> str | None:
    """The one configuration this preflight is a statement about, or a reason.

    A green report means "every queued review would be sent correctly if the
    fence opened". That sentence is only true in one state: notifications on,
    planning on, fence still shut. With notifications or planning off the queue
    is not being fed and the audit describes a frozen picture; with the fence
    already open there is nothing left to authorise — the sends are happening.
    """
    if not bool(getattr(settings, "easyweek_notifications_enabled", False)):
        return REASON_NOTIFICATIONS_DISABLED
    if not bool(getattr(settings, "easyweek_reviews_enabled", False)):
        return REASON_PLANNING_DISABLED
    if bool(getattr(settings, "easyweek_review_send_enabled", False)):
        return REASON_SEND_FENCE_OPEN

    # PR-10. Without a usable link map the planner creates nothing, so the queue
    # is empty for a reason that has nothing to do with the queue. Reported here
    # rather than per-row: otherwise the first rollout reads "candidate_count=0,
    # STOP" and the operator never learns the map is the cause.
    links = parse_google_review_links(settings.easyweek_google_review_links)
    if not links.configured:
        return REVIEW_LINKS_UNCONFIGURED
    if not links.valid:
        return REVIEW_LINKS_INVALID

    # An unusable registry is its own blindness, by the same argument. With it
    # broken `processing_is_configured()` is False, so the worker claims
    # nothing, no job is ever planned, and this report would say
    # "candidate_count=0" without naming the cause — the exact class of silence
    # revision 13 closed for the link map. The registry has no
    # `unavailable_reason` of its own (unlike the link map and the category
    # allowlist), so the two states are read off its own `configured`/`valid`
    # rather than reclassified somewhere else.
    registry = configured_easyweek_locations()
    if not registry.configured:
        return REASON_LOCATION_REGISTRY_UNCONFIGURED
    if not registry.valid:
        return REASON_LOCATION_REGISTRY_INVALID
    if not registry.locations:  # pragma: no cover - a valid parser result is non-empty
        return REASON_LOCATION_REGISTRY_UNCONFIGURED

    # A valid map is not necessarily a COMPLETE one. The location registry is
    # the definition of "which branches are live", so a branch missing from the
    # map is a configuration gap — and the most invisible kind: it plans no
    # jobs, so it never appears in this report as a row, while its events sit
    # in configuration deferral. Exactly the state this preflight exists to
    # surface, and exactly the one it would otherwise miss.
    if uncovered_review_link_companies():
        return REASON_REVIEW_LINKS_INCOMPLETE
    return None


def uncovered_review_link_companies() -> list[int]:
    """Live branches with no entry in the review link map, as company ids.

    Kept separate from :func:`rollout_state_error` deliberately. That function
    answers one question with one string and has callers and tests relying on
    exactly that shape; widening its return type to smuggle a payload would
    churn every one of them for a value only a single branch needs. A named
    helper says what it computes, is callable on its own, and keeps the reason
    code and its detail from drifting apart.

    ``company_id`` is not a link and is already reported by this preflight as
    ``company_ids`` — the links themselves are never returned or printed.
    """
    links = parse_google_review_links(settings.easyweek_google_review_links)
    if not links.valid:
        return []
    registry = configured_easyweek_locations()
    if not registry.ready:
        return []
    return sorted(set(registry.locations) - set(links.links))


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
    """The sender runtime will actually route to, not merely one that exists.

    EasyWeek sends always resolve `sender_code="default"` (the outbox worker
    never runs service-based routing for EasyWeek), so an audit that accepted
    any active row would pass a branch whose only sender is, say, `vip` — and
    the send would then fail with no sender at all.
    """
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


def _send_prerequisites_reason(client: Client, review_url: str) -> str | None:
    """The values the send path itself requires, checked with its own builders.

    `Client.phone_e164` and `display_name` are both nullable, and neither is
    covered by the domain guard. Without this, a review could be proven here and
    then die at send time on "No phone_e164" or on an empty first parameter —
    which is exactly the kind of surprise a preflight exists to remove.

    The parameters are built and validated by the SAME functions the outbox
    calls, so this cannot become a third copy of the contract that drifts.
    """
    if not (getattr(client, "phone_e164", None) or "").strip():
        return REASON_PHONE_MISSING

    params = build_lifecycle_template_params(
        REVIEW_3D,
        {"client_name": getattr(client, "display_name", None) or "", "review_url": review_url},
    )
    if validate_lifecycle_template_params(REVIEW_3D, params) is not None:
        return REASON_TEMPLATE_PARAMS
    return None


def review_visit_bucket(client: Client | None) -> str:
    """The visit verdict for one job's client, exactly as the sender asks it."""
    if not settings.easyweek_visit_counter_enabled:
        return VISIT_COUNTER_DISABLED
    if client is None:
        return VISIT_COUNT_UNPROVEN
    return visit_limit_verdict(
        max_visits=MAX_VISITS_FOR_REVIEW,
        visits_total=client.easyweek_visits_total,
        updated_at=client.easyweek_visits_total_updated_at,
    )


async def check_review_job(session: AsyncSession, job: MessageJob) -> str:
    """One job's verdict as a stable reason code. ``PROVEN`` means sendable."""
    # Imported here rather than at module scope: the outbox worker is a heavy
    # import, and this keeps the command usable as a plain script. Same narrow
    # borrowing the reminder preflight already does.
    from altegio_bot.workers.outbox_worker import (
        _easyweek_owned_branch,
        _easyweek_review_presend_error,
        easyweek_reminder_deadline_passed,
        easyweek_review_url_for_send,
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

    # PR-10: report WHY the link failed rather than folding every cause into
    # the generic domain reason. An operator fixing one variable needs to know
    # whether the entry is absent, malformed, or simply different from the one
    # this job was planned with.
    configured_url, link_error = google_review_url_for_company(
        job.company_id,
        settings.easyweek_google_review_links,
    )
    if link_error == REVIEW_LINK_MISSING:
        return REASON_REVIEW_LINK_MISSING
    if link_error is not None:
        # Defence in depth only: `rollout_state_error()` already refuses these
        # states before the queue is read, so an operator never arrives here.
        # Kept so a future caller of `check_review_job` cannot lose the answer,
        # and reported verbatim because "never set up" and "set up wrongly" are
        # different operator actions.
        return link_error
    planned_url = validate_google_review_url((job.payload or {}).get("review_url"))
    if planned_url is None:
        return REASON_REVIEW_LINK_INVALID
    if planned_url != configured_url:
        return REASON_REVIEW_LINK_CHANGED

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

    assert client is not None  # proven by the domain guard above

    # The visit limit, asked exactly as the sender will ask it (§31.11).
    # `review send on, visit counter off` is a red configuration, not a
    # supported mode: the send guard would hold every one of these jobs.
    visit_verdict = review_visit_bucket(client)
    if visit_verdict != VISIT_LIMIT_ELIGIBLE:
        return visit_verdict

    review_url = easyweek_review_url_for_send(job, record)
    if review_url is None:  # pragma: no cover - the domain guard proves it
        return REASON_DOMAIN
    prerequisites_reason = _send_prerequisites_reason(client, review_url)
    if prerequisites_reason is not None:
        return prerequisites_reason

    return PROVEN


async def run_review_preflight(
    session: AsyncSession,
    *,
    limit: int = DEFAULT_LIMIT,
) -> ReviewPreflightReport:
    """Check every open review job with the runtime rules. Writes nothing."""
    config_error = rollout_state_error()
    if config_error is not None:
        # The queue is deliberately not read: this is not an audit that failed,
        # it is an audit that does not apply. The one config error that has a
        # concrete list behind it carries that list, so the operator does not
        # have to diff two environment variables by hand.
        uncovered = uncovered_review_link_companies() if config_error == REASON_REVIEW_LINKS_INCOMPLETE else []
        return ReviewPreflightReport(config_error=config_error, uncovered_company_ids=uncovered)

    jobs, truncated = await select_open_review_jobs(session, limit=limit)
    report = ReviewPreflightReport(candidate_count=len(jobs), truncated=truncated)

    for job in jobs:
        try:
            reason = await check_review_job(session, job)
        except Exception:  # noqa: BLE001 — a failure to check is never a pass
            reason = "check_failed"

        report.checked_count += 1
        report.reasons[reason] += 1
        try:
            client = await session.get(Client, job.client_id) if job.client_id is not None else None
            report.visit_buckets[review_visit_bucket(client)] += 1
        except Exception:  # noqa: BLE001 — an audit line must not fail the audit
            report.visit_buckets[VISIT_COUNT_UNPROVEN] += 1
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
