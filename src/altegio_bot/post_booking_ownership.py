"""Who owns a migrated booking's marketing follow-ups (plan §31.7).

The failure this closes
-----------------------
`review_3d`, `repeat_10d` and `comeback_3d` are planned from an Altegio booking
and sent with Altegio's own templates, sender and booking link. After a booking
has been migrated to EasyWeek, every one of those is wrong: the appointment is
worked from another system, and the link in the message points at a page nobody
uses any more.

The reminder handover (§30) does not help here, and must not be stretched to. It
proves that timed EasyWeek reminders were CREATED. These three prove the
opposite kind of fact — that the Altegio obligation was given up and NOTHING was
created in its place, because a migrated future booking is not evidence of a
completed visit. Only a real EasyWeek outcome may create the EasyWeek
equivalents: `booking-succeeded` for `review_3d` and `repeat_10d`, a proven
cancellation for `comeback_3d`.

Withdrawing the open jobs once is not enough either. `add_job` resurrects a
`canceled` job on conflict, so a late Altegio `create` re-opens exactly what the
handover withdrew, and a late `delete` plans a fresh `comeback_3d`. A booking
that never had such a job when the handover ran can acquire its FIRST one
afterwards. That is why the marker is written for every eligible row of the wave
and why both runtime paths ask about it: the planner before creating a job, the
outbox immediately before anything external.

Fail-closed, and what that means on each side
---------------------------------------------
Three answers, never two:

* ``EASYWEEK`` — a marker proves the Altegio side gave these up.
* ``ALTEGIO`` — no marker for this exact source identity, so nothing has moved
  and the ordinary path continues untouched.
* ``UNKNOWN`` — the question could not be answered: contradictory rows, half a
  marker, an unusable identity.

``UNKNOWN`` is not ``ALTEGIO``. Planning refuses; sending refuses. Both are
recoverable states a person can look at, and both are better than a real
customer receiving a review request for an appointment that lives in another
system.

Scope is exact and narrow. Provider ``altegio``, one company, one source record,
and only the three job types below. Reminders keep their own §30 fence,
lifecycle jobs, campaigns, newsletters, promos, other companies, unmigrated
records and every EasyWeek job are untouched.
"""

from __future__ import annotations

import logging
from enum import Enum
from typing import Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import PROVIDER_ALTEGIO, EasyWeekMigrationLedger

logger = logging.getLogger(__name__)

# The only job types this fence applies to. Record-bound, planned from one
# booking, and each with its own EasyWeek counterpart that only a proven outcome
# event may create. `reminder_24h` / `reminder_2h` are NOT here: they are §30's,
# they have their own marker, and one fence must never answer for the other.
POST_BOOKING_JOB_TYPES: Final[frozenset[str]] = frozenset({"review_3d", "repeat_10d", "comeback_3d"})

# Stable, PII-free reasons. They reach `message_jobs.last_error`, which an
# operator reads and a report may quote.
REASON_HANDED_OVER: Final = "Canceled: post-booking marketing ownership handed over to EasyWeek"
REASON_UNKNOWN: Final = "post-booking marketing ownership could not be proven"


class PostBookingOwnershipUnproven(RuntimeError):
    """The planner could not answer the ownership question at all.

    Raised rather than returned, and only on the PLANNING side. Suppressing on
    an unanswerable question is right for a send — nothing is lost by not
    sending — but the planner runs once per delivery and its caller acks the
    event afterwards, so quietly skipping would consume the obligation: the
    booking would simply never get its follow-up, with a log line as the only
    trace.

    The inbox worker records the event as `failed` with this stable, PII-free
    reason, which leaves it visible and re-drivable. Fail-closed either way; the
    difference is whether a person can see it.
    """

    def __init__(self) -> None:
        super().__init__(REASON_UNKNOWN)


class PostBookingOwner(str, Enum):
    """Who is responsible for this booking's marketing follow-ups."""

    ALTEGIO = "altegio"
    EASYWEEK = "easyweek"
    UNKNOWN = "unknown"


async def post_booking_owner(
    session: AsyncSession,
    *,
    company_id: object,
    altegio_record_id: object,
) -> PostBookingOwner:
    """Who owns the marketing follow-ups for one exact Altegio source booking.

    Runs inside the caller's transaction on purpose. The planner calls it after
    it already holds the ``Record`` state for this delivery, so a webhook that
    was waiting behind the handover's own row locks sees the marker the moment
    it is unblocked — rather than an answer read before the commit.

    Exact identity only: ``(altegio, company_id, altegio_record_id)`` is the
    ledger's natural key, so at most one row can match. More than one would mean
    the key is not what it claims, which is ``UNKNOWN``, not permission.
    """
    if type(company_id) is not int or type(altegio_record_id) is not int:
        # An identity we cannot state is an identity we cannot check. Guessing
        # here would guess about somebody's messages.
        return PostBookingOwner.UNKNOWN

    stmt = (
        select(
            EasyWeekMigrationLedger.post_booking_jobs_handed_over_at,
            EasyWeekMigrationLedger.post_booking_handover_plan_digest,
        )
        .where(EasyWeekMigrationLedger.source_provider == PROVIDER_ALTEGIO)
        .where(EasyWeekMigrationLedger.source_company_id == company_id)
        .where(EasyWeekMigrationLedger.source_record_id == altegio_record_id)
    )
    try:
        rows = list((await session.execute(stmt)).all())
    except Exception:
        # The question could not be put to the database at all. That is not an
        # answer, and it is certainly not permission.
        logger.error(
            "post-booking ownership lookup failed: company_id=%s source_record_id=%s",
            company_id,
            altegio_record_id,
        )
        return PostBookingOwner.UNKNOWN

    if not rows:
        # No ledger row at all, or one for a different company. Never migrated
        # here, so the ordinary Altegio path owns it exactly as before.
        return PostBookingOwner.ALTEGIO
    if len(rows) > 1:  # pragma: no cover - the unique constraint forbids it
        logger.error(
            "post-booking ownership ambiguous: company_id=%s source_record_id=%s rows=%d",
            company_id,
            altegio_record_id,
            len(rows),
        )
        return PostBookingOwner.UNKNOWN

    handed_over_at, digest = rows[0]
    if handed_over_at is None and digest is None:
        # A migrated booking whose marketing jobs were never handed over. The
        # ordinary state for every wave that has not run PR-12.1 yet, and it
        # must NOT suppress anything — which is also what makes deploying the
        # fences before the first apply safe.
        return PostBookingOwner.ALTEGIO
    if handed_over_at is None or not digest:
        # Half a marker. The database CHECK makes this unreachable through any
        # supported path, so seeing it means something wrote the row directly.
        logger.error(
            "post-booking ownership marker is incomplete: company_id=%s source_record_id=%s",
            company_id,
            altegio_record_id,
        )
        return PostBookingOwner.UNKNOWN
    return PostBookingOwner.EASYWEEK


async def altegio_post_booking_jobs_are_suppressed(
    session: AsyncSession,
    *,
    company_id: object,
    altegio_record_id: object,
) -> tuple[bool, PostBookingOwner]:
    """Should the Altegio path refrain from creating one of these jobs here?

    ``True`` for both ``EASYWEEK`` and ``UNKNOWN``: an unanswerable question is
    not a licence. Returned with the owner so the caller can log and act on the
    difference — one is the expected outcome after a handover, the other is
    something a person needs to look at.
    """
    owner = await post_booking_owner(session, company_id=company_id, altegio_record_id=altegio_record_id)
    return owner is not PostBookingOwner.ALTEGIO, owner
