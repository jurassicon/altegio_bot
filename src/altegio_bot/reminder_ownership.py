"""Who owns an Altegio booking's future reminders (plan §30.11).

The failure this closes
-----------------------
The post-migration handover creates the EasyWeek reminders, withdraws the old
Altegio ones and commits. Altegio inbox and capture are deliberately NOT stopped
while it runs — stopping them would be a much bigger outage than the handover
needs. So a delivery that was already in flight, or one that arrives a minute
later, still reaches the ordinary Altegio planner.

That planner creates reminders through ``add_job``, whose ``ON CONFLICT`` clause
sets a ``canceled`` or ``failed`` job back to ``queued``. Which means a late
``create`` re-opened the very reminder the handover had just withdrawn, and a
late ``reschedule`` added a fresh one under a new dedupe key. The appointment
then had open reminders on both sides, and the Altegio one pointed at a booking
nobody works from any more.

Nothing in the pre-existing state could have prevented it. ``status='created'``
on the ledger row is about the BOOKING and predates the handover; a cancelled
job is exactly what ``add_job`` resurrects; a missing EasyWeek reminder is
legitimate for a cancelled appointment; and the apply report is a file the
runtime never reads.

So the handover writes a durable marker, and this module is how the two runtime
paths ask about it: the planner before creating a reminder, and the outbox
immediately before Meta.

Fail-closed, and what that means on each side
---------------------------------------------
Four answers, never two:

* ``EASYWEEK`` — a marker proves EasyWeek owns these reminders.
* ``SUPPRESSED`` — the bot intentionally owns no reminder obligation for the
  proven unsupported category.
* ``ALTEGIO`` — the ledger has no marker for this exact source identity, so
  nothing has moved and the ordinary path continues untouched.
* ``UNKNOWN`` — the question could not be answered. Contradictory rows, a
  half-written marker, an unusable identity.

``UNKNOWN`` is not ``ALTEGIO``. On the planning side it refuses to create the
reminder; on the send side it refuses to send. Both are recoverable states a
person can look at — and both are better than the alternative, which is a real
customer receiving a message about an appointment that moved to another system.

Scope is exact and narrow. Provider ``altegio``, one company, one source record,
and only ``reminder_24h`` / ``reminder_2h``. Lifecycle, review, retention and
campaign jobs are untouched, other companies are untouched, other records are
untouched, and every EasyWeek job is untouched.
"""

from __future__ import annotations

import logging
import uuid
from enum import Enum
from typing import Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.easyweek_service_category import REMINDER_SUPPRESSION_REASON_CODE
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    EasyWeekMigrationLedger,
)

logger = logging.getLogger(__name__)

# The only job types this fence applies to. A handover moves reminders and
# nothing else, so nothing else may be suppressed by it.
HANDOVER_JOB_TYPES: Final[frozenset[str]] = frozenset({"reminder_24h", "reminder_2h"})

# Stable, PII-free reasons. They reach `message_jobs.last_error`, which an
# operator reads and a report may quote.
REASON_HANDED_OVER: Final = "Canceled: reminder ownership handed over to EasyWeek"
REASON_SUPPRESSED: Final = "Canceled: reminders suppressed for unsupported EasyWeek service category"
REASON_UNKNOWN: Final = "reminder ownership could not be proven"


class ReminderOwner(str, Enum):
    """Who is responsible for this booking's future reminders."""

    ALTEGIO = "altegio"
    EASYWEEK = "easyweek"
    SUPPRESSED = "suppressed"
    UNKNOWN = "unknown"


def _valid_digest(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def _valid_target_uuid(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return str(uuid.UUID(value)) == value
    except (TypeError, ValueError, AttributeError):
        return False


async def reminder_owner(
    session: AsyncSession,
    *,
    company_id: object,
    altegio_record_id: object,
) -> ReminderOwner:
    """Who owns the reminders for one exact Altegio source booking.

    Runs inside the caller's transaction on purpose. The planner calls it after
    it already holds the ``Record`` state for this delivery, so a webhook that
    was waiting behind the handover's own row locks sees the marker the moment
    it is unblocked — rather than a stale answer read before the commit.

    Exact identity only: ``(altegio, company_id, altegio_record_id)`` is the
    ledger's natural key, so at most one row can match. More than one would mean
    the key is not what it claims, which is ``UNKNOWN``, not permission.
    """
    if type(company_id) is not int or type(altegio_record_id) is not int:
        # An identity we cannot state is an identity we cannot check. This is
        # reachable from a delivery whose ids were never proven, and guessing
        # here would guess about somebody's messages.
        return ReminderOwner.UNKNOWN

    stmt = (
        select(
            EasyWeekMigrationLedger.target_provider,
            EasyWeekMigrationLedger.status,
            EasyWeekMigrationLedger.target_booking_uuid,
            EasyWeekMigrationLedger.reminders_handed_over_at,
            EasyWeekMigrationLedger.reminder_handover_plan_digest,
            EasyWeekMigrationLedger.reminders_suppressed_at,
            EasyWeekMigrationLedger.reminder_suppression_plan_digest,
            EasyWeekMigrationLedger.reminder_suppression_reason_code,
        )
        .where(EasyWeekMigrationLedger.source_provider == PROVIDER_ALTEGIO)
        .where(EasyWeekMigrationLedger.source_company_id == company_id)
        .where(EasyWeekMigrationLedger.source_record_id == altegio_record_id)
    )
    try:
        rows = list((await session.execute(stmt)).all())
    except Exception:
        # The question could not be put to the database at all. That is not an
        # answer, and it is certainly not permission: the caller treats UNKNOWN
        # as "do not create, do not send" and retries later.
        logger.error(
            "reminder ownership lookup failed: company_id=%s source_record_id=%s",
            company_id,
            altegio_record_id,
        )
        return ReminderOwner.UNKNOWN

    if not rows:
        # No ledger row at all, or one for a different company. Never migrated
        # here, so the ordinary Altegio path owns it exactly as before.
        return ReminderOwner.ALTEGIO
    if len(rows) > 1:  # pragma: no cover - the unique constraint forbids it
        logger.error(
            "reminder ownership ambiguous: company_id=%s source_record_id=%s rows=%d",
            company_id,
            altegio_record_id,
            len(rows),
        )
        return ReminderOwner.UNKNOWN

    (
        target_provider,
        status,
        target_booking_uuid,
        handed_over_at,
        handover_digest,
        suppressed_at,
        suppression_digest,
        suppression_reason,
    ) = rows[0]
    ownership_empty = handed_over_at is None and handover_digest is None
    suppression_empty = suppressed_at is None and suppression_digest is None and suppression_reason is None

    if ownership_empty and suppression_empty:
        # A migrated booking whose reminders were never handed over. This is the
        # ordinary state for every wave that has not run the handover yet, and
        # it must NOT suppress anything.
        return ReminderOwner.ALTEGIO

    identity_proven = (
        target_provider == PROVIDER_EASYWEEK and status == "created" and _valid_target_uuid(target_booking_uuid)
    )
    ownership_complete = handed_over_at is not None and _valid_digest(handover_digest)
    suppression_complete = (
        suppressed_at is not None
        and _valid_digest(suppression_digest)
        and suppression_reason == REMINDER_SUPPRESSION_REASON_CODE
    )

    if identity_proven and ownership_complete and suppression_empty:
        return ReminderOwner.EASYWEEK
    if identity_proven and ownership_empty and suppression_complete:
        return ReminderOwner.SUPPRESSED

    # Half/dual/corrupt markers or a marker on an unproven migration identity.
    # The database constraints make most of these states unreachable through a
    # supported write, but direct SQL or a partially applied external migration
    # must still fail closed rather than becoming permission to send.
    logger.error(
        "reminder ownership marker is invalid: company_id=%s source_record_id=%s",
        company_id,
        altegio_record_id,
    )
    return ReminderOwner.UNKNOWN


async def altegio_reminders_are_suppressed(
    session: AsyncSession,
    *,
    company_id: object,
    altegio_record_id: object,
) -> tuple[bool, ReminderOwner]:
    """Should the Altegio path refrain from creating a reminder here?

    ``True`` for ``EASYWEEK``, ``SUPPRESSED`` and ``UNKNOWN``: an unanswerable
    question is not a licence. Returned with the owner so the caller can log and
    act on the difference — two are normal outcomes after a handover, the other
    is something a person needs to look at.
    """
    owner = await reminder_owner(session, company_id=company_id, altegio_record_id=altegio_record_id)
    return owner is not ReminderOwner.ALTEGIO, owner
