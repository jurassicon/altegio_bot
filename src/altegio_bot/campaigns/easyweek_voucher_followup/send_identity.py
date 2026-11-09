"""Names, versions and reason codes for the follow-up SEND (§46.7–46.8, PR-22.3).

One vocabulary, for the same reason the preview has one: these strings reach an
operator's screen, a durable approval, an execution row and an audit line, so
they have to mean the same thing in all four and they have to be safe to show.
Every code here is a label — no phone number, no name, no voucher code, no
rendered body, no raw provider identifier and no configuration value is part of
one.

What this stage is
------------------
The half that can reach a person. A preview proposes; this consumes one extra
attempt per original recipient, once, and records honestly what became of it.
So every default here is a refusal, and every uncertainty is its own answer
rather than a quiet yes.

Where the attempt is consumed
-----------------------------
At the CLAIM, and the claim is a committed transaction that happens strictly
before the external POST. :data:`ATTEMPT_BOUNDARY` names that moment in one
place so the contract, the runbook and the tests all point at the same line:
once that commit returns, this original recipient's one extra message is spent
whatever the network then does, and nothing — not a crash, not a restart, not a
new confirmation, not a new run — gives it back.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Final

from altegio_bot.models.models import (
    FOLLOWUP_ACTION_SEND,
    FOLLOWUP_SEND_ACCEPTED,
    FOLLOWUP_SEND_ATTEMPTED_STATUSES,
    FOLLOWUP_SEND_BLOCKING_STATUSES,
    FOLLOWUP_SEND_CLAIMED,
    FOLLOWUP_SEND_DELIVERED,
    FOLLOWUP_SEND_FAILED,
    FOLLOWUP_SEND_PLANNED,
    FOLLOWUP_SEND_READ,
    FOLLOWUP_SEND_REJECTED,
    FOLLOWUP_SEND_SKIPPED,
    FOLLOWUP_SEND_STOPPED,
    FOLLOWUP_SEND_UNKNOWN,
)

__all__ = [
    "APPROVAL_ALREADY_CONSUMED",
    "APPROVAL_CONTRACT_MOVED",
    "APPROVAL_COUNT_UNCONFIRMED",
    "APPROVAL_EXPIRED",
    "APPROVAL_PRINCIPAL_MISMATCH",
    "APPROVAL_SIGNATURE_INVALID",
    "APPROVAL_UNKNOWN",
    "ATTEMPT_ALREADY_USED",
    "ATTEMPT_BOUNDARY",
    "CLAIM_LEASE_LOST",
    "CONSENT_MOVED",
    "DATABASE_UNAVAILABLE",
    "EXECUTOR_UNAVAILABLE",
    "FOLLOWUP_ACTION_SEND",
    "LEASE",
    "META_ACCEPTED",
    "META_REJECTED",
    "META_UNKNOWN",
    "PLAN_MAX_AGE",
    "PREVIEW_NOT_CURRENT",
    "PREVIEW_UNKNOWN",
    "RUN_ALREADY_ACTIVE",
    "RUN_INTERRUPTED",
    "RUN_STOPPED",
    "SEND_CONTRACT_VERSION",
    "SEND_DISABLED",
    "SOURCE_TERMINAL_STOP",
    "TERMINALLY_STOPPED",
    "attempt_consumed",
    "blocks_further_attempt",
]

# The version of the whole send contract: what is checked, in what order, where
# the attempt is consumed and what each outcome means. An approval records it,
# so a plan built under one version can never be confirmed under another.
SEND_CONTRACT_VERSION: Final = "followup-send-v1"

# How long an offer may be confirmed for, measured from when the plan was
# ISSUED and never from when it was stored. A plan read half an hour ago
# describes a world that has had half an hour to change.
PLAN_MAX_AGE: Final = timedelta(minutes=10)

# How long an executor may hold a run before a sweep calls it abandoned. Only
# ever reached when the single executor died: a live one renews while it works.
LEASE: Final = timedelta(minutes=10)

# The exact line past which this original recipient's one extra attempt is
# spent. Named rather than described, because the whole safety argument of this
# stage is that everybody means the same line by it.
ATTEMPT_BOUNDARY: Final = (
    "the committed transaction that moves this recipient's follow-up row to "
    "'claimed' with attempt_count = 1 and a durable intent, which happens "
    "strictly before the one external POST and is never reversed"
)

# ---------------------------------------------------------------------------
# Why a whole plan, confirmation or run cannot proceed
# ---------------------------------------------------------------------------
# The follow-up's OWN fence. Closed by default, and the primary mailing's
# permission does not open it: §46.8 requires a separate switch, so this code
# reads a separate setting and a deployment with the §42 fence open still
# refuses here.
SEND_DISABLED: Final = "followup_send_disabled"
EXECUTOR_UNAVAILABLE: Final = "followup_executor_unavailable"
PREVIEW_UNKNOWN: Final = "followup_preview_unknown"
# The stored preview is no longer the current answer: it has expired, its
# message contract has moved, its source composition or stop generation has
# changed, or something blocks a follow-up for this batch right now.
PREVIEW_NOT_CURRENT: Final = "followup_preview_not_current"
APPROVAL_UNKNOWN: Final = "followup_approval_unknown"
APPROVAL_EXPIRED: Final = "followup_approval_expired"
APPROVAL_ALREADY_CONSUMED: Final = "followup_approval_already_consumed"
APPROVAL_PRINCIPAL_MISMATCH: Final = "followup_approval_principal_mismatch"
# The presented signature is not the one this server issued over this row.
APPROVAL_SIGNATURE_INVALID: Final = "followup_approval_signature_invalid"
APPROVAL_COUNT_UNCONFIRMED: Final = "followup_approval_count_unconfirmed"
# Something the approval pinned has changed since: the template, the policy, the
# sender, the source composition, the source stop generation, the preview.
APPROVAL_CONTRACT_MOVED: Final = "followup_approval_contract_moved"
RUN_ALREADY_ACTIVE: Final = "followup_run_already_active"
RUN_STOPPED: Final = "followup_run_stopped"
RUN_INTERRUPTED: Final = "followup_run_interrupted"
DATABASE_UNAVAILABLE: Final = "followup_database_unavailable"
# The ORIGINAL batch has been terminally stopped. §46.5: that forbids creating
# or executing its follow-up as well.
SOURCE_TERMINAL_STOP: Final = "followup_source_terminal_stop"

# ---------------------------------------------------------------------------
# Why ONE recipient is not sent to
# ---------------------------------------------------------------------------
# This original recipient's one extra attempt is already accounted for: spent in
# some run, or tombstoned by a terminal stop. Reported rather than silently
# dropped, because an operator reading a smaller result has to know why.
ATTEMPT_ALREADY_USED: Final = "followup_attempt_already_used"
TERMINALLY_STOPPED: Final = "followup_terminally_stopped"
# The consent aggregate moved between the preview and the final boundary, or a
# refusal or an unresolved inbound message arrived. The locked guard's own
# reason is kept beside this in the row's evidence.
CONSENT_MOVED: Final = "followup_consent_moved"
# The executor lost this run's lease — a second executor, or a sweep that called
# it abandoned — so it may not claim on its behalf.
CLAIM_LEASE_LOST: Final = "followup_claim_lease_lost"

# ---------------------------------------------------------------------------
# What one external attempt turned out to be
# ---------------------------------------------------------------------------
# Deliberately three, and deliberately not "delivered": accepted means Meta took
# responsibility for the message, which is not a phone displaying it.
META_ACCEPTED: Final = "followup_meta_accepted"
META_UNKNOWN: Final = "followup_meta_unknown"
META_REJECTED: Final = "followup_meta_rejected"


def attempt_consumed(status: str) -> bool:
    """Did this row really spend its one external attempt?

    Narrower than :func:`blocks_further_attempt`: a terminal tombstone blocks a
    resend without anything having been sent, and the two questions must not be
    answered by one predicate — a report that counted tombstones as attempts
    would overstate what reached real people.
    """
    return status in FOLLOWUP_SEND_ATTEMPTED_STATUSES


def blocks_further_attempt(status: str) -> bool:
    """Does this row occupy the single-attempt key for its original recipient?

    True for every spent attempt AND for a terminal stop tombstone. False only
    for a plain reservation and for a row that lost eligibility before the
    boundary — the two states §46.7 allows to be recomputed.
    """
    return status in FOLLOWUP_SEND_BLOCKING_STATUSES


# Re-exported so a caller has one import for the row vocabulary and cannot
# reach for a status name this phase does not use.
PLANNED: Final = FOLLOWUP_SEND_PLANNED
SKIPPED: Final = FOLLOWUP_SEND_SKIPPED
CLAIMED: Final = FOLLOWUP_SEND_CLAIMED
ACCEPTED: Final = FOLLOWUP_SEND_ACCEPTED
DELIVERED: Final = FOLLOWUP_SEND_DELIVERED
READ: Final = FOLLOWUP_SEND_READ
REJECTED: Final = FOLLOWUP_SEND_REJECTED
FAILED: Final = FOLLOWUP_SEND_FAILED
UNKNOWN: Final = FOLLOWUP_SEND_UNKNOWN
STOPPED: Final = FOLLOWUP_SEND_STOPPED
