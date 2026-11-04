"""Names, versions and reason codes for the follow-up preview (§46.2, PR-22.2).

One place for the vocabulary, for the same reason the production phase has one:
these strings reach an operator's screen, a stored snapshot and a future
execution ledger, so they have to mean the same thing in all three and they have
to be safe to show. Every code here is a label — no phone number, no name, no
voucher code, no raw provider identifier and no configuration value is ever part
of one.

What this stage is and is not
-----------------------------
A preview is a PROPOSAL. It reads the original mailing, judges each recipient
against evidence that already exists, and records what it found. It reserves
nothing, claims nothing, spends no send attempt and opens no send path: the
executor, its single-attempt key and its terminal tombstones are PR-22.3.
"""

from __future__ import annotations

from typing import Final

from altegio_bot.models.models import (
    FOLLOWUP_GROUP_DELIVERED,
    FOLLOWUP_GROUP_FAILED,
    FOLLOWUP_GROUP_READ,
    FOLLOWUP_GROUPS,
    FOLLOWUP_ROW_EXCLUDED,
    FOLLOWUP_ROW_INCLUDED,
    FOLLOWUP_ROW_UNKNOWN,
)

__all__ = [
    "BATCH_NOT_FOUND",
    "BATCH_NOT_FROZEN",
    "BATCH_SCHEMA_UNSUPPORTED",
    "BATCH_SCOPE_UNSUPPORTED",
    "BATCH_TERMINAL_STOP",
    "CONSENT_PENDING_INBOUND",
    "CONSENT_REFUSED",
    "CONSENT_UNKNOWN",
    "CONTRACT_VERSION",
    "CUSTOMER_REPLIED",
    "DUPLICATE_SOURCE",
    "ENTITLEMENT_INVALID",
    "ENTITLEMENT_UNPROVEN",
    "EVIDENCE_UNPROVEN",
    "FAILURE_POLICY_UNPROVEN",
    "FOLLOWUP_GROUPS",
    "FOLLOWUP_GROUP_DELIVERED",
    "FOLLOWUP_GROUP_FAILED",
    "FOLLOWUP_GROUP_READ",
    "FOLLOWUP_ROW_EXCLUDED",
    "FOLLOWUP_ROW_INCLUDED",
    "FOLLOWUP_ROW_UNKNOWN",
    "FUTURE_BOOKING_ACTIVE",
    "GROUPS_REQUIRED",
    "HISTORY_UNPROVEN",
    "IDENTITY_CONFLICT",
    "INTERVAL_NOT_ELAPSED",
    "INTERVAL_REQUIRED",
    "NEW_BOOKING_AFTER_ATTEMPT",
    "PREVIEW_VALIDITY_SECONDS",
    "SENDER_UNPROVEN",
    "SOURCE_ATTEMPT_UNPROVEN",
    "TEMPLATE_UNPROVEN",
    "canonical_groups",
]

# The version of the whole preview contract: what is checked, in what order, and
# what a stored snapshot means. A change to any of that is a new version, and an
# existing snapshot keeps the version it was computed under.
CONTRACT_VERSION: Final = "followup-preview-v1"

# How long a stored preview is treated as current. A snapshot is evidence of
# what was true at ``checked_at``, never a standing permission: consent, a
# reply, a booking or a stop can all land a minute later. Twenty-four hours is
# long enough for an operator to read the list and come back to it, and short
# enough that nobody acts on a week-old proof.
PREVIEW_VALIDITY_SECONDS: Final = 24 * 60 * 60

# ---------------------------------------------------------------------------
# Why a whole preview cannot be produced
# ---------------------------------------------------------------------------
BATCH_NOT_FOUND: Final = "followup_batch_not_found"
# §46.2: the first version supports the original 10 EUR request schemas and
# nothing else. A canary, a snapshot ledger, a manual-basis batch or an Altegio
# campaign is refused by name rather than filtered quietly.
BATCH_SCHEMA_UNSUPPORTED: Final = "followup_batch_schema_unsupported"
BATCH_SCOPE_UNSUPPORTED: Final = "followup_batch_scope_unsupported"
BATCH_NOT_FROZEN: Final = "followup_batch_not_frozen"
# §45.4: a terminal operator STOP on the original batch forbids creating its
# follow-up preview at all. Checked on the server on every request — a hidden
# button is not a check.
BATCH_TERMINAL_STOP: Final = "followup_batch_terminal_stop"
GROUPS_REQUIRED: Final = "followup_groups_required"
INTERVAL_REQUIRED: Final = "followup_interval_required"
TEMPLATE_UNPROVEN: Final = "followup_template_unproven"
SENDER_UNPROVEN: Final = "followup_sender_unproven"

# ---------------------------------------------------------------------------
# Why one recipient is not included
# ---------------------------------------------------------------------------
# The original attempt is the anchor for everything time-based. Without a proven
# one there is no interval to measure and no group to belong to.
SOURCE_ATTEMPT_UNPROVEN: Final = "followup_source_attempt_unproven"
INTERVAL_NOT_ELAPSED: Final = "followup_interval_not_elapsed"
# Anything that is not one of the three proven groups: accepted-only, unsent, a
# timeout, ``send_unknown``, a slot with no attempt, a local validation refusal,
# a CREATE or PAY error. None of them is "unread" and none is a confirmed
# failure (§46.3).
EVIDENCE_UNPROVEN: Final = "followup_evidence_unproven"
# A confirmed failure exists, but no proven policy admits its cause. The default
# is to refuse: §46.3 forbids inventing permitted error codes or waiting times.
FAILURE_POLICY_UNPROVEN: Final = "followup_failure_policy_unproven"
CONSENT_REFUSED: Final = "followup_consent_refused"
CONSENT_PENDING_INBOUND: Final = "followup_consent_pending_inbound"
CONSENT_UNKNOWN: Final = "followup_consent_unknown"
CUSTOMER_REPLIED: Final = "followup_customer_replied"
NEW_BOOKING_AFTER_ATTEMPT: Final = "followup_new_booking_after_attempt"
FUTURE_BOOKING_ACTIVE: Final = "followup_future_booking_active"
# The live read could not establish the booking history: a timeout, incomplete
# pagination, an identity mismatch, a foreign workspace. Unknown, never an empty
# list of bookings.
HISTORY_UNPROVEN: Final = "followup_history_unproven"
IDENTITY_CONFLICT: Final = "followup_identity_conflict"
# The ORIGINAL artifact is what a follow-up may talk about. Explicitly expired,
# invalid or refunded excludes; an absent individual date does not (§45.4).
ENTITLEMENT_INVALID: Final = "followup_entitlement_invalid"
ENTITLEMENT_UNPROVEN: Final = "followup_entitlement_unproven"
# Another slot of this same version already represents this person or this
# entitlement. Reported rather than silently dropped.
DUPLICATE_SOURCE: Final = "followup_duplicate_source"


def canonical_groups(selected: object) -> tuple[str, ...]:
    """The operator's selection, in one canonical order, or empty.

    Sorted and de-duplicated so the stored snapshot and its digest do not depend
    on the order a form happened to submit. An unknown name is dropped here and
    the caller refuses on the empty result, which keeps "they asked for a group
    that does not exist" from silently becoming "they asked for all of them".
    """
    if not isinstance(selected, (list, tuple, set, frozenset)):
        return ()
    chosen = {value for value in selected if isinstance(value, str) and value in FOLLOWUP_GROUPS}
    return tuple(group for group in FOLLOWUP_GROUPS if group in chosen)
