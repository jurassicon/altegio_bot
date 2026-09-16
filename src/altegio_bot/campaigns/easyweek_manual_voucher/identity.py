"""The pinned identity, baseline and reason vocabulary of the §37.2 canary.

§36 delivers a voucher to somebody whose first visit earned it, or to the one
pre-configured test account. This canary delivers to somebody an **operator
chose by hand** in a preview: a real customer, with a real phone, and no
first-visit proof behind them at all.

That difference is the whole reason this module exists rather than a flag on
§36. The manual basis is a human decision, and a human decision is not an
entitlement — so it must never be written into a ledger that says "earned", must
never borrow a booking UUID it does not have, and must never reuse the scope
that says "this workspace has had its one canary".

Everything here is a literal a reviewer sees in a diff. The runtime identities
that cannot be constants — which customer, which staffer, which payment account,
which recipient — arrive per run and are proven live.
"""

from __future__ import annotations

import hashlib
from typing import Any, Final

from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    SUPPORTED_VOUCHER_PRICE_MINOR,
)

# One canary, one scope, enforced by a unique constraint. A second canary is
# deliberately a code change plus a review, not a flag somebody flips twice.
MANUAL_VOUCHER_SCOPE: Final = "easyweek_manual_voucher_canary_v1"
MANUAL_VOUCHER_SCHEMA_VERSION: Final = "1"

# The one branch and the one campaign this canary may ever name.
KARLSRUHE_COMPANY_ID: Final = 322579
NEW_CLIENT_CAMPAIGN_CODE: Final = "new_clients_monthly"

# The internal template code of the message that carries the voucher. NOT the
# old `newsletter_new_clients_monthly`: that one promises 10% and a Kundenkarte,
# which is a different offer and would be a false statement to a customer.
VOUCHER_TEMPLATE_CODE: Final = "new_client_voucher"

# The stages, in the only order they may happen.
STAGE_CREATE: Final = "create"
STAGE_PAY: Final = "pay"
STAGE_DELIVER: Final = "deliver"
STAGE_REFUND: Final = "refund"
MUTATION_STAGES: Final = (STAGE_CREATE, STAGE_PAY, STAGE_DELIVER, STAGE_REFUND)

# ---------------------------------------------------------------------------
# The versioned baseline configuration
# ---------------------------------------------------------------------------
# §35 froze the template at 43/43 services and proved a production canary
# against it. That record is history and is not rewritten here.
#
# A read-only probe taken for this phase observed 42/42 twice, with both
# all-services and all-branches flags true and every other frozen field
# unchanged. So this canary carries its OWN baseline, named and versioned, and
# compares against that.
#
# What it must never do is adapt. A template that reads 41 is not "close
# enough": it is a product somebody edited while a €15 payment was pending, and
# the only safe response is to stop and ask a human. Changing this literal is a
# code change a reviewer sees.
MANUAL_BASELINE_VERSION: Final = "2026-09-15-42"

MANUAL_BASELINE_TEMPLATE_FACTS: Final[dict[str, Any]] = {
    "is_enabled": True,
    "is_online": False,
    "is_single_charge": True,
    "cost": SUPPORTED_VOUCHER_PRICE_MINOR,
    "value": SUPPORTED_VOUCHER_PRICE_MINOR,
    "validity": None,
    "forces_activation": True,
    "activate_after": 0,
    "activate_at": None,
    "is_connected_all_branches": True,
    "branches_count": 3,
    "all_branches_count": 3,
    "is_connected_all_services": True,
    "services_count": 42,
    "all_services_count": 42,
    "goods_count": 0,
}

# The template the baseline describes. Repeated here so a reader of this module
# sees which template the numbers belong to.
MANUAL_BASELINE_TEMPLATE_UUID: Final = EASYWEEK_VOUCHER_TEMPLATE_UUID

# ---------------------------------------------------------------------------
# The closed reason vocabulary
# ---------------------------------------------------------------------------
# Stable, PII-free strings. A wrapper acts on these; prose is for humans only.
# Prefixed `manual_voucher_` so a report cannot be mistaken for a §36 one.

# -- fences ----------------------------------------------------------------
CANARY_DISABLED: Final = "manual_voucher_canary_disabled"
STAFFER_UNCONFIGURED: Final = "manual_voucher_staffer_unconfigured"
ACCOUNT_UNCONFIGURED: Final = "manual_voucher_account_unconfigured"
HMAC_KEY_MISSING: Final = "manual_voucher_hmac_key_missing"
HMAC_KEY_INVALID: Final = "manual_voucher_hmac_key_invalid"

# -- the message it would be delivered with --------------------------------
TEMPLATE_UNPROVEN: Final = "manual_voucher_template_unproven"
SENDER_UNPROVEN: Final = "manual_voucher_sender_unproven"
# The approved template takes three positional parameters and the third is a
# booking link. A message that cannot be filled completely is not one to send,
# and an empty string in that slot would be a link to nowhere in a real
# customer's WhatsApp.
BOOKING_LINK_UNPROVEN: Final = "manual_voucher_booking_link_unproven"
TEMPLATE_PARAMETERS_UNPROVEN: Final = "manual_voucher_template_parameters_unproven"

# -- who it would be delivered to ------------------------------------------
# Deliberately separate codes: "the run is not one we serve", "the row is not a
# manual selection" and "the person we read is not the person we stored" are
# three different situations for the operator holding the terminal.
RUN_UNPROVEN: Final = "manual_voucher_run_unproven"
RECIPIENT_UNPROVEN: Final = "manual_voucher_recipient_unproven"
RECIPIENT_BASIS_UNSUPPORTED: Final = "manual_voucher_recipient_basis_unsupported"
RECIPIENT_NOT_CANDIDATE: Final = "manual_voucher_recipient_not_candidate"
CUSTOMER_UUID_MISSING: Final = "manual_voucher_customer_uuid_missing"
CUSTOMER_IDENTITY_NOT_CURRENT: Final = "manual_voucher_customer_identity_not_current"
CUSTOMER_PHONE_NOT_CURRENT: Final = "manual_voucher_customer_phone_not_current"
CUSTOMER_NAME_MISSING: Final = "manual_voucher_customer_name_missing"
LOCAL_CLIENT_UNPROVEN: Final = "manual_voucher_local_client_unproven"
# Two EasyWeek customers answer to this number — a couple, a family phone, a
# duplicated import. Not something to pick a winner from when the prize is a
# real €15 code.
CUSTOMER_AMBIGUOUS: Final = "manual_voucher_customer_ambiguous"
# The workspace-wide read did not finish, or did not happen: an unfinished read
# is not an absence and not a proof.
CUSTOMER_LOOKUP_UNDETERMINED: Final = "manual_voucher_customer_lookup_undetermined"
RECIPIENT_OPTED_OUT: Final = "manual_voucher_recipient_opted_out"
LIVE_GUARD_UNCERTAIN: Final = "manual_voucher_live_guard_uncertain"

# -- what has already happened ---------------------------------------------
CANARY_SCOPE_ALREADY_CONSUMED: Final = "manual_voucher_scope_already_consumed"
ENTITLEMENT_ALREADY_EXISTS: Final = "manual_voucher_entitlement_already_exists"
SNAPSHOT_NOT_FROZEN: Final = "manual_voucher_snapshot_not_frozen"

# -- the voucher itself ------------------------------------------------------
ORDER_UNPROVEN: Final = "manual_voucher_order_unproven"
ARTIFACT_UNPROVEN: Final = "manual_voucher_artifact_unproven"
BINDING_MISMATCH: Final = "manual_voucher_binding_mismatch"
ORDER_NOT_PAYABLE: Final = "manual_voucher_order_not_payable"
ORDER_NOT_PAID: Final = "manual_voucher_order_not_paid"
ORDER_ALREADY_REFUNDED: Final = "manual_voucher_order_already_refunded"
ORDER_STATE_UNATTRIBUTABLE: Final = "manual_voucher_order_state_unattributable"

# -- the baseline ------------------------------------------------------------
# Separate from every other refusal: a drift here means the product changed
# under a pending payment, and the operator has to decide, not the tool.
BASELINE_DRIFT: Final = "manual_voucher_baseline_drift"
BASELINE_COUNTERS_UNREADABLE: Final = "manual_voucher_baseline_counters_unreadable"

# -- reconciliation of an unknown create --------------------------------------
# Zero matches is not "it was not created": the walk may simply not have seen
# it. Several matches is a full stop. Both stay unknown.
MARKER_SEARCH_UNRESOLVED: Final = "manual_voucher_marker_search_unresolved"
MARKER_SEARCH_AMBIGUOUS: Final = "manual_voucher_marker_search_ambiguous"
MARKER_SEARCH_INCOMPLETE: Final = "manual_voucher_marker_search_incomplete"

# The reconcile ran and the uncertainty it was asked about is still there. Its
# own code, because "we looked and cannot say" is not the same as any of the
# specific things that could be wrong.
RECONCILE_UNRESOLVED: Final = "manual_voucher_reconcile_unresolved"

# -- the send ---------------------------------------------------------------
DELIVERY_ALREADY_ATTEMPTED: Final = "manual_voucher_delivery_already_attempted"
DELIVERY_OUTCOME_UNKNOWN: Final = "manual_voucher_delivery_outcome_unknown"
MANUAL_CLEANUP_REQUIRED: Final = "manual_voucher_manual_cleanup_required"
REFUND_FORBIDDEN_AFTER_SEND: Final = "manual_voucher_refund_forbidden_after_send"

# -- operator authorisation --------------------------------------------------
LEDGER_STATE_UNEXPECTED: Final = "manual_voucher_ledger_state_unexpected"
LEDGER_IDENTITY_INCOMPLETE: Final = "manual_voucher_ledger_identity_incomplete"
IDENTITY_BINDING_MISMATCH: Final = "manual_voucher_identity_binding_mismatch"
PLAN_DIGEST_MISMATCH: Final = "manual_voucher_plan_digest_mismatch"
PLAN_EXPIRED: Final = "manual_voucher_plan_expired"
CONFIRMATION_MISMATCH: Final = "manual_voucher_confirmation_mismatch"
UNKNOWN_STAGE: Final = "manual_voucher_unknown_stage"
APPLY_FLAG_MISSING: Final = "manual_voucher_apply_flag_missing"
RUNTIME_IDENTITY_UNUSABLE: Final = "manual_voucher_runtime_identity_unusable"
API_UNAVAILABLE: Final = "manual_voucher_api_unavailable"
API_UNCERTAIN: Final = "manual_voucher_api_uncertain"
MUTATION_UNKNOWN: Final = "manual_voucher_mutation_unknown"
MUTATION_REJECTED: Final = "manual_voucher_mutation_rejected"
DATABASE_UNAVAILABLE: Final = "manual_voucher_database_unavailable"


def manual_marker(*, preview_run_id: int, campaign_recipient_id: int) -> str:
    """The non-personal comment marker for this canary's one order.

    Deterministic, so a reconciliation after a crash recomputes exactly the
    marker it would have sent and an operator can paste it into the EasyWeek
    dashboard to find an open draft. Derived from THIS scope, so it cannot
    collide with the §35 or §36 marker or with another recipient's.

    A digest rather than the ids themselves: the marker lands on a real order in
    a real POS system that other people read.
    """
    material = f"{MANUAL_VOUCHER_SCOPE}:{preview_run_id}:{campaign_recipient_id}"
    return "ewmv1-" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:12]


__all__ = [
    "KARLSRUHE_COMPANY_ID",
    "MANUAL_BASELINE_TEMPLATE_FACTS",
    "MANUAL_BASELINE_TEMPLATE_UUID",
    "MANUAL_BASELINE_VERSION",
    "MANUAL_VOUCHER_SCHEMA_VERSION",
    "MANUAL_VOUCHER_SCOPE",
    "MUTATION_STAGES",
    "NEW_CLIENT_CAMPAIGN_CODE",
    "STAGE_CREATE",
    "STAGE_DELIVER",
    "STAGE_PAY",
    "STAGE_REFUND",
    "VOUCHER_TEMPLATE_CODE",
    "manual_marker",
]
