"""The pinned identity and the closed reason vocabulary of the §36 canary.

Literals a reviewer sees in a diff, not values a deployment can change. The
branch, the template and the nominal are the same ones §35 proved in
production; repeating them here as constants rather than reading them from
configuration is what stops a misconfigured environment from pointing a real
payment and a real customer message at something nobody approved.

The runtime identities that CANNOT be constants — which customer, which staffer,
which payment account, which recipient — arrive per run and are proven live.
"""

from __future__ import annotations

import hashlib
from typing import Final

# One canary, one scope. A second canary is deliberately a code change plus a
# review, not a flag somebody can flip twice.
VOUCHER_DELIVERY_SCOPE: Final = "easyweek_voucher_delivery_canary_v1"
VOUCHER_DELIVERY_SCHEMA_VERSION: Final = "1"

# The campaign this entitlement belongs to. A voucher earned by a first visit is
# not transferable to another campaign's audience.
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
# The closed reason vocabulary
# ---------------------------------------------------------------------------
# Stable, PII-free strings. A wrapper acts on these; prose is for humans only.

# -- fences ----------------------------------------------------------------
CANARY_DISABLED: Final = "voucher_delivery_canary_disabled"
HMAC_KEY_MISSING: Final = "voucher_delivery_hmac_key_missing"
HMAC_KEY_INVALID: Final = "voucher_delivery_hmac_key_invalid"

# -- the message it would be delivered with --------------------------------
TEMPLATE_UNPROVEN: Final = "voucher_delivery_template_unproven"
TEMPLATE_MISMATCH: Final = "voucher_delivery_template_mismatch"
SENDER_UNPROVEN: Final = "voucher_delivery_sender_unproven"

# -- who it would be delivered to ------------------------------------------
RECIPIENT_IDENTITY_UNPROVEN: Final = "recipient_identity_unproven"
SOURCE_BOOKING_NOT_CURRENT: Final = "source_booking_not_current"
CUSTOMER_IDENTITY_NOT_CURRENT: Final = "customer_identity_not_current"
CUSTOMER_HISTORY_INCOMPLETE: Final = "customer_history_incomplete"
FIRST_VISIT_NOT_CURRENT: Final = "first_visit_not_current"
ACTIVE_FUTURE_BOOKING_PRESENT: Final = "active_future_booking_present"
LIVE_GUARD_UNCERTAIN: Final = "live_guard_uncertain"

# -- what has already happened ---------------------------------------------
CANARY_SCOPE_ALREADY_CONSUMED: Final = "canary_scope_already_consumed"
VOUCHER_ENTITLEMENT_ALREADY_EXISTS: Final = "voucher_entitlement_already_exists"

# -- the voucher itself ------------------------------------------------------
VOUCHER_ORDER_UNPROVEN: Final = "voucher_order_unproven"
VOUCHER_ARTIFACT_UNPROVEN: Final = "voucher_artifact_unproven"
VOUCHER_BINDING_MISMATCH: Final = "voucher_binding_mismatch"
VOUCHER_ORDER_NOT_PAYABLE: Final = "voucher_order_not_payable"
VOUCHER_ORDER_NOT_PAID: Final = "voucher_order_not_paid"

# -- the send ---------------------------------------------------------------
DELIVERY_ALREADY_ATTEMPTED: Final = "delivery_already_attempted"
DELIVERY_OUTCOME_UNKNOWN: Final = "delivery_outcome_unknown"
MANUAL_CLEANUP_REQUIRED: Final = "manual_cleanup_required"

# -- operator authorisation --------------------------------------------------
LEDGER_STATE_UNEXPECTED: Final = "voucher_delivery_ledger_state_unexpected"
PLAN_DIGEST_MISMATCH: Final = "voucher_delivery_plan_digest_mismatch"
PLAN_EXPIRED: Final = "voucher_delivery_plan_expired"
CONFIRMATION_MISMATCH: Final = "voucher_delivery_confirmation_mismatch"
UNKNOWN_STAGE: Final = "voucher_delivery_unknown_stage"
APPLY_FLAG_MISSING: Final = "voucher_delivery_apply_flag_missing"
IDENTITY_BINDING_MISMATCH: Final = "voucher_delivery_identity_binding_mismatch"
RUNTIME_IDENTITY_UNUSABLE: Final = "voucher_delivery_runtime_identity_unusable"
API_UNAVAILABLE: Final = "voucher_delivery_api_unavailable"
API_UNCERTAIN: Final = "voucher_delivery_api_uncertain"
REFUND_FORBIDDEN_AFTER_SEND: Final = "voucher_delivery_refund_forbidden_after_send"
MUTATION_UNKNOWN: Final = "voucher_delivery_mutation_unknown"
MUTATION_REJECTED: Final = "voucher_delivery_mutation_rejected"


def delivery_marker(*, preview_run_id: int, campaign_recipient_id: int) -> str:
    """The non-personal comment marker for this canary's one order.

    Deterministic, so a reconciliation after a crash can recompute exactly the
    marker it would have sent and an operator can paste it into the EasyWeek
    dashboard to find an open draft. Derived from the scope and the two ids the
    operator chose, so it also cannot collide with the §35 marker or with a
    different recipient's.

    A digest rather than the ids themselves: the marker ends up on a real order
    in a real POS system that other people read.
    """
    material = f"{VOUCHER_DELIVERY_SCOPE}:{preview_run_id}:{campaign_recipient_id}"
    return "ewvd1-" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:12]


__all__ = [
    "MUTATION_STAGES",
    "NEW_CLIENT_CAMPAIGN_CODE",
    "STAGE_CREATE",
    "STAGE_DELIVER",
    "STAGE_PAY",
    "STAGE_REFUND",
    "VOUCHER_DELIVERY_SCHEMA_VERSION",
    "VOUCHER_DELIVERY_SCOPE",
    "VOUCHER_TEMPLATE_CODE",
    "delivery_marker",
]
