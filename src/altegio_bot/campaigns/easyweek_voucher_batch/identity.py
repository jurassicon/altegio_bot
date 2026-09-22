"""The pinned identity, limits and reason vocabulary of the §41 batch (PR-18).

§37.2 proved the whole irreversible sequence — issue, pay, deliver, observe —
for exactly one manually selected person, in production, on 15.09.2026. This
phase repeats that sequence for a **bounded handful** of them, and the only new
question it has to answer is *how many*.

Everything about the answer is a literal here and a CHECK constraint in the
database: at most five recipients, at most €15 each, at most €75 in total, and
at most one batch ever. None of it is configuration. A ceiling an environment
variable could raise is not a ceiling, and "the sixth recipient" must be a row
PostgreSQL cannot store rather than a branch somebody could forget to write.

What arrives per run instead of being pinned — which preview, which recipients,
which staffer, which payment account — is proven live, every stage, from
scratch.
"""

from __future__ import annotations

import hashlib
from typing import Final

from altegio_bot.models.models import (
    VOUCHER_BATCH_CAMPAIGN_CODE,
    VOUCHER_BATCH_COMPANY_ID,
    VOUCHER_BATCH_MAX_EXPOSURE_MINOR,
    VOUCHER_BATCH_MAX_RECIPIENTS,
    VOUCHER_BATCH_SCHEMA_VERSION,
    VOUCHER_BATCH_SCOPE,
    VOUCHER_BATCH_UNIT_PRICE_MINOR,
)

# Re-exported from the model layer rather than re-declared, so that the literal
# a CHECK constraint enforces and the literal this package compares against can
# never drift apart.
BATCH_SCOPE: Final = VOUCHER_BATCH_SCOPE
BATCH_SCHEMA_VERSION: Final = VOUCHER_BATCH_SCHEMA_VERSION
MAX_RECIPIENTS: Final = VOUCHER_BATCH_MAX_RECIPIENTS
UNIT_PRICE_MINOR: Final = VOUCHER_BATCH_UNIT_PRICE_MINOR
MAX_EXPOSURE_MINOR: Final = VOUCHER_BATCH_MAX_EXPOSURE_MINOR
KARLSRUHE_COMPANY_ID: Final = VOUCHER_BATCH_COMPANY_ID
NEW_CLIENT_CAMPAIGN_CODE: Final = VOUCHER_BATCH_CAMPAIGN_CODE

# The internal template code of the message that carries the voucher. NOT the
# old `newsletter_new_clients_monthly`: that one promises 10% and a Kundenkarte,
# which is a different offer and would be a false statement to a customer.
VOUCHER_TEMPLATE_CODE: Final = "new_client_voucher"

# The stages, in the only order they may happen. `freeze` is first and is the
# only one that is purely local: it writes the composition and nothing leaves
# the process. The other three each reach EasyWeek or Meta.
STAGE_FREEZE: Final = "freeze"
STAGE_CREATE: Final = "create"
STAGE_PAY: Final = "pay"
STAGE_DELIVER: Final = "deliver"
STAGE_REFUND: Final = "refund"
BATCH_STAGES: Final = (STAGE_FREEZE, STAGE_CREATE, STAGE_PAY, STAGE_DELIVER, STAGE_REFUND)

# The stages that may reach the outside world, in the order an operator walks
# them. Deliberately a tuple a reader can see rather than a sequence some
# command executes: there is no function that runs two of these.
EXTERNAL_STAGES: Final = (STAGE_CREATE, STAGE_PAY, STAGE_DELIVER)

# ---------------------------------------------------------------------------
# The closed reason vocabulary
# ---------------------------------------------------------------------------
# Stable, PII-free strings. A wrapper acts on these; prose is for humans only.
# Prefixed `voucher_batch_` so a report can never be mistaken for a §36 or a
# §37.2 one, and so an operator reading a refusal knows which phase refused.

# -- fences ------------------------------------------------------------------
BATCH_DISABLED: Final = "voucher_batch_disabled"
STAFFER_UNCONFIGURED: Final = "voucher_batch_staffer_unconfigured"
ACCOUNT_UNCONFIGURED: Final = "voucher_batch_account_unconfigured"
HMAC_KEY_MISSING: Final = "voucher_batch_hmac_key_missing"
HMAC_KEY_INVALID: Final = "voucher_batch_hmac_key_invalid"
RUNTIME_IDENTITY_UNUSABLE: Final = "voucher_batch_runtime_identity_unusable"

# -- the message it would be delivered with ----------------------------------
TEMPLATE_UNPROVEN: Final = "voucher_batch_template_unproven"
SENDER_UNPROVEN: Final = "voucher_batch_sender_unproven"
BOOKING_LINK_UNPROVEN: Final = "voucher_batch_booking_link_unproven"
TEMPLATE_PARAMETERS_UNPROVEN: Final = "voucher_batch_template_parameters_unproven"

# -- the preview this batch would be frozen from -----------------------------
RUN_UNPROVEN: Final = "voucher_batch_run_unproven"
# The preview holds no active manually selected candidate at all. An empty batch
# is not a small batch: there is nothing to approve.
COMPOSITION_EMPTY: Final = "voucher_batch_composition_empty"
# More than five. Refused whole rather than truncated: a tool that quietly took
# the first five would be choosing who gets €15 and who does not.
COMPOSITION_TOO_LARGE: Final = "voucher_batch_composition_too_large"
# An earned or owner-test candidate sits in the same snapshot. This batch serves
# one basis, and a mixed snapshot is an operator's decision to make again, not
# one for a tool to resolve by filtering.
COMPOSITION_MIXED_BASIS: Final = "voucher_batch_composition_mixed_basis"
# Two active rows resolve to one EasyWeek customer.
COMPOSITION_DUPLICATE_CUSTOMER: Final = "voucher_batch_composition_duplicate_customer"
# This preview, or one of its recipients, already belongs to a historical
# voucher canary. Old evidence is not a fresh snapshot.
PREVIEW_ALREADY_CONSUMED: Final = "voucher_batch_preview_already_consumed"
ENTITLEMENT_ALREADY_EXISTS: Final = "voucher_batch_entitlement_already_exists"
# A batch already exists. The second one is a new owner-approved plan and a new
# PR, never a second run of this command.
BATCH_ALREADY_EXISTS: Final = "voucher_batch_already_exists"
BATCH_NOT_FROZEN: Final = "voucher_batch_not_frozen"
# The composition in the database is not the composition the operator approved.
FROZEN_DIGEST_MISMATCH: Final = "voucher_batch_frozen_digest_mismatch"
SNAPSHOT_NOT_FROZEN: Final = "voucher_batch_snapshot_not_frozen"

# -- who it would be delivered to --------------------------------------------
RECIPIENT_UNPROVEN: Final = "voucher_batch_recipient_unproven"
RECIPIENT_BASIS_UNSUPPORTED: Final = "voucher_batch_recipient_basis_unsupported"
RECIPIENT_NOT_CANDIDATE: Final = "voucher_batch_recipient_not_candidate"
CUSTOMER_UUID_MISSING: Final = "voucher_batch_customer_uuid_missing"
CUSTOMER_IDENTITY_NOT_CURRENT: Final = "voucher_batch_customer_identity_not_current"
CUSTOMER_PHONE_NOT_CURRENT: Final = "voucher_batch_customer_phone_not_current"
CUSTOMER_NAME_MISSING: Final = "voucher_batch_customer_name_missing"
LOCAL_CLIENT_UNPROVEN: Final = "voucher_batch_local_client_unproven"
CUSTOMER_AMBIGUOUS: Final = "voucher_batch_customer_ambiguous"
CUSTOMER_LOOKUP_UNDETERMINED: Final = "voucher_batch_customer_lookup_undetermined"
RECIPIENT_OPTED_OUT: Final = "voucher_batch_recipient_opted_out"
LIVE_GUARD_UNCERTAIN: Final = "voucher_batch_live_guard_uncertain"
# Between the freeze and this stage the snapshot changed under us.
COMPOSITION_DRIFTED: Final = "voucher_batch_composition_drifted"

# -- the voucher itself ------------------------------------------------------
ORDER_UNPROVEN: Final = "voucher_batch_order_unproven"
ARTIFACT_UNPROVEN: Final = "voucher_batch_artifact_unproven"
BINDING_MISMATCH: Final = "voucher_batch_binding_mismatch"
ORDER_NOT_PAYABLE: Final = "voucher_batch_order_not_payable"
ORDER_NOT_PAID: Final = "voucher_batch_order_not_paid"
ORDER_ALREADY_REFUNDED: Final = "voucher_batch_order_already_refunded"

# -- the baseline ------------------------------------------------------------
BASELINE_DRIFT: Final = "voucher_batch_baseline_drift"

# -- reconciliation of an unknown create -------------------------------------
MARKER_SEARCH_UNRESOLVED: Final = "voucher_batch_marker_search_unresolved"
MARKER_SEARCH_AMBIGUOUS: Final = "voucher_batch_marker_search_ambiguous"
MARKER_SEARCH_INCOMPLETE: Final = "voucher_batch_marker_search_incomplete"
RECONCILE_UNRESOLVED: Final = "voucher_batch_reconcile_unresolved"

# -- the send ----------------------------------------------------------------
DELIVERY_ALREADY_ATTEMPTED: Final = "voucher_batch_delivery_already_attempted"
MANUAL_CLEANUP_REQUIRED: Final = "voucher_batch_manual_cleanup_required"
REFUND_FORBIDDEN_AFTER_SEND: Final = "voucher_batch_refund_forbidden_after_send"
# The slot an operator named is not one this batch has.
SLOT_UNKNOWN: Final = "voucher_batch_slot_unknown"

# -- the halt ----------------------------------------------------------------
# The first unknown stops the whole remaining suffix. Its own code, because
# "this slot was never attempted" is a different fact from any refusal about the
# slot itself, and an operator has to be able to tell them apart.
BATCH_HALTED: Final = "voucher_batch_halted"
HALTED_BY_PREDECESSOR: Final = "voucher_batch_halted_by_predecessor"

# -- operator authorisation --------------------------------------------------
LEDGER_STATE_UNEXPECTED: Final = "voucher_batch_ledger_state_unexpected"
IDENTITY_BINDING_MISMATCH: Final = "voucher_batch_identity_binding_mismatch"
PLAN_DIGEST_MISMATCH: Final = "voucher_batch_plan_digest_mismatch"
PLAN_EXPIRED: Final = "voucher_batch_plan_expired"
CONFIRMATION_MISMATCH: Final = "voucher_batch_confirmation_mismatch"
UNKNOWN_STAGE: Final = "voucher_batch_unknown_stage"
APPLY_FLAG_MISSING: Final = "voucher_batch_apply_flag_missing"
API_UNAVAILABLE: Final = "voucher_batch_api_unavailable"
MUTATION_UNKNOWN: Final = "voucher_batch_mutation_unknown"
MUTATION_REJECTED: Final = "voucher_batch_mutation_rejected"
DATABASE_UNAVAILABLE: Final = "voucher_batch_database_unavailable"


def batch_marker(*, preview_run_id: int, campaign_recipient_id: int, slot: int) -> str:
    """The non-personal comment marker for one slot's order.

    Deterministic, so a reconciliation after a crash recomputes exactly the
    marker it would have sent and an operator can paste it into the EasyWeek
    dashboard to find an open draft. Derived from THIS scope and THIS slot, so
    it can collide neither with §35's, §36's and §37.2's markers nor with
    another slot of the same batch.

    A digest rather than the ids themselves: the marker lands on a real order in
    a real POS system that other people read.
    """
    material = f"{BATCH_SCOPE}:{preview_run_id}:{campaign_recipient_id}:{slot}"
    return "ewvb1-" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:12]


__all__ = [
    "BATCH_SCHEMA_VERSION",
    "BATCH_SCOPE",
    "BATCH_STAGES",
    "EXTERNAL_STAGES",
    "KARLSRUHE_COMPANY_ID",
    "MAX_EXPOSURE_MINOR",
    "MAX_RECIPIENTS",
    "NEW_CLIENT_CAMPAIGN_CODE",
    "STAGE_CREATE",
    "STAGE_DELIVER",
    "STAGE_FREEZE",
    "STAGE_PAY",
    "STAGE_REFUND",
    "UNIT_PRICE_MINOR",
    "VOUCHER_TEMPLATE_CODE",
    "batch_marker",
]
