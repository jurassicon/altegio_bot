"""The pinned identity, the money contract and the reason vocabulary of §42.

§41 proved the whole irreversible sequence — issue, pay, deliver, observe — for
a bounded handful of manually selected people, in production, on 28.09.2026.
This phase is the working mode: a real operator-curated list, of whatever size
that list honestly is.

What is pinned, and what is not
-------------------------------
Pinned here as literals, because they are the topology the owner approved: the
branch and campaign. The exported €15/schema-2 primitives below retain the
historical contract; §45 selects its fixed €10/schema-3 product explicitly through
``easyweek_voucher_production_contract``.

**Not** pinned, deliberately: how many people. §41's ceiling of five was right
for a controlled experiment, and carrying it into production would be wrong.
Replacing it with an invented number — ten, fifty, a hundred — would be no
better; it would be this module deciding how many real customers a real
campaign may have.

So the size is the operator's to state and the schema's to verify. What
:data:`APPROVAL_ARITHMETIC` describes is the only thing this phase insists on:
the operator must say the count and the money BEFORE the freeze, and both must
describe the full active snapshot exactly. A batch may have fifty recipients;
it may not have a size nobody stated.

What arrives per run instead of being pinned — which preview, which recipients,
which staffer, which payment account — is proven live, every stage, from
scratch.
"""

from __future__ import annotations

import hashlib
import json
from typing import Final

from altegio_bot.easyweek_voucher_production_contract import (
    BOUND_SCHEMA_VERSIONS,
    is_current_fixed_contract,
    production_contract,
)
from altegio_bot.models.models import (
    VOUCHER_PRODUCTION_CAMPAIGN_CODE,
    VOUCHER_PRODUCTION_COMPANY_ID,
    VOUCHER_PRODUCTION_SCHEMA_VERSION,
    VOUCHER_PRODUCTION_SCOPE,
    VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
)

# Re-exported from the model layer rather than re-declared, so that the literal
# a CHECK constraint enforces and the literal this package compares against can
# never drift apart.
PRODUCTION_SCOPE: Final = VOUCHER_PRODUCTION_SCOPE
PRODUCTION_SCHEMA_VERSION: Final = VOUCHER_PRODUCTION_SCHEMA_VERSION
UNIT_PRICE_MINOR: Final = VOUCHER_PRODUCTION_UNIT_PRICE_MINOR
KARLSRUHE_COMPANY_ID: Final = VOUCHER_PRODUCTION_COMPANY_ID
NEW_CLIENT_CAMPAIGN_CODE: Final = VOUCHER_PRODUCTION_CAMPAIGN_CODE

# Printed on every plan and every report, in place of the ceiling §41 had. It
# says what this phase actually guarantees about money, which is a relationship
# rather than a maximum.
APPROVAL_ARITHMETIC: Final = "approved_exposure_minor = expected_recipient_count * 1500"

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
PRODUCTION_STAGES: Final = (STAGE_FREEZE, STAGE_CREATE, STAGE_PAY, STAGE_DELIVER, STAGE_REFUND)

# The stages that may reach the outside world, in the order an operator walks
# them. Deliberately a tuple a reader can see rather than a sequence some
# command executes: there is no function that runs two of these.
EXTERNAL_STAGES: Final = (STAGE_CREATE, STAGE_PAY, STAGE_DELIVER)

# ---------------------------------------------------------------------------
# The closed reason vocabulary
# ---------------------------------------------------------------------------
# Stable, PII-free strings. A wrapper acts on these; prose is for humans only.
# Prefixed `voucher_production_` so a report can never be mistaken for a §36,
# §37.2 or §41 one, and so an operator reading a refusal knows which phase
# refused.

# -- fences ------------------------------------------------------------------
PRODUCTION_DISABLED: Final = "voucher_production_disabled"
STAFFER_UNCONFIGURED: Final = "voucher_production_staffer_unconfigured"
ACCOUNT_UNCONFIGURED: Final = "voucher_production_account_unconfigured"
HMAC_KEY_MISSING: Final = "voucher_production_hmac_key_missing"
HMAC_KEY_INVALID: Final = "voucher_production_hmac_key_invalid"
RUNTIME_IDENTITY_UNUSABLE: Final = "voucher_production_runtime_identity_unusable"

# -- the message it would be delivered with ----------------------------------
TEMPLATE_UNPROVEN: Final = "voucher_production_template_unproven"
SENDER_UNPROVEN: Final = "voucher_production_sender_unproven"
BOOKING_LINK_UNPROVEN: Final = "voucher_production_booking_link_unproven"
TEMPLATE_PARAMETERS_UNPROVEN: Final = "voucher_production_template_parameters_unproven"

# -- the preview this batch would be frozen from -----------------------------
RUN_UNPROVEN: Final = "voucher_production_run_unproven"
# The preview holds no active manually selected candidate at all. An empty batch
# is not a small batch: there is nothing to approve.
COMPOSITION_EMPTY: Final = "voucher_production_composition_empty"
# Retained stable refusal for a snapshot containing unsupported owner-test or
# unknown bases, or any earned row in a historical v1 manual-only batch.
COMPOSITION_MIXED_BASIS: Final = "voucher_production_composition_mixed_basis"
# Two active rows resolve to one EasyWeek customer.
COMPOSITION_DUPLICATE_CUSTOMER: Final = "voucher_production_composition_duplicate_customer"
# This preview, or one of its recipients, already belongs to a historical
# voucher canary or to the §41 batch. Old evidence is not a fresh snapshot.
PREVIEW_ALREADY_CONSUMED: Final = "voucher_production_preview_already_consumed"
ENTITLEMENT_ALREADY_EXISTS: Final = "voucher_production_entitlement_already_exists"
# This preview already has a production batch. One preview is frozen once.
PREVIEW_ALREADY_FROZEN: Final = "voucher_production_preview_already_frozen"
BATCH_NOT_FROZEN: Final = "voucher_production_batch_not_frozen"
# The operator named a batch id this phase does not have.
BATCH_UNKNOWN: Final = "voucher_production_batch_unknown"
# The named batch exists but is not the one this preview is bound to.
BATCH_PREVIEW_MISMATCH: Final = "voucher_production_batch_preview_mismatch"
# The composition in the database is not the composition the operator approved.
FROZEN_DIGEST_MISMATCH: Final = "voucher_production_frozen_digest_mismatch"
SNAPSHOT_NOT_FROZEN: Final = "voucher_production_snapshot_not_frozen"

# -- the size and the money an operator must state ---------------------------
# §42.5, as refusals. Three different mistakes, three different codes, because
# "you did not say" and "what you said was wrong" are not the same problem and
# an operator has to be able to tell them apart.
APPROVAL_COUNT_MISSING: Final = "voucher_production_approved_count_missing"
APPROVAL_EXPOSURE_MISSING: Final = "voucher_production_approved_exposure_missing"
APPROVAL_COUNT_MISMATCH: Final = "voucher_production_approved_count_mismatch"
APPROVAL_EXPOSURE_MISMATCH: Final = "voucher_production_approved_exposure_mismatch"

# -- who it would be delivered to --------------------------------------------
RECIPIENT_UNPROVEN: Final = "voucher_production_recipient_unproven"
RECIPIENT_BASIS_UNSUPPORTED: Final = "voucher_production_recipient_basis_unsupported"
RECIPIENT_NOT_CANDIDATE: Final = "voucher_production_recipient_not_candidate"
CUSTOMER_UUID_MISSING: Final = "voucher_production_customer_uuid_missing"
CUSTOMER_IDENTITY_NOT_CURRENT: Final = "voucher_production_customer_identity_not_current"
CUSTOMER_PHONE_NOT_CURRENT: Final = "voucher_production_customer_phone_not_current"
CUSTOMER_NAME_MISSING: Final = "voucher_production_customer_name_missing"
LOCAL_CLIENT_UNPROVEN: Final = "voucher_production_local_client_unproven"
CUSTOMER_AMBIGUOUS: Final = "voucher_production_customer_ambiguous"
CUSTOMER_LOOKUP_UNDETERMINED: Final = "voucher_production_customer_lookup_undetermined"
RECIPIENT_OPTED_OUT: Final = "voucher_production_recipient_opted_out"
LIVE_GUARD_UNCERTAIN: Final = "voucher_production_live_guard_uncertain"
# Between the freeze and this stage the snapshot changed under us.
COMPOSITION_DRIFTED: Final = "voucher_production_composition_drifted"

# -- the voucher itself ------------------------------------------------------
ORDER_UNPROVEN: Final = "voucher_production_order_unproven"
ARTIFACT_UNPROVEN: Final = "voucher_production_artifact_unproven"
BINDING_MISMATCH: Final = "voucher_production_binding_mismatch"
ORDER_NOT_PAYABLE: Final = "voucher_production_order_not_payable"
ORDER_NOT_PAID: Final = "voucher_production_order_not_paid"
ORDER_ALREADY_REFUNDED: Final = "voucher_production_order_already_refunded"

# -- the baseline ------------------------------------------------------------
BASELINE_DRIFT: Final = "voucher_production_baseline_drift"

# -- reconciliation of an unknown create -------------------------------------
MARKER_SEARCH_UNRESOLVED: Final = "voucher_production_marker_search_unresolved"
MARKER_SEARCH_AMBIGUOUS: Final = "voucher_production_marker_search_ambiguous"
MARKER_SEARCH_INCOMPLETE: Final = "voucher_production_marker_search_incomplete"
RECONCILE_UNRESOLVED: Final = "voucher_production_reconcile_unresolved"

# -- the send ----------------------------------------------------------------
DELIVERY_ALREADY_ATTEMPTED: Final = "voucher_production_delivery_already_attempted"
MANUAL_CLEANUP_REQUIRED: Final = "voucher_production_manual_cleanup_required"
REFUND_FORBIDDEN_AFTER_SEND: Final = "voucher_production_refund_forbidden_after_send"
# The slot an operator named is not one this batch has.
SLOT_UNKNOWN: Final = "voucher_production_slot_unknown"

# -- a command that died part-way through ------------------------------------
# Not an unknown and not a refusal. Something this invocation did IS on the
# record — a voucher created, €15 charged, a message Meta accepted — and then
# the command stopped before finishing the rest of its slots or printing its
# report. What happened is known; what did not get to happen is the question,
# and it is one for a human with a fresh plan.
EXECUTION_INTERRUPTED: Final = "voucher_production_execution_interrupted"

# -- the pinned issuer (§43.9) ------------------------------------------------
# Every production voucher is sold by ONE approved EasyWeek staffer, whoever the
# client is, whoever served them and whoever is logged into Ops. These are the
# four ways that can fail, kept apart because they need four different answers:
# a wrong configuration is an administrator's job, an unproven membership may be
# EasyWeek being slow, and a browser that tried to name a staffer is an attempt.
ISSUER_NOT_APPROVED: Final = "voucher_production_issuer_not_approved"
ISSUER_MEMBERSHIP_MISSING: Final = "voucher_production_issuer_membership_missing"
ISSUER_MEMBERSHIP_AMBIGUOUS: Final = "voucher_production_issuer_membership_ambiguous"
ISSUER_MEMBERSHIP_INCOMPLETE: Final = "voucher_production_issuer_membership_incomplete"
ISSUER_SUPPLIED_BY_CLIENT: Final = "voucher_production_issuer_supplied_by_client"

# -- the operator's stop (§43.6) ---------------------------------------------
# Not a halt and not an unknown. The operator asked for the stage to stop after
# the request that was already in flight, so the slots behind it were never
# claimed and nothing about them is in doubt.
STOPPED_BY_OPERATOR: Final = "voucher_production_stopped_by_operator"

# -- the composition read's own bound ----------------------------------------
# The live proof of the audience did not finish inside the budget this phase gives
# it (see ``dispatch.composition_read_budget_seconds``). Its own code, because it is
# not a statement about the audience: nothing was established, so the composition is
# UNKNOWN rather than empty or refused, and the next step is to look again.
COMPOSITION_READ_TIMEOUT: Final = "voucher_production_composition_read_timeout"

# -- excluding one recipient from the preview being checked -------------------
# The soft removal refused: the snapshot is no longer editable, a voucher batch is
# already frozen onto it, a canary holds it, the row belongs to another run, or the
# segmenter had already excluded it for its own reason. One code rather than the
# operation's own Russian prose, which was written for a different screen.
RECIPIENT_NOT_EXCLUDABLE: Final = "voucher_production_recipient_not_excludable"

# -- browser authorisation of one stage (§43.4) ------------------------------
APPROVAL_UNKNOWN: Final = "voucher_production_approval_unknown"
APPROVAL_NOT_READY: Final = "voucher_production_approval_not_ready"
APPROVAL_ALREADY_CONSUMED: Final = "voucher_production_approval_already_consumed"
APPROVAL_PRINCIPAL_MISMATCH: Final = "voucher_production_approval_principal_mismatch"
APPROVAL_COUNT_UNCONFIRMED: Final = "voucher_production_approval_count_unconfirmed"
APPROVAL_EXPOSURE_UNCONFIRMED: Final = "voucher_production_approval_exposure_unconfirmed"
OPERATION_UNKNOWN: Final = "voucher_production_operation_unknown"
# The single dedicated executor is not running, so a confirmation would sit in a
# queue nobody drains. Said out loud rather than shown as a stuck spinner.
EXECUTOR_UNAVAILABLE: Final = "voucher_production_executor_unavailable"
# The operator session is missing, unconfigured or not a browser session.
OPS_SESSION_REQUIRED: Final = "voucher_production_ops_session_required"
OPS_CSRF_INVALID: Final = "voucher_production_ops_csrf_invalid"
OPS_ORIGIN_REJECTED: Final = "voucher_production_ops_origin_rejected"

# -- serialising a batch's own activity (review R1, R2) ----------------------
# A batch does one thing at a time, and the two ways that was not true were both
# ways to lose money or a message.
#
# A plan built BEFORE a stop cannot authorise carrying on after it: the operator
# who pressed stop had not seen that plan, and a second tab confirming it would
# have lifted their stop. Continuing is a plan built in full knowledge of the
# stop, which is what the generation comparison establishes.
STOP_ACTIVE: Final = "voucher_production_stop_active"
# §45.4, and ONLY for the fixed €10 contract (request schema ``3``). An explicit
# operator stop ends that batch's CREATE/PAY/DELIVER for good: the owner decided
# the vouchers of a mailing somebody deliberately stopped are not wanted later, so
# a stop there is a terminal cancellation of further execution rather than the
# pause schemas ``1`` and ``2`` keep.
#
# That makes it a different answer from :data:`STOP_ACTIVE`, which says "this plan
# predates the stop, build a fresh one". Nothing lifts this: not a fresh plan, not
# a new confirmation, not an operation that was already queued, not a restart, not
# a reconcile and not a refund. It stops execution only — reading status, delivery
# webhooks, reconciliation and a separately confirmed pre-send refund all stay
# available, and it never claims an issued voucher was annulled or money returned.
STOP_TERMINAL: Final = "voucher_production_stop_terminal"
# Another operation for this batch is queued or running. Admitting a second one
# would let it clear the first one's stop and resume the slots behind a request
# that is still in flight.
OPERATION_IN_FLIGHT: Final = "voucher_production_operation_in_flight"
# A readback was asked for while a stage of the same batch is executing.
# Reconciling then would reinterpret a LIVE claim as abandoned, and the success
# that is about to come back would have nowhere to land.
RECONCILE_BUSY: Final = "voucher_production_reconcile_busy"
# An external effect happened and its durable record did not land. Never
# reported as success: the effect is real and the ledger does not say so.
LEDGER_WRITE_LOST: Final = "voucher_production_ledger_write_lost"

# -- the closed CLI mutation surface (§43.6) ---------------------------------
# FREEZE/CREATE/PAY/DELIVER/REFUND are UI actions now. The command still exists,
# still reads and still diagnoses; what it no longer does is act.
CLI_MUTATION_CLOSED: Final = "voucher_production_cli_mutation_closed"

# -- the halt ----------------------------------------------------------------
# The first unknown stops the whole remaining suffix OF THIS BATCH. Its own
# code, because "this slot was never attempted" is a different fact from any
# refusal about the slot itself, and an operator has to tell them apart.
BATCH_HALTED: Final = "voucher_production_batch_halted"
HALTED_BY_PREDECESSOR: Final = "voucher_production_halted_by_predecessor"

# -- operator authorisation --------------------------------------------------
LEDGER_STATE_UNEXPECTED: Final = "voucher_production_ledger_state_unexpected"
IDENTITY_BINDING_MISMATCH: Final = "voucher_production_identity_binding_mismatch"
PLAN_DIGEST_MISMATCH: Final = "voucher_production_plan_digest_mismatch"
PLAN_EXPIRED: Final = "voucher_production_plan_expired"
CONFIRMATION_MISMATCH: Final = "voucher_production_confirmation_mismatch"
UNKNOWN_STAGE: Final = "voucher_production_unknown_stage"
APPLY_FLAG_MISSING: Final = "voucher_production_apply_flag_missing"
API_UNAVAILABLE: Final = "voucher_production_api_unavailable"
MUTATION_UNKNOWN: Final = "voucher_production_mutation_unknown"
MUTATION_REJECTED: Final = "voucher_production_mutation_rejected"
DATABASE_UNAVAILABLE: Final = "voucher_production_database_unavailable"


def production_marker(*, preview_run_id: int, campaign_recipient_id: int, slot: int) -> str:
    """The non-personal comment marker for one slot's order.

    Deterministic, so a reconciliation after a crash recomputes exactly the
    marker it would have sent and an operator can paste it into the EasyWeek
    dashboard to find an open draft. Derived from THIS scope, THIS preview and
    THIS slot, so it can collide neither with §35's, §36's, §37.2's and §41's
    markers nor with another slot of the same batch nor with the same slot
    number of a different batch.

    Keyed on the preview rather than on the batch id deliberately: the marker
    has to be recomputable by a reconcile, and the preview is part of the
    identity from before the batch row exists.

    A digest rather than the ids themselves: the marker lands on a real order in
    a real POS system that other people read.
    """
    material = f"{PRODUCTION_SCOPE}:{preview_run_id}:{campaign_recipient_id}:{slot}"
    return "ewvp1-" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:12]


def binding_material(
    *,
    batch_id: int,
    slot: int,
    schema_version: str = "1",
    frozen_digest: str | None = None,
    recipient_basis: str | None = None,
    manual_policy: str | None = None,
    source_proof_digest: str | None = None,
    customer_uuid: str | None = None,
    product_contract_version: str | None = None,
    message_contract_code: str | None = None,
) -> str:
    """What a slot's voucher MAC is bound to, besides the order and the product.

    Both halves matter and neither is enough alone. The slot alone repeats
    across batches — slot 1 exists in every one of them — so a MAC bound to the
    slot only would verify a code from a different batch. The batch alone
    ignores which of its recipients the code belongs to. Together they name
    exactly one row, table-wide, for the lifetime of the phase.
    """
    contract = production_contract(schema_version, contract_version=product_contract_version)
    if message_contract_code is not None and message_contract_code != contract.message_code:
        raise ValueError("voucher_production_binding_identity_unproven")
    legacy = f"{PRODUCTION_SCOPE}:{batch_id}:{slot}"
    if schema_version == "1":
        return legacy
    # Every version that binds a slot to its recipient and snapshot, which is all
    # of them except the schema 1 legacy domain returned above. Listed centrally so
    # a new fixed product inherits the bound domain instead of falling off the end
    # of a literal tuple and refusing every voucher it issues.
    if schema_version not in BOUND_SCHEMA_VERSIONS or not frozen_digest or not recipient_basis or not customer_uuid:
        raise ValueError("voucher_production_binding_identity_unproven")
    material = {
        "schema_version": schema_version,
        "frozen_digest": frozen_digest,
        "recipient_basis": recipient_basis,
        "manual_policy": manual_policy,
        "source_proof_digest": source_proof_digest,
        "customer_uuid": customer_uuid,
    }
    if is_current_fixed_contract(schema_version):
        material["product_contract"] = contract.digest_material()
    return legacy + f":v{schema_version}:" + hashlib.sha256(json.dumps(material, sort_keys=True).encode()).hexdigest()


__all__ = [
    "APPROVAL_ARITHMETIC",
    "EXTERNAL_STAGES",
    "KARLSRUHE_COMPANY_ID",
    "NEW_CLIENT_CAMPAIGN_CODE",
    "PRODUCTION_SCHEMA_VERSION",
    "PRODUCTION_SCOPE",
    "PRODUCTION_STAGES",
    "STAGE_CREATE",
    "STAGE_DELIVER",
    "STAGE_FREEZE",
    "STAGE_PAY",
    "STAGE_REFUND",
    "UNIT_PRICE_MINOR",
    "VOUCHER_TEMPLATE_CODE",
    "binding_material",
    "production_marker",
]
