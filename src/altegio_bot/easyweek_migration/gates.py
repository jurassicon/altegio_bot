"""Everything that must be true before the FIRST mutation (PR-11.1).

Migrating a booking into EasyWeek is the kind of write that talks back to the
customer. Two independent notification systems can react to it:

1. **EasyWeek's own.** Email, SMS, push, WhatsApp, automatic confirmations,
   reminders and change notices are configured in the EasyWeek UI, and this code
   cannot read them, let alone turn them off. Migrating 400 future appointments
   with those on means 400 people are told, at once, that they have "a new
   appointment" they made weeks ago.
2. **Ours.** The bot's own EasyWeek lifecycle notifications and review requests,
   which react to the webhooks the migration is about to generate.

Only the second is machine-checkable, so the gate does both halves of what it
can: it **verifies** our flags, and it **requires the operator to attest** to
theirs. The attestation is a separate explicit flag, not a prompt and not an
inference, because "I turned the EasyWeek notifications off" is a claim about a
system this process cannot see, and it should read like one.

What must be OFF:  ``EASYWEEK_NOTIFICATIONS_ENABLED``, ``EASYWEEK_REVIEWS_ENABLED``
What must be ON:   ``EASYWEEK_PROCESSING_ENABLED``, ``EASYWEEK_ENABLED``

The two ON conditions are not an oversight in a "turn everything off" gate.
Capture and processing are how the resulting webhooks are *recorded*: turning
them off would mean the migration's own events are lost forever (EasyWeek never
re-delivers, plan §1.3), taking with them the ``booking-succeeded`` snapshots
PR-11 stores and the evidence any later reconciliation would need.

The visit counter may stay on. It sends nothing; it records a fact.

The gate has since become the place where every other precondition lands too —
the branch identities, the canary proof, and the previous waves' cumulative
context — for the same reason: it is the one chokepoint every forward mutation
passes through, so a check placed here cannot be skipped by taking another route.

A failed gate raises before a single request is built. There is no partial mode,
no ``--force``, and no path where a mutation happens with an unproven gate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Final

from altegio_bot.easyweek_migration.bindings import MUTATION_CART_TWO, MUTATION_SINGLE
from altegio_bot.easyweek_migration.branch_identity import BranchIdentityResult
from altegio_bot.easyweek_migration.canary import CanaryVerdict
from altegio_bot.easyweek_migration.customers import CustomerDirectory
from altegio_bot.easyweek_migration.manifest import MigrationManifest
from altegio_bot.easyweek_migration.previous_wave import PreviousWaveContext
from altegio_bot.settings import settings

# Flags that must be FALSE, with the reason each one is here.
GATE_NOTIFICATIONS_ENABLED: Final = "bot_easyweek_notifications_enabled"
GATE_REVIEWS_ENABLED: Final = "bot_easyweek_reviews_enabled"
# Flags that must be TRUE.
GATE_PROCESSING_DISABLED: Final = "bot_easyweek_processing_disabled"
GATE_CAPTURE_DISABLED: Final = "bot_easyweek_capture_disabled"
# Operator attestations and run inputs.
GATE_NATIVE_NOTIFICATIONS_UNCONFIRMED: Final = "easyweek_native_notifications_unconfirmed"
GATE_APPLY_FLAG_MISSING: Final = "apply_flag_missing"
GATE_CUTOVER_MISSING: Final = "cutover_at_missing"
GATE_DRY_RUN_ID_MISSING: Final = "verified_dry_run_id_missing"
GATE_DRY_RUN_ID_MISMATCH: Final = "verified_dry_run_id_mismatch"
GATE_MANIFEST_INVALID: Final = "manifest_invalid"
GATE_CUSTOMER_DIRECTORY_INVALID: Final = "customer_directory_invalid"
GATE_CANARY_NOTIFICATION_OBSERVED: Final = "canary_notification_observed"
# Independent proof that each manifest target really is the branch it claims.
GATE_BRANCH_IDENTITY_UNPROVEN: Final = "target_branch_identity_unproven"
# A bulk apply needs a machine-checked canary, not an operator's recollection.
GATE_CANARY_PROOF_MISSING: Final = "canary_proof_missing_or_stale"
# The same refusal for the cart contract, under its own name. One proof licenses
# ONE contract, so a plan containing cart bookings needs a cart canary that a
# single-booking canary can never stand in for — and an operator reading
# `canary_proof_missing_or_stale` next to a verified single canary would
# reasonably conclude the tool was wrong. The code says which proof is missing.
GATE_CART_CANARY_PROOF_MISSING: Final = "cart_canary_proof_missing_or_stale"
# Refusal codes by contract, so the mapping lives in one place.
CANARY_PROOF_FAILURES: Final[dict[str, str]] = {
    MUTATION_SINGLE: GATE_CANARY_PROOF_MISSING,
    MUTATION_CART_TWO: GATE_CART_CANARY_PROOF_MISSING,
}
# The manifest of wave N must still carry the mappings and catalogue baselines
# that waves 1..N-1 were migrated against. See `previous_wave`.
GATE_PREVIOUS_WAVE_CONTEXT_UNPROVEN: Final = "previous_wave_context_unprovable"


class ApplyGateError(RuntimeError):
    """The apply gate refused. Raised before any request is constructed."""

    def __init__(self, failures: list[str]) -> None:
        self.failures = failures
        super().__init__("apply gate refused: " + ", ".join(failures))


@dataclass(frozen=True)
class EffectiveBotSettings:
    """The notification-relevant flags of the process that is actually running.

    Read from ``settings`` at call time — which is why the runbook has the
    operator invoke this tool **inside the EasyWeek worker's own container**,
    with the worker's own ``env_file``. That is the only way "the effective
    settings of the running worker" is a statement about the worker rather than
    about somebody's laptop.
    """

    easyweek_enabled: bool
    easyweek_processing_enabled: bool
    easyweek_notifications_enabled: bool
    easyweek_reviews_enabled: bool
    easyweek_review_send_enabled: bool
    easyweek_reminders_enabled: bool
    easyweek_visit_counter_enabled: bool

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "EASYWEEK_ENABLED": self.easyweek_enabled,
            "EASYWEEK_PROCESSING_ENABLED": self.easyweek_processing_enabled,
            "EASYWEEK_NOTIFICATIONS_ENABLED": self.easyweek_notifications_enabled,
            "EASYWEEK_REVIEWS_ENABLED": self.easyweek_reviews_enabled,
            "EASYWEEK_REVIEW_SEND_ENABLED": self.easyweek_review_send_enabled,
            "EASYWEEK_REMINDERS_ENABLED": self.easyweek_reminders_enabled,
            "EASYWEEK_VISIT_COUNTER_ENABLED": self.easyweek_visit_counter_enabled,
        }


def read_effective_settings() -> EffectiveBotSettings:
    """Snapshot the flags this process is running with."""
    return EffectiveBotSettings(
        easyweek_enabled=bool(getattr(settings, "easyweek_enabled", False)),
        easyweek_processing_enabled=bool(getattr(settings, "easyweek_processing_enabled", False)),
        easyweek_notifications_enabled=bool(getattr(settings, "easyweek_notifications_enabled", False)),
        easyweek_reviews_enabled=bool(getattr(settings, "easyweek_reviews_enabled", False)),
        easyweek_review_send_enabled=bool(getattr(settings, "easyweek_review_send_enabled", False)),
        easyweek_reminders_enabled=bool(getattr(settings, "easyweek_reminders_enabled", False)),
        easyweek_visit_counter_enabled=bool(getattr(settings, "easyweek_visit_counter_enabled", False)),
    )


@dataclass(frozen=True)
class ApplyGateResult:
    """A gate decision, recorded into the run's report whether it passed or not."""

    passed: bool
    failures: list[str] = field(default_factory=list)
    effective_settings: dict[str, Any] = field(default_factory=dict)
    branch_identity: dict[str, Any] | None = None
    canary: dict[str, Any] | None = None
    previous_wave_context: dict[str, Any] | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "failures": list(self.failures),
            "effective_settings": dict(self.effective_settings),
            "branch_identity": self.branch_identity,
            "canary": self.canary,
            "previous_wave_context": self.previous_wave_context,
        }


def evaluate_apply_gate(
    *,
    apply_requested: bool,
    native_notifications_confirmed: bool,
    cutover_supplied: bool,
    verified_dry_run_id: str | None,
    computed_plan_digest: str,
    manifest: MigrationManifest,
    directory: CustomerDirectory,
    canary_notification_observed: bool = False,
    effective: EffectiveBotSettings | None = None,
    branch_identity: BranchIdentityResult | None = None,
    canary_verdict: CanaryVerdict | None = None,
    require_canary_proof: bool = True,
    previous_wave_context: PreviousWaveContext | None = None,
    require_previous_wave_context: bool = False,
) -> ApplyGateResult:
    """Collect EVERY reason the apply may not proceed, then decide.

    Deliberately not short-circuiting. An operator who has three things wrong
    should learn all three from one run, not discover them across three failed
    attempts — each of which is another chance to get impatient with a gate.
    """
    settings_snapshot = effective or read_effective_settings()
    failures: list[str] = []

    if not apply_requested:
        failures.append(GATE_APPLY_FLAG_MISSING)
    if not cutover_supplied:
        failures.append(GATE_CUTOVER_MISSING)

    # The verified dry-run. `computed_plan_digest` is derived from the plan this
    # run just built; the operator passes back the digest their reviewed dry-run
    # printed. If the two differ, the source changed since the review — new
    # bookings, cancelled bookings, an edited manifest — and the reviewed plan is
    # no longer the plan about to run.
    if not verified_dry_run_id:
        failures.append(GATE_DRY_RUN_ID_MISSING)
    elif verified_dry_run_id != computed_plan_digest:
        failures.append(GATE_DRY_RUN_ID_MISMATCH)

    if not native_notifications_confirmed:
        failures.append(GATE_NATIVE_NOTIFICATIONS_UNCONFIRMED)

    if not manifest.valid:
        failures.append(GATE_MANIFEST_INVALID)
    if not directory.valid:
        failures.append(GATE_CUSTOMER_DIRECTORY_INVALID)

    # Our own notification surfaces.
    if settings_snapshot.easyweek_notifications_enabled:
        failures.append(GATE_NOTIFICATIONS_ENABLED)
    if settings_snapshot.easyweek_reviews_enabled:
        failures.append(GATE_REVIEWS_ENABLED)

    # Capture and processing must survive the migration — see module docstring.
    if not settings_snapshot.easyweek_processing_enabled:
        failures.append(GATE_PROCESSING_DISABLED)
    if not settings_snapshot.easyweek_enabled:
        failures.append(GATE_CAPTURE_DISABLED)

    # A single unexpected customer message during the canary stops everything
    # that comes after it. The operator states this; nothing here can observe it.
    if canary_notification_observed:
        failures.append(GATE_CANARY_NOTIFICATION_OBSERVED)

    # Every manifest target must be provably the branch it claims to be, checked
    # against the runtime registry rather than against the manifest's own word.
    # `None` means the caller did not run the check at all, which is treated as
    # "not proven" — a check that can be skipped is not a check.
    if branch_identity is None or not branch_identity.proven:
        failures.append(GATE_BRANCH_IDENTITY_UNPROVEN)

    # A bulk apply needs a verified canary that still applies to this manifest,
    # request schema, cutover and branch mapping. The canary run itself is the
    # one apply that legitimately has no proof yet.
    if require_canary_proof and (canary_verdict is None or not canary_verdict.licensed):
        # An unknown contract has no code of its own, and inventing one from the
        # kind string would put unvalidated text into a stable reason code.
        # It falls back to the generic refusal, which is still a refusal.
        kind = canary_verdict.contract_kind if canary_verdict is not None else MUTATION_SINGLE
        failures.append(CANARY_PROOF_FAILURES.get(kind, GATE_CANARY_PROOF_MISSING))

    # Every wave after the first inherits the previous waves' live `created`
    # rows, and its manifest has to keep proving them. Required on the two paths
    # that mutate EasyWeek forward — canary and bulk apply — and not on rollback,
    # which exists to undo a wave and must not be gated on the wave being sound.
    if require_previous_wave_context and (previous_wave_context is None or not previous_wave_context.proven):
        failures.append(GATE_PREVIOUS_WAVE_CONTEXT_UNPROVEN)

    return ApplyGateResult(
        passed=not failures,
        failures=failures,
        effective_settings=settings_snapshot.as_safe_dict(),
        branch_identity=branch_identity.as_safe_dict() if branch_identity is not None else None,
        canary=canary_verdict.as_safe_dict() if canary_verdict is not None else None,
        previous_wave_context=(previous_wave_context.as_safe_dict() if previous_wave_context is not None else None),
    )


def require_apply_gate(result: ApplyGateResult) -> None:
    """Raise unless the gate passed. The single chokepoint before any mutation."""
    if not result.passed:
        raise ApplyGateError(result.failures)
