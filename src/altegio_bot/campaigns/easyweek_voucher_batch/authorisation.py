"""What turns an operator's "yes" into permission for exactly one stage (§41).

The same contract §35, §36 and §37.2 earned, applied to a batch. A plan is a
bounded, PII-free snapshot of everything that must hold before one stage may
run, plus a digest of that snapshot. The digest is the authorisation token: an
operator reads the plan, the owner approves that exact digest, and the apply
command refuses unless the plan it rebuilds — live, seconds before the first
claim — still hashes to the same value.

*Per stage.* One plan cannot cover the batch. The moment ``create`` succeeds, a
plan that required no order to exist can never be satisfied again, so a create
digest is deliberately useless for a payment or a send.

*The moment is signed.* ``issued_at`` is canonical material inside the digest,
not a label printed beside it. An approval is a statement about a workspace AT A
MOMENT; a digest that did not cover the moment could be replayed forever by
pairing it with a freshly typed timestamp, and the age check alone cannot see
that because it only ever reads the timestamp it was handed.

*It goes stale.* The digest already catches drift, but an approval from an hour
ago has had an hour in which the world could change and change back.

What a batch adds
-----------------
The composition is inside the snapshot, so the digest covers *who* as well as
*what*: five slots, five row ids, five customers and the money they add up to.
An operator who edits the preview between reading a plan and approving it
changes the digest, and the stage refuses before anything leaves the process.

Separate from every earlier phase, deliberately
-----------------------------------------------
The scope string is inside the digest and the phase name is inside the
confirmation phrase, so a §36 or §37.2 approval cannot authorise a §41 stage
even if an operator pastes it — and the phrase an operator types says out loud
which phase they are authorising.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Final

from altegio_bot.campaigns.easyweek_voucher_batch.identity import (
    BATCH_SCHEMA_VERSION,
    BATCH_SCOPE,
    CONFIRMATION_MISMATCH,
    MAX_EXPOSURE_MINOR,
    MAX_RECIPIENTS,
    PLAN_DIGEST_MISMATCH,
    PLAN_EXPIRED,
)
from altegio_bot.utils import utcnow

# Short on purpose. Long enough for a human to read a plan covering up to five
# people and decide, short enough that the world cannot have changed materially
# in between.
PLAN_MAX_AGE: Final = timedelta(minutes=30)


def _digest_over(material: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(material, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def stage_digest(
    *,
    stage: str,
    snapshot: dict[str, Any],
    ledger_state: dict[str, Any],
    issued_at: datetime,
) -> str:
    """The authorisation digest of one stage at one exact moment.

    Microsecond resolution is deliberate: two plans built in the same second are
    two different approvals.
    """
    return _digest_over(
        {
            "batch_scope": BATCH_SCOPE,
            "schema_version": BATCH_SCHEMA_VERSION,
            "stage": stage,
            "snapshot": snapshot,
            "ledger_state": ledger_state,
            "plan_issued_at": issued_at.isoformat(),
        }
    )


@dataclass(frozen=True)
class StagePlan:
    """A PII-free snapshot plus the digest that authorises ONE stage of it."""

    stage: str
    ready: bool
    reasons: tuple[str, ...]
    digest: str
    issued_at: datetime
    snapshot: dict[str, Any]
    ledger_state: dict[str, Any]
    observations: tuple[dict[str, Any], ...] = field(default=())

    @property
    def expires_at(self) -> datetime:
        return self.issued_at + PLAN_MAX_AGE

    def digest_for(self, issued_at: datetime) -> str:
        """This plan's digest as if it had been issued at *issued_at*.

        The apply command rebuilds the plan itself, so the plan it verifies
        against carries a new ``issued_at``. Recomputing with the operator's
        timestamp is what makes that timestamp part of what was signed.
        """
        return stage_digest(
            stage=self.stage,
            snapshot=self.snapshot,
            ledger_state=self.ledger_state,
            issued_at=issued_at,
        )

    def phrase_for(self, digest: str) -> str:
        # Names the phase, not only the stage: an operator typing this should be
        # able to see from the phrase alone which one they are authorising.
        return f"{self.stage}-voucher-batch-{digest[:12]}"

    @property
    def confirmation_phrase(self) -> str:
        """The exact phrase an operator must type for THIS stage of THIS plan."""
        return self.phrase_for(self.digest)

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "voucher_batch_stage_plan",
            "batch_scope": BATCH_SCOPE,
            "schema_version": BATCH_SCHEMA_VERSION,
            "stage": self.stage,
            "ready": self.ready,
            "reasons": list(self.reasons),
            "plan_digest": self.digest,
            "plan_issued_at": self.issued_at.isoformat(),
            "plan_expires_at": self.expires_at.isoformat(),
            "confirmation_phrase": self.confirmation_phrase,
            "snapshot": dict(self.snapshot),
            "ledger_state": dict(self.ledger_state),
            "observations": [dict(entry) for entry in self.observations],
            # Repeated on every plan, ready or not. A green stage of a five-
            # recipient batch is never a campaign permission.
            "max_recipients": MAX_RECIPIENTS,
            "max_exposure_minor": MAX_EXPOSURE_MINOR,
            "campaign_send_authorized": False,
            "bulk_delivery_authorized": False,
            "global_ready_for_send": False,
        }


def verify_plan_authorisation(
    plan: StagePlan,
    *,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    now: datetime | None = None,
) -> tuple[str, ...]:
    """Reasons this freshly rebuilt plan does NOT authorise its stage.

    An empty tuple means the operator's digest, the operator's phrase and the
    live world all still agree, and the approval is not stale. Anything else
    stops the command before a claim exists.
    """
    moment = now or utcnow()
    reasons: list[str] = list(plan.reasons)

    if supplied_issued_at is None:
        # With no timestamp there is nothing to recompute against, so neither
        # the digest nor the phrase can be checked at all.
        return tuple(dict.fromkeys([*reasons, PLAN_EXPIRED, PLAN_DIGEST_MISMATCH]))

    expected = plan.digest_for(supplied_issued_at)
    if supplied_digest != expected:
        reasons.append(PLAN_DIGEST_MISMATCH)
    if supplied_phrase != plan.phrase_for(expected):
        reasons.append(CONFIRMATION_MISMATCH)
    if moment - supplied_issued_at > PLAN_MAX_AGE or supplied_issued_at > moment:
        reasons.append(PLAN_EXPIRED)
    return tuple(dict.fromkeys(reasons))


__all__ = [
    "PLAN_MAX_AGE",
    "StagePlan",
    "stage_digest",
    "verify_plan_authorisation",
]
