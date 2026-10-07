"""What turns an operator's "yes" into permission for exactly one stage (§42).

The same contract §§35–37.2 and §41 earned, applied to a phase that has more
than one batch. A plan is a bounded, PII-free snapshot of everything that must
hold before one stage may run, plus a digest of that snapshot. The digest is the
authorisation token: an operator reads the plan, the owner approves that exact
digest, and the apply command refuses unless the plan it rebuilds — live,
seconds before the first claim — still hashes to the same value.

*Per stage.* One plan cannot cover the batch. The moment ``create`` succeeds, a
plan that required no order to exist can never be satisfied again, so a create
digest is deliberately useless for a payment or a send.

*Per batch.* This is the new one. §41 had a single batch, so "which batch" was
never a question an approval had to answer. Here the batch identity is inside
the signed snapshot, so an approval taken for one batch cannot authorise a
stage of another — even the same stage, even seconds later, even by the same
operator. Two mailings running in the same week is the normal case, and pasting
one's digest into the other's command must fail.

*The moment is signed.* ``issued_at`` is canonical material inside the digest,
not a label printed beside it. An approval is a statement about a workspace AT A
MOMENT; a digest that did not cover the moment could be replayed forever by
pairing it with a freshly typed timestamp, and the age check alone cannot see
that because it only ever reads the timestamp it was handed.

*It goes stale.* The digest already catches drift, but an approval from an hour
ago has had an hour in which the world could change and change back. The TTL is
not widened to accommodate a slow stage: a plan that expired while an operator
was reading it is a plan they should build again.

What a production batch adds
----------------------------
The composition is inside the snapshot, so the digest covers *who* as well as
*what* — every slot, every row id, every customer and the money they add up to.
For a freeze it also covers the two numbers the operator stated themselves, so
an approval for four people cannot be presented as one for forty. An operator
who edits the preview between reading a plan and approving it changes the
digest, and the stage refuses before anything leaves the process.

Separate from every earlier phase, deliberately
-----------------------------------------------
The scope string is inside the digest and the phase name is inside the
confirmation phrase, so a §36, §37.2 or §41 approval cannot authorise a §42
stage even if an operator pastes it — and the phrase an operator types says out
loud which phase they are authorising.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Final

from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    APPROVAL_ARITHMETIC,
    CONFIRMATION_MISMATCH,
    PLAN_DIGEST_MISMATCH,
    PLAN_EXPIRED,
    PRODUCTION_SCHEMA_VERSION,
    PRODUCTION_SCOPE,
)
from altegio_bot.utils import utcnow

# Short on purpose. Long enough for a human to read a plan and decide, short
# enough that the world cannot have changed materially in between.
#
# Deliberately NOT scaled with the batch size. A bigger list is a reason to read
# the plan more carefully, not a reason to let an approval sit around longer,
# and a TTL that grew with N would be longest exactly when the exposure was
# largest. An operator who needs more time builds a fresh plan.
PLAN_MAX_AGE: Final = timedelta(minutes=30)


def _digest_over(material: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(material, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def phrase_for_digest(stage: str, digest: str) -> str:
    """The exact phrase that authorises *stage* at *digest*. One definition.

    Named the phase, not only the stage: an operator reading it should be able to
    see which phase they are authorising, and a §41 phrase must not look like a
    §42 one.

    Worth being precise about what this adds, because the browser path (§43.4)
    derives it rather than asking a human to type it. The phrase is a pure
    function of the digest, so it carries no strength the digest does not already
    have — anyone holding the digest can compute it. What it bought in §42 was
    CEREMONY: a human typing the phase name out loud, in a terminal, as the last
    act before money moved.

    In the browser that ceremony is the confirmation screen and the stored
    approval: a named operator agreeing to this stage, this count and this amount,
    recorded in a row with its own expiry. So the UI path computes the phrase from
    the digest its approval stored, and the digest comparison — which is the real
    check — is unchanged and unweakened.
    """
    return f"{stage}-voucher-production-{digest[:12]}"


def stage_digest(
    *,
    stage: str,
    snapshot: dict[str, Any],
    ledger_state: dict[str, Any],
    issued_at: datetime,
) -> str:
    """The authorisation digest of one stage of one batch at one exact moment.

    Microsecond resolution is deliberate: two plans built in the same second are
    two different approvals.
    """
    return _digest_over(
        {
            "batch_scope": PRODUCTION_SCOPE,
            "schema_version": "3" if snapshot.get("product_contract") is not None else PRODUCTION_SCHEMA_VERSION,
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
        return phrase_for_digest(self.stage, digest)

    @property
    def confirmation_phrase(self) -> str:
        """The exact phrase an operator must type for THIS stage of THIS plan."""
        return self.phrase_for(self.digest)

    @property
    def authorised_slots(self) -> tuple[int, ...]:
        """Exactly the slots this approval covers, read back out of the digest.

        The signed material, not a re-derivation. ``target_slots`` went into
        :func:`stage_digest` when the plan was built, so an operator who
        approved this digest approved these slots and no others — and a slot
        that became actionable afterwards is simply not in here.

        This is the list every acting stage walks. Reading it off the plan
        rather than re-querying the ledger is what stops "what may be acted on"
        from growing between the approval and the act: a CREATE that lands in
        that window makes its slot eligible for the NEXT plan, not for this
        one's payment.
        """
        raw = self.snapshot.get("target_slots") or []
        return tuple(sorted(int(value) for value in raw))

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "voucher_production_stage_plan",
            "batch_scope": PRODUCTION_SCOPE,
            "schema_version": "3" if self.snapshot.get("product_contract") is not None else PRODUCTION_SCHEMA_VERSION,
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
            # Repeated on every plan, ready or not. A green stage of one
            # mailing is never a campaign permission, however large the mailing.
            "approval_arithmetic": (
                "approved_exposure_minor = expected_recipient_count * 1000"
                if self.snapshot.get("product_contract") is not None
                else APPROVAL_ARITHMETIC
            ),
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
    "phrase_for_digest",
    "stage_digest",
    "verify_plan_authorisation",
]
