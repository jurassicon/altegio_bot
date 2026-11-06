"""When a confirmed send failure may put somebody in a follow-up audience (§46.3).

The question this module refuses to answer by guessing
-----------------------------------------------------
PR-22.1 established the FACT: this one original send really failed, Meta refused
it or a signed ``failed`` callback named that exact message on that exact route.
The fact says nothing about whether writing to that person again is allowed, and
the difference matters more here than anywhere else in the phase. The other two
groups write to somebody whose device demonstrably received a message. This one
writes to somebody about whom the only thing known is that a send went wrong —
and some of the reasons a send goes wrong are the provider saying *do not do
that again*.

§46.3 is explicit: one ``transient`` class, or one ``send_rejected`` state, is
not a licence; an unknown reason, a permanent block, a policy restriction and an
unresolved sender or template problem give no eligibility at all; and a waiting
time may not be invented. A proven policy would have to come from the provider's
documented semantics for specific codes together with an owner decision about
how long to wait — neither of which exists in this repository today.

So v1 admits nothing
--------------------
:data:`FAILURE_POLICY_V1` carries no admissible class, no admissible code and no
waiting time. Every confirmed failure is therefore excluded with
``followup_failure_policy_unproven``, and the Ops screen reports that as a
missing prerequisite rather than as an empty coincidence. The operator may still
select the group: the result is simply an honest zero, which §46.6 calls a
correct answer.

What a later version must still obey
------------------------------------
The shape is here so that a future proven policy is a data change under review
rather than new judgement scattered through the audience code. It cannot be used
to smuggle the refused cases back in: :class:`FailurePolicy` validates itself
against a floor that no version may lower.

* only a ``confirmed`` fact counts — never ``unmatched``, ``conflicting`` or
  ``unproven``;
* only the ``transient`` class may ever be admissible, so ``permanent`` and
  ``unknown`` stay out by construction;
* an admissible code must be a specific integer from the transient set, named
  one by one. The catch-all ``0`` is not a specific reason and is refused;
* a waiting time must be stated positively, and it is counted from the proven
  original attempt by the caller, not from a callback's own arrival.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

from altegio_bot.campaigns.easyweek_voucher_followup.identity import FAILURE_POLICY_UNPROVEN
from altegio_bot.models.models import (
    DELIVERY_FAILURE_CLASS_PERMANENT,
    DELIVERY_FAILURE_CLASS_TRANSIENT,
    DELIVERY_FAILURE_CLASS_UNKNOWN,
    DELIVERY_FAILURE_MATCH_CONFIRMED,
)
from altegio_bot.services.meta_error_classifier import DELIVERY_TRANSIENT_ERROR_CODES

__all__ = [
    "ADMISSIBLE_CLASS_FLOOR",
    "ADMISSIBLE_CODE_FLOOR",
    "CURRENT_FAILURE_POLICY",
    "FAILURE_POLICY_V1",
    "FailurePolicy",
    "FailureFact",
]

# The only class a policy version is ever allowed to admit. ``permanent`` is a
# block or a template defect and ``unknown`` is not a reason at all.
ADMISSIBLE_CLASS_FLOOR: Final[frozenset[str]] = frozenset({DELIVERY_FAILURE_CLASS_TRANSIENT})

# The only codes a policy version may name. The transient set minus ``0``,
# which Meta uses for "something went wrong" and which therefore identifies
# nothing a policy could reason about.
ADMISSIBLE_CODE_FLOOR: Final[frozenset[int]] = frozenset(DELIVERY_TRANSIENT_ERROR_CODES) - {0}


@dataclass(frozen=True)
class FailureFact:
    """The parts of one stored failure a policy is allowed to look at.

    A plain value object on purpose. The policy never sees the ORM row, so it
    cannot reach for the free-text Meta payload, the recipient or anything else
    that is neither proven nor safe; and a test can state a case in one line.
    """

    match_state: str
    failure_class: str
    meta_error_code: int | None
    reason_code: str


@dataclass(frozen=True)
class FailurePolicy:
    """A versioned answer to "may this confirmed failure be followed up?"."""

    version: str
    admissible_classes: frozenset[str] = field(default_factory=frozenset)
    admissible_meta_error_codes: frozenset[int] = field(default_factory=frozenset)
    min_wait_after_attempt_seconds: int | None = None

    def __post_init__(self) -> None:
        # The floor, checked at import time so a lowered policy cannot ship.
        bad_classes = self.admissible_classes - ADMISSIBLE_CLASS_FLOOR
        if bad_classes:
            raise ValueError(
                f"follow-up failure policy {self.version!r} would admit "
                f"{sorted(bad_classes)}; only {sorted(ADMISSIBLE_CLASS_FLOOR)} can ever be admissible"
            )
        bad_codes = self.admissible_meta_error_codes - ADMISSIBLE_CODE_FLOOR
        if bad_codes:
            raise ValueError(
                f"follow-up failure policy {self.version!r} would admit Meta code(s) "
                f"{sorted(bad_codes)}, which are not specific transient reasons"
            )
        if self.admits_anything and self.min_wait_after_attempt_seconds is None:
            raise ValueError(
                f"follow-up failure policy {self.version!r} admits failures without stating a waiting time"
            )
        if self.min_wait_after_attempt_seconds is not None and self.min_wait_after_attempt_seconds <= 0:
            raise ValueError(f"follow-up failure policy {self.version!r} states a non-positive waiting time")

    @property
    def admits_anything(self) -> bool:
        """``False`` for a policy that is a placeholder rather than a decision."""
        return bool(self.admissible_classes and self.admissible_meta_error_codes)

    def refusal_for(self, fact: FailureFact | None) -> str | None:
        """``None`` when this failure is admissible, else a safe reason code.

        One code is returned for every refused case on purpose. An operator
        does not need to know which clause refused — ``permanent``, ``unknown``,
        an unnamed code, a conflicting fact or, as in v1, the absence of any
        policy at all — and spelling the distinctions out on screen would
        describe the provider's opinion of a real person's number.
        """
        if fact is None:
            return FAILURE_POLICY_UNPROVEN
        if not self.admits_anything:
            return FAILURE_POLICY_UNPROVEN
        if fact.match_state != DELIVERY_FAILURE_MATCH_CONFIRMED:
            return FAILURE_POLICY_UNPROVEN
        if fact.failure_class not in self.admissible_classes:
            return FAILURE_POLICY_UNPROVEN
        if fact.meta_error_code is None or fact.meta_error_code not in self.admissible_meta_error_codes:
            return FAILURE_POLICY_UNPROVEN
        return None

    def required_interval_seconds(self, operator_interval_seconds: int) -> int:
        """The larger of the operator's interval and the policy's own waiting time.

        The operator's choice is a floor they set deliberately; a policy's
        waiting time is a floor the provider's semantics impose. Neither may be
        softened by the other, so the binding one is whichever is longer.
        """
        if self.min_wait_after_attempt_seconds is None:
            return operator_interval_seconds
        return max(operator_interval_seconds, self.min_wait_after_attempt_seconds)


# The one shipped version: a decision not to decide. It is stored on every
# snapshot, so a preview computed today stays readable once a later version
# exists and admits something.
FAILURE_POLICY_V1: Final = FailurePolicy(version="followup-failure-policy-v1")

CURRENT_FAILURE_POLICY: Final = FAILURE_POLICY_V1

# Imported for the floor's sake: a reader checking that ``permanent`` and
# ``unknown`` really cannot be admitted should see them named here.
_FLOOR_EXCLUDES: Final = (DELIVERY_FAILURE_CLASS_PERMANENT, DELIVERY_FAILURE_CLASS_UNKNOWN)
assert not (ADMISSIBLE_CLASS_FLOOR & frozenset(_FLOOR_EXCLUDES))
