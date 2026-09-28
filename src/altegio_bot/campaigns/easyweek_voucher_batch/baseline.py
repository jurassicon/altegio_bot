"""The versioned 43/43 baseline the §41 batch (PR-18) compares the template against.

Why this module exists at all
-----------------------------
The batch used to reuse the §37.2 manual canary's baseline. That was defensible
while both phases read the same template and saw the same numbers, and it stopped
being defensible the moment they disagreed: a read-only production probe on
27.09.2026 returned ``services_count=43`` and ``all_services_count=43`` with every
other frozen field unchanged, because one brow/lash lamination service had been
switched back on. The manual canary's baseline is HISTORY — it is the configuration
that canary actually ran and proved against, and it is not rewritten, not
reinterpreted and not moved to make a later phase pass.

So the batch carries its own baseline, named :data:`BATCH_BASELINE_VERSION`, and
compares against that. Two phases, two versioned records, neither editing the
other's. The owner accepted the live 43/43 catalogue; which exact service returned
is deliberately not established, because a voucher of €15 against the whole
catalogue does not depend on the answer.

The one rule that matters
-------------------------
It does not adapt. A template reading 42 is not "close enough" and is not quietly
accepted as the new normal: it is a product somebody edited while a €15 payment was
pending, and the only safe response is to stop and let a human decide. Moving this
baseline is a code change a reviewer sees, never something this module infers from a
response.

Two things are checked that a field-by-field comparison alone would miss:

* ``services_count`` and ``all_services_count`` must be EQUAL. Each matching its
  own literal already implies it, but stating the relationship separately is what
  catches a future baseline edited on one line and not the other.
* the counters must be exact non-negative integers with
  ``activated_vouchers_count <= vouchers_count``. A counter that cannot be read is
  not a zero.

Redemption stays out of scope. Nothing here — or anywhere in §41 — reads, stores or
can prove that a voucher was applied to a booking; that remains an owner-reported
manual observation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

from altegio_bot.campaigns.easyweek_voucher_batch.identity import UNIT_PRICE_MINOR
from altegio_bot.easyweek_voucher_canary.plan import (
    frozen_template_mismatches,
    immutable_template_digest,
    template_counters,
)

# The batch's own baseline. Deliberately NOT imported from the manual canary: the
# two phases are allowed to disagree, and the day they did is why this file exists.
BATCH_BASELINE_VERSION: Final = "2026-09-27-43"

# Every frozen field of the template, as the owner-approved live configuration
# reads it on 27.09.2026. The service counters are the only values that differ
# from the historical manual baseline.
BATCH_BASELINE_TEMPLATE_FACTS: Final[dict[str, Any]] = {
    "is_enabled": True,
    "is_online": False,
    "is_single_charge": True,
    "cost": UNIT_PRICE_MINOR,
    "value": UNIT_PRICE_MINOR,
    "validity": None,
    "forces_activation": True,
    "activate_after": 0,
    "activate_at": None,
    "is_connected_all_branches": True,
    "branches_count": 3,
    "all_branches_count": 3,
    "is_connected_all_services": True,
    "services_count": 43,
    "all_services_count": 43,
    "goods_count": 0,
}

# Stated relationships, checked on top of the field-by-field comparison. The names
# are what a report may print; the observed values stay in EasyWeek's UI.
ALL_SERVICES_COUNT_MISMATCH: Final = "services_count_vs_all_services_count"
ALL_SERVICES_FLAG: Final = "is_connected_all_services"
ALL_BRANCHES_FLAG: Final = "is_connected_all_branches"


@dataclass(frozen=True)
class BatchBaselineProof:
    """What one exact template GET said, compared with the approved batch baseline."""

    proven: bool
    baseline_version: str
    # Field NAMES only. A value belongs in the EasyWeek UI, not in a ticket.
    mismatched_fields: tuple[str, ...] = ()
    counters: dict[str, int] | None = None
    digest: str | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "baseline_version": self.baseline_version,
            "baseline_proven": self.proven,
            "mismatched_fields": list(self.mismatched_fields),
            "counters_readable": self.counters is not None,
            "counters": dict(self.counters or {}),
            "template_config_digest": self.digest,
        }


def prove_batch_baseline(template_payload: object) -> BatchBaselineProof:
    """Compare one template response with the approved 43/43 batch baseline.

    Returns names and booleans. Nothing here decides what to do about a drift: that
    is the caller's refusal to make, and the operator's decision to take.
    """
    mismatched = list(frozen_template_mismatches(template_payload, facts=BATCH_BASELINE_TEMPLATE_FACTS))

    # The two service counts must agree with each other, not only with their own
    # literals. A baseline edited on one line and not the other would otherwise
    # pass field-by-field and describe a template nobody approved.
    template = template_payload if isinstance(template_payload, dict) else {}
    services = template.get("services_count")
    all_services = template.get("all_services_count")
    if type(services) is not int or type(all_services) is not int or services != all_services:
        if ALL_SERVICES_COUNT_MISMATCH not in mismatched:
            mismatched.append(ALL_SERVICES_COUNT_MISMATCH)

    counters = template_counters(template_payload)
    digest = immutable_template_digest(template_payload, facts=BATCH_BASELINE_TEMPLATE_FACTS)

    return BatchBaselineProof(
        proven=not mismatched and counters is not None,
        baseline_version=BATCH_BASELINE_VERSION,
        mismatched_fields=tuple(mismatched),
        counters=counters,
        digest=digest,
    )


__all__ = [
    "ALL_BRANCHES_FLAG",
    "ALL_SERVICES_COUNT_MISMATCH",
    "ALL_SERVICES_FLAG",
    "BATCH_BASELINE_TEMPLATE_FACTS",
    "BATCH_BASELINE_VERSION",
    "BatchBaselineProof",
    "prove_batch_baseline",
]
