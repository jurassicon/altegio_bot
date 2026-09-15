"""The versioned 42/42 baseline this canary compares the template against.

§35 froze the voucher template at 43/43 services and proved a production canary
against it. That record is history: it is not rewritten, not reinterpreted, and
its ledger, digests and confirmed facts stay exactly as they are.

A later read-only probe, taken twice for this phase, observed 42/42 with both
all-services and all-branches flags true, cost and value at 1500, and every
other frozen field unchanged. So this canary carries its own baseline, named
:data:`MANUAL_BASELINE_VERSION`, and compares against that.

The one rule that matters
-------------------------
It does not adapt. A template reading 41 is not "close enough" and is not
quietly accepted as the new normal: it is a product somebody edited while a €15
payment was pending, and the only safe response is to stop and let a human
decide. Moving the baseline is a code change a reviewer sees, never something
this module infers from a response.

Two things are checked that a field-by-field comparison alone would miss:

* ``services_count`` and ``all_services_count`` must be EQUAL. Each matching its
  own literal already implies it, but stating the relationship separately is
  what catches a future baseline edited on one line and not the other.
* the counters must be exact non-negative integers with
  ``activated_vouchers_count <= vouchers_count``. A counter that cannot be read
  is not a zero.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from altegio_bot.campaigns.easyweek_manual_voucher.identity import (
    MANUAL_BASELINE_TEMPLATE_FACTS,
    MANUAL_BASELINE_VERSION,
)
from altegio_bot.easyweek_voucher_canary.plan import (
    frozen_template_mismatches,
    immutable_template_digest,
    template_counters,
)

# Stated relationships, checked on top of the field-by-field comparison. The
# names are what a report may print; the observed values stay in EasyWeek's UI.
ALL_SERVICES_COUNT_MISMATCH = "services_count_vs_all_services_count"
ALL_SERVICES_FLAG = "is_connected_all_services"
ALL_BRANCHES_FLAG = "is_connected_all_branches"


@dataclass(frozen=True)
class BaselineProof:
    """What one exact template GET said, compared with the approved baseline."""

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


def prove_baseline(template_payload: object) -> BaselineProof:
    """Compare one template response with the approved 42/42 baseline.

    Returns names and booleans. Nothing here decides what to do about a drift:
    that is the caller's refusal to make, and the operator's decision to take.
    """
    mismatched = list(frozen_template_mismatches(template_payload, facts=MANUAL_BASELINE_TEMPLATE_FACTS))

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
    digest = immutable_template_digest(template_payload, facts=MANUAL_BASELINE_TEMPLATE_FACTS)

    return BaselineProof(
        proven=not mismatched and counters is not None,
        baseline_version=MANUAL_BASELINE_VERSION,
        mismatched_fields=tuple(mismatched),
        counters=counters,
        digest=digest,
    )


__all__ = ["BaselineProof", "prove_baseline"]
