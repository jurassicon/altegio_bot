"""The versioned 43/43 baseline the §42 production mailing compares against.

Its own record, not a borrowed one
----------------------------------
§41.9 already settled the principle and it applies here unchanged: a baseline is
the configuration a phase was actually proved against, and two phases are
allowed to disagree. The §37.2 manual canary keeps ``2026-09-15-42`` at 42/42
because that is what it really ran on; §41 carries ``2026-09-27-43`` because the
catalogue moved before it ran.

This phase reads the same template and the same owner-approved 43/43 catalogue
as §41, and it still gets its own constant and its own copy of the frozen facts.
The separation is structural, not a coincidence of values: the day these two
phases legitimately disagree, neither should be editing the other's history to
stay green.

The one rule that matters
-------------------------
It does not adapt. A template reading 42 is not "close enough" and is not
quietly accepted as the new normal: it is a product somebody edited while a €15
payment was pending, and the only safe response is to stop and let a human
decide. Moving this baseline is a code change a reviewer sees, never something
this module infers from a response.

Two things are checked that a field-by-field comparison alone would miss:

* ``services_count`` and ``all_services_count`` must be EQUAL. Each matching its
  own literal already implies it, but stating the relationship separately is
  what catches a future baseline edited on one line and not the other.
* the counters must be exact non-negative integers with
  ``activated_vouchers_count <= vouchers_count``. A counter that cannot be read
  is not a zero.

Redemption stays out of scope. Nothing here — or anywhere in §42 — reads, stores
or can prove that a voucher was applied to a booking; that remains an
owner-reported manual observation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

from altegio_bot.easyweek_voucher_canary.plan import (
    frozen_template_mismatches,
    immutable_template_digest,
    template_counters,
)
from altegio_bot.easyweek_voucher_production_contract import (
    LEGACY_PRODUCTION_CONTRACT,
    ProductionVoucherContract,
)

# This phase's own baseline label. The VALUE matches §41's because the catalogue
# has not moved since; the CONSTANT is separate because the phases are.
PRODUCTION_BASELINE_VERSION: Final = "2026-09-27-43"

# Every frozen field of the template, as the owner-approved live configuration
# reads it on 27.09.2026.
PRODUCTION_BASELINE_TEMPLATE_FACTS: Final[dict[str, Any]] = LEGACY_PRODUCTION_CONTRACT.template_facts()

# Stated relationships, checked on top of the field-by-field comparison. The
# names are what a report may print; the observed values stay in EasyWeek's UI.
ALL_SERVICES_COUNT_MISMATCH: Final = "services_count_vs_all_services_count"
ALL_SERVICES_FLAG: Final = "is_connected_all_services"
ALL_BRANCHES_FLAG: Final = "is_connected_all_branches"


@dataclass(frozen=True)
class ProductionBaselineProof:
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


def prove_production_baseline(
    template_payload: object, *, contract: ProductionVoucherContract = LEGACY_PRODUCTION_CONTRACT
) -> ProductionBaselineProof:
    """Compare one template response with the approved 43/43 baseline.

    Returns names and booleans. Nothing here decides what to do about a drift:
    that is the caller's refusal to make, and the operator's decision to take.
    """
    mismatched = list(
        frozen_template_mismatches(
            template_payload, facts=contract.template_facts(), expected_template_uuid=contract.template_uuid
        )
    )

    # The two service counts must agree with each other, not only with their own
    # literals. A baseline edited on one line and not the other would otherwise
    # pass field-by-field and describe a template nobody approved.
    template = template_payload if isinstance(template_payload, dict) else {}
    if contract.request_schema_version == "3":
        # An omitted activation setting is unknown, even when the required
        # value is null. Historical baselines retain their original semantics.
        mismatched.extend(name for name in contract.template_facts() if name not in template and name not in mismatched)
    services = template.get("services_count")
    all_services = template.get("all_services_count")
    if type(services) is not int or type(all_services) is not int or services != all_services:
        if ALL_SERVICES_COUNT_MISMATCH not in mismatched:
            mismatched.append(ALL_SERVICES_COUNT_MISMATCH)

    counters = template_counters(template_payload)
    digest = immutable_template_digest(template_payload, facts=contract.template_facts())

    return ProductionBaselineProof(
        proven=not mismatched and counters is not None,
        baseline_version=contract.baseline_version,
        mismatched_fields=tuple(mismatched),
        counters=counters,
        digest=digest,
    )


__all__ = [
    "ALL_BRANCHES_FLAG",
    "ALL_SERVICES_COUNT_MISMATCH",
    "ALL_SERVICES_FLAG",
    "PRODUCTION_BASELINE_TEMPLATE_FACTS",
    "PRODUCTION_BASELINE_VERSION",
    "ProductionBaselineProof",
    "prove_production_baseline",
]
