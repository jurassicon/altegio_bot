"""The confirmed EasyWeek voucher identities, and nothing else.

A tiny, dependency-free module so that both the transport
(:mod:`altegio_bot.easyweek_voucher_calculation`) and the domain evaluator
(:mod:`altegio_bot.campaigns.easyweek_voucher_contract`) can pin themselves to
the same literals without either importing the other, and without a transport
module reaching into the ``campaigns`` package.

This is deliberately NOT a voucher configuration layer: there is no loader, no
override, no environment variable and no registry. Every name here is a literal
that a production read proved once, and changing one is a code change that a
reviewer sees.

The numeric dashboard identity is not an API identity and is absent by design.
"""

from __future__ import annotations

from typing import Final

# Workspace identity, re-proved on every operator run before any POST.
EASYWEEK_WORKSPACE_UUID: Final = "e66be240-362c-4fe4-9388-6ed187b27b93"
EASYWEEK_WORKSPACE_SLUG: Final = "kitilash"
EASYWEEK_WORKSPACE_CURRENCY: Final = "EUR"

# The one branch this evidence path may ever name.
KARLSRUHE_LOCATION_UUID: Final = "8395fab6-7ee8-4702-88d9-fd78f92539c1"

# The one voucher template API identity.
EASYWEEK_VOUCHER_TEMPLATE_UUID: Final = "49bc000c-c3a6-47c7-bdfd-b8ccd3ae2677"

# The only nominal the supported contract recognises, in minor units. The POST
# price is still read from the *fresh* template cost; this literal only decides
# whether that fresh value is one the application supports, and it is the only
# price the transport will put on the wire.
SUPPORTED_VOUCHER_PRICE_MINOR: Final = 1500

# One voucher line, one unit. Not a parameter anywhere: a caller able to set it
# could preview a bulk purchase, and no bulk evidence exists.
SUPPORTED_VOUCHER_QUANTITY: Final = 1

# ---------------------------------------------------------------------------
# Controlled voucher mutation canary (§35)
# ---------------------------------------------------------------------------
# One scope string, one canary, enforced by a unique constraint in PostgreSQL.
# Running a second canary is deliberately a code change plus a review, not an
# operator decision: the whole point of the ledger is that "has this workspace
# already had its one-off voucher canary?" has exactly one durable answer.
VOUCHER_CANARY_SCOPE: Final = "easyweek_voucher_canary_v1"

# The request body shape the canary sends. Bumped whenever that shape changes,
# so an old ledger row cannot vouch for a request we no longer send.
VOUCHER_CANARY_REQUEST_SCHEMA_VERSION: Final = "1"

# Template facts the owner froze for the duration of the canary, confirmed by a
# read-only production probe on 11.09.2026. These are compared before every
# mutation and after every verification; a difference stops the canary rather
# than adapting to it.
FROZEN_TEMPLATE_FACTS: Final = {
    "is_enabled": True,
    "is_online": False,
    "is_single_charge": True,
    "cost": SUPPORTED_VOUCHER_PRICE_MINOR,
    "value": SUPPORTED_VOUCHER_PRICE_MINOR,
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
