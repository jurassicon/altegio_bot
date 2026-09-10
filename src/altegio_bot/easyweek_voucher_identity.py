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
