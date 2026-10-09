"""One approved till per product contract, pinned only for new §45 effects.

The paid 10 EUR product settles through the Card account and the free gift
certificate through Aktionsgutscheine. Which till a contract uses is part of the
contract, not a deployment choice and not something a browser may name: the pin
below is compared against the configured runtime UUID, so a deployment pointed at
the wrong till refuses rather than quietly charging the wrong account.

A till's NAME is not its identity. Only the fingerprint of the canonical UUID is,
which is also why no account UUID is tracked in this repository.
"""

from __future__ import annotations

import hashlib
from typing import Any
from uuid import UUID

from altegio_bot.easyweek_voucher_production_contract import (
    CURRENT_PAYMENT_ACCOUNT_FINGERPRINT,
    ProductionVoucherContract,
    production_contract,
)

# A non-secret identity pin, like the issuer pin. Runtime identity stays in
# deployment configuration; neither a new env setting nor arbitrary UI input.
# Kept as the schema 3 answer so existing callers and recovery paths are unchanged.
APPROVED_ACCOUNT_FINGERPRINT = CURRENT_PAYMENT_ACCOUNT_FINGERPRINT


def account_fingerprint_for_schema(schema_version: str) -> str:
    """The approved till for this request schema, by the one contract it means."""
    return expected_account_fingerprint(production_contract(schema_version))


def account_fingerprint(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        canonical = str(UUID(value.strip()))
    except ValueError:
        return None
    return hashlib.sha256(f"easyweek-voucher-production-account-v1:{canonical}".encode()).hexdigest()


def expected_account_fingerprint(contract: ProductionVoucherContract | None = None) -> str:
    """The till this contract's money moves through.

    ``None`` keeps the historical answer — the Card account of schema 3 — so every
    caller written before the gift certificate existed means exactly what it meant.
    """
    if contract is None:
        return APPROVED_ACCOUNT_FINGERPRINT
    return contract.payment_account_fingerprint


async def prove_current_account(
    reader: Any,
    *,
    account_uuid: str,
    location_uuid: str,
    contract: ProductionVoucherContract | None = None,
) -> bool:
    """Exact configured account and one matching entry in the branch collection.

    ``list_location_accounts`` is documented as a complete non-paginated list
    or data envelope. A different/malformed shape cannot prove membership.
    This proof is not a dependency of historical recovery or pre-send refund.

    Both halves matter and neither substitutes for the other: the fingerprint says
    this is the till the CONTRACT approves, and the live listing says that till
    still belongs to this branch. A name match would say neither.
    """
    if account_fingerprint(account_uuid) != expected_account_fingerprint(contract):
        return False
    try:
        payload = await reader.list_location_accounts(location_uuid)
    except Exception:
        return False
    if isinstance(payload, dict):
        if payload.get("meta") is not None or payload.get("links") is not None:
            return False
        payload = payload.get("data")
    if not isinstance(payload, list) or any(not isinstance(row, dict) for row in payload):
        return False
    for row in payload:
        value = row.get("uuid")
        if not isinstance(value, str):
            return False
        try:
            if str(UUID(value)) != value:
                return False
        except ValueError:
            return False
    return sum(row.get("uuid") == account_uuid for row in payload) == 1
