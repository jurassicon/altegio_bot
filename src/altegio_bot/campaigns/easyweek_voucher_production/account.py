"""The unchanged approved Card account, pinned only for new §45 effects."""

from __future__ import annotations

import hashlib
from typing import Any
from uuid import UUID

from altegio_bot.easyweek_voucher_production_contract import CURRENT_PAYMENT_ACCOUNT_FINGERPRINT

# A non-secret identity pin, like the issuer pin. Runtime identity stays in
# deployment configuration; neither a new env setting nor arbitrary UI input.
APPROVED_ACCOUNT_FINGERPRINT = CURRENT_PAYMENT_ACCOUNT_FINGERPRINT


def account_fingerprint(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        canonical = str(UUID(value.strip()))
    except ValueError:
        return None
    return hashlib.sha256(f"easyweek-voucher-production-account-v1:{canonical}".encode()).hexdigest()


def expected_account_fingerprint() -> str:
    return APPROVED_ACCOUNT_FINGERPRINT


async def prove_current_account(reader: Any, *, account_uuid: str, location_uuid: str) -> bool:
    """Exact configured account and one matching entry in the branch collection.

    ``list_location_accounts`` is documented as a complete non-paginated list
    or data envelope. A different/malformed shape cannot prove membership.
    This proof is not a dependency of historical recovery or pre-send refund.
    """
    if account_fingerprint(account_uuid) != expected_account_fingerprint():
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
