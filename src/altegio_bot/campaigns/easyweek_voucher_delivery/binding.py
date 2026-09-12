"""Proving a voucher code without ever storing it (§36).

The problem
-----------
The code is a bearer secret: whoever reads it can spend €15. So it must not be
written to the ledger, the evidence, a job, an outbox row, a log, a report or a
ticket. But between paying for the order and sending the message the canary has
to answer one question about it — *is the code I am about to send the code this
paid order actually issued?* — and answering that from memory alone would mean
trusting whatever the last read happened to return.

Why not a plain digest
----------------------
A SHA-256 of the code would answer the question and be storable. It would also
be reversible in practice: voucher codes are short and drawn from a small
alphabet, so a digest of one can be walked back by enumeration in seconds. That
is the same reasoning that keeps §35's artifact values unhashed. A digest of a
low-entropy secret is the secret wearing a hat.

A keyed MAC removes that option from anyone holding the database alone: without
the key there is nothing to enumerate against.

What the MAC is bound to
------------------------
Not just the code. The material names the domain, the version, the ledger row
and the order, so a MAC is only meaningful in the exact place it was made:

* a MAC copied into another ledger row verifies against nothing;
* a MAC from another order verifies against nothing;
* a key rotation is loud rather than silent, because the stored key id no
  longer matches the configured one and the canary stops instead of comparing
  against the wrong secret.

The encoding is length-prefixed, so no two different tuples of fields can ever
produce the same byte string — a separator-only scheme would let a code
containing the separator impersonate a different binding.

Comparison is ``hmac.compare_digest``: an ordinary ``==`` on a MAC leaks, by
timing, how many leading characters were right.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Final

from altegio_bot.settings import settings

# Domain separation. A MAC made here can never be mistaken for, or reused as, a
# MAC made by some other part of this codebase with the same key.
_DOMAIN: Final = b"altegio_bot/easyweek_voucher_delivery/voucher_code/v1"

# Anything shorter is not a key. 32 bytes is the output width of the hash, and a
# secret narrower than the digest it keys is the weakest link by construction.
MIN_KEY_BYTES: Final = 32

# The key id is written next to every MAC, so it has to be a stable, bounded,
# non-secret label rather than a slice of the key itself.
MAX_KEY_ID_LENGTH: Final = 64


class VoucherBindingKeyError(RuntimeError):
    """The configured key cannot produce a usable binding.

    Carries a stable reason and never the key, its length or any part of it.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _encode(*parts: bytes) -> bytes:
    """Unambiguous canonical encoding: every part carries its own length.

    Without the length prefix, ``("ab", "c")`` and ``("a", "bc")`` would encode
    identically, and a voucher code containing the separator could impersonate a
    binding for a different order.
    """
    return b"".join(len(part).to_bytes(8, "big") + part for part in parts)


def load_binding_key(
    *,
    key: str | None = None,
    key_id: str | None = None,
) -> tuple[str, bytes]:
    """The configured key and its id, or a refusal naming which one failed.

    Reads the settings by default so that callers do not each re-implement the
    fence. There is deliberately no fallback and no generated default: a missing
    key stops the canary rather than silently downgrading it to an unkeyed
    digest that would not protect anything.
    """
    from altegio_bot.campaigns.easyweek_voucher_delivery.identity import (
        HMAC_KEY_INVALID,
        HMAC_KEY_MISSING,
    )

    raw_key = key if key is not None else settings.easyweek_voucher_delivery_hmac_key.get_secret_value()
    raw_id = key_id if key_id is not None else settings.easyweek_voucher_delivery_hmac_key_id

    secret = (raw_key or "").strip().encode("utf-8")
    identifier = (raw_id or "").strip()
    if not secret or not identifier:
        raise VoucherBindingKeyError(HMAC_KEY_MISSING)
    if len(secret) < MIN_KEY_BYTES or len(identifier) > MAX_KEY_ID_LENGTH:
        raise VoucherBindingKeyError(HMAC_KEY_INVALID)
    return identifier, secret


def binding_key_reason(*, key: str | None = None, key_id: str | None = None) -> str | None:
    """Diagnose the key without raising. ``None`` means it is usable.

    A plan has to be able to SAY that the key is missing — refusing to print
    anything at all would leave an operator with no way to tell a missing secret
    from a broken deployment.
    """
    try:
        load_binding_key(key=key, key_id=key_id)
    except VoucherBindingKeyError as exc:
        return exc.reason
    return None


def voucher_code_mac(
    *,
    voucher_code: str,
    ledger_uuid: str,
    target_order_uuid: str,
    voucher_template_uuid: str,
    key: str | None = None,
    key_id: str | None = None,
) -> tuple[str, str]:
    """``(key_id, hex MAC)`` for this code in this exact place.

    The code is consumed here and is not retained by this module: nothing is
    cached, logged or attached to the returned value.
    """
    identifier, secret = load_binding_key(key=key, key_id=key_id)
    material = _encode(
        _DOMAIN,
        identifier.encode("utf-8"),
        ledger_uuid.encode("utf-8"),
        target_order_uuid.encode("utf-8"),
        voucher_template_uuid.encode("utf-8"),
        voucher_code.encode("utf-8"),
    )
    return identifier, hmac.new(secret, material, hashlib.sha256).hexdigest()


def voucher_code_matches(
    *,
    voucher_code: str,
    expected_mac: str | None,
    expected_key_id: str | None,
    ledger_uuid: str,
    target_order_uuid: str,
    voucher_template_uuid: str,
    key: str | None = None,
    key_id: str | None = None,
) -> bool:
    """Is this the code the stored binding was made from?

    False — never an exception — for a missing binding, a rotated key or a
    mismatch, so that a caller always fails closed on the same branch. A key
    that cannot be loaded at all still raises, because that is a deployment
    fault rather than a verdict about the code.
    """
    identifier, computed = voucher_code_mac(
        voucher_code=voucher_code,
        ledger_uuid=ledger_uuid,
        target_order_uuid=target_order_uuid,
        voucher_template_uuid=voucher_template_uuid,
        key=key,
        key_id=key_id,
    )
    if not expected_mac or not expected_key_id:
        return False
    if not hmac.compare_digest(identifier, expected_key_id):
        # A MAC made with a different key is not a mismatch to investigate; it
        # is a key rotation nobody re-bound. Refuse before comparing.
        return False
    return hmac.compare_digest(computed, expected_mac)


__all__ = [
    "MIN_KEY_BYTES",
    "VoucherBindingKeyError",
    "binding_key_reason",
    "load_binding_key",
    "voucher_code_mac",
    "voucher_code_matches",
]
