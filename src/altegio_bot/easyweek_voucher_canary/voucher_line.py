"""The single place that decides "is this one voucher for €15?" (§35).

One proof, used everywhere
--------------------------
Two parts of the canary have to answer this question: the payment
pre-condition, which must refuse to settle anything but the approved order, and
the artifact observation, which reports what a response actually showed. When
they answered it separately they could disagree, and a disagreement here is
either a payment that should not have happened or a proof that was not one.

So the decision lives here, in a module that imports nothing from the rest of
the canary, and both callers use it.

Why quantity is not simply defaulted
------------------------------------
The production smoke test showed that the created order — in the CREATE
response and in the exact readback alike — carries ``vouchers`` as a list of one
object with ``code``, ``voucher_template_uuid``, ``value`` and ``price``, and
**no** ``quantity`` key at all.

Writing ``line.get("quantity", 1)`` would have made that order payable, but it
would also have made *any* order payable whose quantity we simply could not
read — including one for ten vouchers whose count arrived in a field we do not
know about. A default is an assumption wearing the costume of a fact.

What the response does prove is a count, just not in the field we expected: the
list holds exactly one issued artifact, each with its own ``code``. One element
with one code is one voucher, and that is a proof about the list, not a guess
about a missing key. It is recorded under its own label so a report can never
imply the stronger ``quantity: 1`` we did not see.

The two accepted proofs are therefore:

``explicit_quantity``
    the line names ``quantity`` and it is exactly the integer 1;

``singleton_issued_artifact``
    ``vouchers`` is a list of exactly one issued artifact — a non-empty string
    ``code``, the confirmed template, ``price`` and ``value`` both exactly 1500
    — and the ``quantity`` key is wholly absent.

Either proof needs exactly one container. An order naming both ``vouchers`` and
``voucher`` proves nothing at all, even when one of them is null: two
containers is a body we do not understand, and reading whichever we happen to
look at first would be choosing an answer rather than finding one.

Anything else is ``unproven``. A ``quantity`` that IS present but wrong — null,
``true``, ``1.0``, ``"1"``, 0, 2 — never falls through to the singleton proof:
the field was readable and it did not say one.

The CREATE request is unaffected. It still sends an exact integer
``quantity: 1``, because what we ask for and what we can prove we received are
different things.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

# How the count of one was established. A closed vocabulary: these three
# strings reach reports, stage snapshots and therefore the authorisation digest.
QUANTITY_PROOF_EXPLICIT: Final = "explicit_quantity"
QUANTITY_PROOF_SINGLETON: Final = "singleton_issued_artifact"
QUANTITY_PROOF_UNPROVEN: Final = "unproven"

# The only container whose cardinality may stand in for a quantity. A singular
# ``voucher`` object proves nothing about how many were bought.
VOUCHER_COLLECTION_KEY: Final = "vouchers"
VOUCHER_OBJECT_KEY: Final = "voucher"


@dataclass(frozen=True)
class VoucherLineProof:
    """Whether this order holds exactly one voucher, and how we know."""

    proven: bool
    quantity_proof: str
    # True when the body carries a voucher-shaped node naming our template, even
    # if the numbers were wrong. Distinguishes "not the order we expected" from
    # "a shape we have never seen".
    shape_recognised: bool


_UNPROVEN: Final = VoucherLineProof(
    proven=False,
    quantity_proof=QUANTITY_PROOF_UNPROVEN,
    shape_recognised=False,
)


def _exact_int(value: object) -> int | None:
    """Exact ``int`` only. ``True`` is not 1 here, and ``1.0`` is not 1."""
    return value if type(value) is int else None


def _nonempty_str(value: object) -> bool:
    return isinstance(value, str) and bool(value)


def _single_line(order: dict[str, Any]) -> tuple[dict[str, Any] | None, bool]:
    """The one voucher node, and whether it came from the ``vouchers`` list.

    Returns ``(None, False)`` for every shape that is not exactly one voucher
    node under exactly one container.

    An order may name ``vouchers`` or ``voucher``, never both. Two containers
    is not one order described twice: it is a body we do not understand, and
    the one we happened to read first says nothing about the one we ignored.
    Even a null second container counts, because the KEY is what makes the body
    ambiguous.

    Presence is decided by the key, not by the value. ``vouchers: null`` is the
    provider saying something about the collection; it is not permission to go
    and read a different field instead, so it never falls back to the singular
    form.
    """
    has_collection = VOUCHER_COLLECTION_KEY in order
    has_single = VOUCHER_OBJECT_KEY in order
    if has_collection and has_single:
        return None, False

    if has_collection:
        collection = order[VOUCHER_COLLECTION_KEY]
        if not isinstance(collection, list):
            # Present but not a list: a shape we will not interpret.
            return None, False
        if len(collection) != 1 or not isinstance(collection[0], dict):
            return None, False
        return collection[0], True

    if has_single:
        single = order[VOUCHER_OBJECT_KEY]
        if isinstance(single, dict):
            return single, False
    return None, False


def prove_voucher_line(
    order: object,
    *,
    expected_template_uuid: str,
    expected_price_minor: int,
) -> VoucherLineProof:
    """Prove — or refuse to prove — one voucher of the approved template.

    Never assumes a missing field. Every accepted path is listed in the module
    docstring, and both of them are about something the response actually said.
    """
    if not isinstance(order, dict):
        return _UNPROVEN

    line, from_collection = _single_line(order)
    if line is None:
        return _UNPROVEN

    template_matches = line.get("voucher_template_uuid") == expected_template_uuid
    price = _exact_int(line.get("price"))
    value_present = "value" in line
    value = _exact_int(line.get("value"))
    code_present = "code" in line

    if not template_matches or price != expected_price_minor:
        # A recognised shape only when it names our template; a line for some
        # other product is not "our order with a wrong price".
        return VoucherLineProof(
            proven=False,
            quantity_proof=QUANTITY_PROOF_UNPROVEN,
            shape_recognised=template_matches,
        )

    # A value or a code that is present must be right, on either path.
    if value_present and value != expected_price_minor:
        return VoucherLineProof(False, QUANTITY_PROOF_UNPROVEN, True)
    if code_present and not _nonempty_str(line.get("code")):
        return VoucherLineProof(False, QUANTITY_PROOF_UNPROVEN, True)

    if "quantity" in line:
        # The field exists, so it — and nothing else — decides the count.
        if _exact_int(line.get("quantity")) == 1:
            return VoucherLineProof(True, QUANTITY_PROOF_EXPLICIT, True)
        return VoucherLineProof(False, QUANTITY_PROOF_UNPROVEN, True)

    # No quantity key at all. The only thing that may stand in for it is the
    # cardinality of a list of ISSUED artifacts: one element, one code, the
    # confirmed template, and both money fields exactly at the nominal.
    if from_collection and _nonempty_str(line.get("code")) and value == expected_price_minor:
        return VoucherLineProof(True, QUANTITY_PROOF_SINGLETON, True)

    return VoucherLineProof(False, QUANTITY_PROOF_UNPROVEN, True)
