"""Proving "exactly one voucher for €15" without inventing a quantity (§35).

The production smoke test returned an order whose ``vouchers`` list holds one
issued artifact — ``code``, ``voucher_template_uuid``, ``value``, ``price`` —
and no ``quantity`` key at all. The canary refused to pay for it, correctly:
nothing in that body said "one", and defaulting a missing quantity to 1 would
make an order for ten vouchers payable the moment its count arrived in a field
we do not know about.

What the body does prove is a count, in a different place: the list holds
exactly one issued artifact with its own code. This suite pins both accepted
proofs and, more importantly, everything that must still be refused.

Every identifier and code here is synthetic.
"""

from __future__ import annotations

import pytest

from altegio_bot.easyweek_voucher_canary.artifact import observe_artifact
from altegio_bot.easyweek_voucher_canary.orders import (
    CANARY_VOUCHER_LINE_UNPROVEN,
    payable_order_reasons,
)
from altegio_bot.easyweek_voucher_canary.plan import canary_marker
from altegio_bot.easyweek_voucher_canary.voucher_line import (
    QUANTITY_PROOF_EXPLICIT,
    QUANTITY_PROOF_SINGLETON,
    QUANTITY_PROOF_UNPROVEN,
    prove_voucher_line,
)
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    SUPPORTED_VOUCHER_PRICE_MINOR,
)
from altegio_bot.tests.easyweek_voucher_canary_fixtures import (
    CUSTOMER_UUID,
    OTHER_UUID,
    issued_voucher,
    open_order,
    voucher_line,
)

MARKER = canary_marker()
CODE_SENTINEL = "SYNTHETIC-CODE-aaa111"


def _prove(order):
    return prove_voucher_line(
        order,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )


def _payable(order) -> tuple[str, ...]:
    return payable_order_reasons(
        order,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )


def _observe(order):
    return observe_artifact(
        order,
        stage="line_proof",
        expected_customer_uuid=CUSTOMER_UUID,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )


# ---------------------------------------------------------------------------
# The two proofs that are accepted
# ---------------------------------------------------------------------------


def test_the_observed_production_shape_proves_one_issued_voucher() -> None:
    """One element, one code, the right template, price and value at 1500."""
    order = open_order(marker=MARKER, vouchers=[issued_voucher(code=CODE_SENTINEL)])

    proof = _prove(order)

    assert proof.proven is True
    assert proof.quantity_proof == QUANTITY_PROOF_SINGLETON
    assert "quantity" not in order["vouchers"][0]


def test_an_explicit_integer_quantity_still_proves_one() -> None:
    order = open_order(marker=MARKER, vouchers=[voucher_line()])

    proof = _prove(order)

    assert proof.proven is True
    assert proof.quantity_proof == QUANTITY_PROOF_EXPLICIT


def test_a_singular_voucher_object_with_an_exact_quantity_still_proves_one() -> None:
    """The previously supported shape is not dropped."""
    order = open_order(marker=MARKER)
    del order["vouchers"]
    order["voucher"] = voucher_line()

    proof = _prove(order)

    assert proof.proven is True
    assert proof.quantity_proof == QUANTITY_PROOF_EXPLICIT


def test_a_singular_voucher_object_without_quantity_proves_nothing() -> None:
    """Cardinality of a list is a count. A lone object is not.

    One object under `voucher` says nothing about how many were bought — there
    is no list whose length could mean anything.
    """
    order = open_order(marker=MARKER)
    del order["vouchers"]
    order["voucher"] = issued_voucher(code=CODE_SENTINEL)

    proof = _prove(order)

    assert proof.proven is False
    assert proof.quantity_proof == QUANTITY_PROOF_UNPROVEN


# ---------------------------------------------------------------------------
# A present quantity decides, and never falls back
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("quantity", [None, True, False, 1.0, "1", 0, 2, -1, [1], {"n": 1}])
def test_a_quantity_that_is_present_but_not_exactly_one_blocks(quantity) -> None:
    """The field was readable. It did not say one. That is the end of it.

    Especially `True`: in Python `True == 1`, so a truthiness or equality check
    would have accepted a boolean as a count of one.
    """
    order = open_order(marker=MARKER, vouchers=[issued_voucher(code=CODE_SENTINEL, quantity=quantity)])

    proof = _prove(order)

    assert proof.proven is False
    assert proof.quantity_proof == QUANTITY_PROOF_UNPROVEN


def test_a_wrong_quantity_never_falls_through_to_the_singleton_proof() -> None:
    """Otherwise "2" would be silently downgraded to "one issued artifact"."""
    order = open_order(marker=MARKER, vouchers=[issued_voucher(code=CODE_SENTINEL, quantity=2)])

    assert _prove(order).quantity_proof == QUANTITY_PROOF_UNPROVEN
    assert CANARY_VOUCHER_LINE_UNPROVEN in _payable(order)


# ---------------------------------------------------------------------------
# What the implicit proof requires
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("code", [None, "", 17, [], {}, True])
def test_an_issued_artifact_without_a_usable_code_proves_nothing(code) -> None:
    """The code is what makes the element an ISSUED artifact rather than a line."""
    line = issued_voucher(code=CODE_SENTINEL)
    line["code"] = code
    order = open_order(marker=MARKER, vouchers=[line])

    assert _prove(order).proven is False


def test_an_issued_artifact_with_no_code_key_proves_nothing() -> None:
    line = issued_voucher(code=CODE_SENTINEL)
    del line["code"]
    order = open_order(marker=MARKER, vouchers=[line])

    assert _prove(order).proven is False


@pytest.mark.parametrize("value", [None, 1499, 1500.0, "1500", True, 0])
def test_an_issued_artifact_whose_value_is_not_the_exact_nominal_blocks(value) -> None:
    line = issued_voucher(code=CODE_SENTINEL)
    line["value"] = value
    order = open_order(marker=MARKER, vouchers=[line])

    assert _prove(order).proven is False


def test_an_issued_artifact_with_no_value_key_proves_nothing() -> None:
    line = issued_voucher(code=CODE_SENTINEL)
    del line["value"]
    order = open_order(marker=MARKER, vouchers=[line])

    assert _prove(order).proven is False


@pytest.mark.parametrize("price", [None, 1499, 1500.0, "1500", True, 0])
def test_a_price_that_is_not_the_exact_nominal_blocks_either_proof(price) -> None:
    implicit = open_order(marker=MARKER, vouchers=[issued_voucher(code=CODE_SENTINEL, price=price)])
    explicit = open_order(marker=MARKER, vouchers=[voucher_line(price=price)])

    assert _prove(implicit).proven is False
    assert _prove(explicit).proven is False


@pytest.mark.parametrize("template", [None, OTHER_UUID, "", EASYWEEK_VOUCHER_TEMPLATE_UUID.upper()])
def test_another_template_blocks_either_proof(template) -> None:
    implicit = open_order(marker=MARKER, vouchers=[issued_voucher(code=CODE_SENTINEL, voucher_template_uuid=template)])
    explicit = open_order(marker=MARKER, vouchers=[voucher_line(voucher_template_uuid=template)])

    assert _prove(implicit).proven is False
    assert _prove(explicit).proven is False
    # A line for some other product is not "our order with a wrong field".
    assert _prove(implicit).shape_recognised is False


@pytest.mark.parametrize(
    "vouchers",
    [
        [],
        [issued_voucher(code=CODE_SENTINEL), issued_voucher(code=CODE_SENTINEL + "b")],
        [voucher_line(), voucher_line()],
        ["a string"],
        [None],
        "not a list",
        {"code": CODE_SENTINEL},
        17,
    ],
)
def test_any_container_that_is_not_exactly_one_voucher_object_blocks(vouchers) -> None:
    order = open_order(marker=MARKER, vouchers=vouchers)

    assert _prove(order).proven is False
    assert CANARY_VOUCHER_LINE_UNPROVEN in _payable(order)


def test_an_order_with_no_voucher_container_at_all_proves_nothing() -> None:
    order = open_order(marker=MARKER)
    del order["vouchers"]

    assert _prove(order).proven is False


def test_a_body_that_is_not_an_order_proves_nothing() -> None:
    for payload in (None, [], "order", 17):
        assert _prove(payload).proven is False


# ---------------------------------------------------------------------------
# One proof, two callers — they can never disagree
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "vouchers",
    [
        [issued_voucher(code=CODE_SENTINEL)],
        [voucher_line()],
        [issued_voucher(code=CODE_SENTINEL, quantity=2)],
        [voucher_line(price=999)],
        [],
        [voucher_line(), voucher_line()],
    ],
)
def test_the_observation_and_the_payment_gate_always_agree(vouchers) -> None:
    """Two implementations of this question could disagree, and either
    direction of that disagreement is a bug with money in it."""
    order = open_order(marker=MARKER, vouchers=vouchers)

    observation = _observe(order)
    blocked = CANARY_VOUCHER_LINE_UNPROVEN in _payable(order)

    assert observation.voucher_line_proven is not blocked
    assert observation.voucher_quantity_proof == _prove(order).quantity_proof


def test_the_safe_report_names_the_proof_and_still_hides_the_artifact() -> None:
    order = open_order(marker=MARKER, vouchers=[issued_voucher(code=CODE_SENTINEL)])

    safe = _observe(order).as_safe_dict()

    assert safe["voucher_quantity_proof"] == QUANTITY_PROOF_SINGLETON
    assert safe["voucher_line_proven"] is True
    # Finding an individual artifact is not the public customer-facing contract.
    assert safe["artifact_contract_proven"] is False
    assert CODE_SENTINEL not in str(safe)


def test_the_production_shape_is_payable_once_its_sum_is_published() -> None:
    """The end-to-end effect of the fix, at the gate that matters."""
    order = open_order(marker=MARKER, vouchers=[issued_voucher(code=CODE_SENTINEL)])

    assert _payable(order) == ()
