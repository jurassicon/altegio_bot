"""§45.1 free gift certificate: the nominal and the issue price are separate (schema 4).

The product this covers issues a certificate worth 10 EUR for 0 EUR, through the
Aktionsgutscheine till. That one sentence contains the whole hazard: there are now
TWO money numbers per certificate where there used to be one, and almost every
existing check in this phase was written when they were the same number.

What is deliberately NOT asserted here
--------------------------------------
That our CREATE and PAY actually work against EasyWeek. Everything below runs
against fakes. A passing run of this module says the application refuses the wrong
shapes and sends the right ones; it does not say the provider accepts them, and it
is not evidence about accounting, revenue or master payroll. The owner's API
evidence of 09.10.2026 was gathered by issuing one certificate BY HAND in the
EasyWeek dashboard, which proves the product exists and what a correct order looks
like — not that this code path produces one.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import pytest
import sqlalchemy as sa
from sqlalchemy import text

from altegio_bot.campaigns.easyweek_voucher_production import (
    authorisation as authorisation_module,
)
from altegio_bot.campaigns.easyweek_voucher_production import (
    identity as identity_module,
)
from altegio_bot.campaigns.easyweek_voucher_production.account import (
    account_fingerprint,
    account_fingerprint_for_schema,
    expected_account_fingerprint,
    prove_current_account,
)
from altegio_bot.easyweek_voucher_canary.orders import classify_order, paid_order_amounts_proven, payable_order_reasons
from altegio_bot.easyweek_voucher_canary.voucher_line import prove_voucher_line
from altegio_bot.easyweek_voucher_production_contract import (
    BOUND_SCHEMA_VERSIONS,
    CURRENT_PRODUCTION_CONTRACT,
    CURRENT_SCHEMA_VERSIONS,
    GIFT_PRODUCTION_CONTRACT,
    LEGACY_PRODUCTION_CONTRACT,
    NEW_MAILING_SCHEMA_VERSION,
    is_current_fixed_contract,
    new_mailing_contract,
    production_contract,
)
from altegio_bot.tests import easyweek_voucher_production_fixtures as old

GIFT = GIFT_PRODUCTION_CONTRACT
PAID = CURRENT_PRODUCTION_CONTRACT


# ---------------------------------------------------------------------------
# 1. The product itself
# ---------------------------------------------------------------------------


def test_the_new_contract_is_a_ten_euro_certificate_that_costs_nothing_to_issue():
    """The two sums, the till and the version, stated once and exactly."""
    assert GIFT.face_value_minor == 1000
    assert GIFT.issue_price_minor == 0
    assert GIFT.free_issue is True
    assert GIFT.quantity == 1
    assert GIFT.validity_months == 1
    assert GIFT.version == "easyweek-production-gift-10eur-v1"
    assert GIFT.request_schema_version == "4" == NEW_MAILING_SCHEMA_VERSION
    assert GIFT.title == "Kundenkarte - 10€"
    assert GIFT.payment_account_label == "Aktionsgutscheine"
    # A new mailing is prepared under THIS product, by one name the page, the
    # composition read and the stage plan all use.
    assert new_mailing_contract() is GIFT
    assert production_contract("4") is GIFT


def test_the_template_the_provider_is_asked_for_separates_cost_from_value():
    """``cost`` is the money, ``value`` is the nominal. Both exact integers."""
    facts = GIFT.template_facts()
    assert facts["cost"] == 0
    assert facts["value"] == 1000
    assert type(facts["cost"]) is int and type(facts["value"]) is int
    assert facts["is_enabled"] is True
    assert facts["is_online"] is False
    assert facts["is_single_charge"] is True
    assert facts["forces_activation"] is True
    assert facts["activate_after"] == 0
    assert facts["activate_at"] is None
    # The paid product keeps its own single number, unchanged.
    assert PAID.template_facts()["cost"] == PAID.template_facts()["value"] == 1000


def test_the_new_product_settles_through_a_different_till_than_the_paid_one():
    assert GIFT.payment_account_fingerprint != PAID.payment_account_fingerprint
    # Schema 3 keeps meaning the Card account, which is the whole point of giving
    # the gift certificate its own schema rather than a flag on the old one.
    assert account_fingerprint_for_schema("3") == PAID.payment_account_fingerprint
    assert account_fingerprint_for_schema("4") == GIFT.payment_account_fingerprint
    # A caller written before the gift certificate existed still means the Card
    # account, because it passes no contract at all.
    assert expected_account_fingerprint() == PAID.payment_account_fingerprint
    # The pin is a fingerprint of a UUID, so no till UUID is stored in this
    # repository and a till's NAME decides nothing.
    assert len(GIFT.payment_account_fingerprint) == 64
    assert account_fingerprint("Aktionsgutscheine") is None
    assert account_fingerprint(GIFT.payment_account_label) is None


# ---------------------------------------------------------------------------
# 2. Nothing already written changed its meaning
# ---------------------------------------------------------------------------


def test_the_historical_contracts_keep_their_exact_digest_bytes():
    """The signed material and template facts of schemas 1-3 are unchanged.

    This is the compatibility guarantee the whole change rests on. Every HMAC,
    digest and frozen snapshot already written was computed over these bytes; if
    they move, existing records stop verifying and allowed refunds stop working.
    """
    # Captured from the pre-gift implementation at 8be7157. Never read git HEAD:
    # after committing this PR that would compare the implementation to itself.
    for contract, digest, facts_digest in (
        (
            LEGACY_PRODUCTION_CONTRACT,
            "8661ba1a731cb1d728171bc46c9a588050d367ddfb2f3c23173a963c7cf3cfe6",
            "270d62b10837867aa095b8734e9df3b7d4edfe731abfa8eb1073a42f141c925e",
        ),
        (
            CURRENT_PRODUCTION_CONTRACT,
            "f52fbff93cf2d7697ae6099e989c3e06ac2ccbd20a1495aff4fd8eb7be84dd72",
            "ac55eb0da3b97c6cbb8ce4b39a9c038a68bb92c7a0edffc34dc4a5fe959e3644",
        ),
    ):
        assert hashlib.sha256(json.dumps(contract.digest_material(), sort_keys=True).encode()).hexdigest() == digest
        assert (
            hashlib.sha256(json.dumps(contract.template_facts(), sort_keys=True).encode()).hexdigest() == facts_digest
        )
    # The two new sums appear ONLY where they say something: a contract with one
    # number does not grow two fields that repeat it.
    paid = CURRENT_PRODUCTION_CONTRACT.digest_material()
    assert "voucher_face_value_minor" not in paid
    assert "voucher_issue_price_minor" not in paid
    assert "product_title" not in paid


def test_the_new_contract_signs_both_sums_the_product_the_till_and_the_title():
    material = GIFT.digest_material()
    # The historical field keeps its historical meaning — the NOMINAL — so a
    # schema 3 digest is unchanged while a schema 4 one is unambiguous.
    assert material["voucher_unit_price_minor"] == 1000
    assert material["voucher_face_value_minor"] == 1000
    assert material["voucher_issue_price_minor"] == 0
    assert material["product_title"] == "Kundenkarte - 10€"
    assert material["product_contract_version"] == GIFT.version
    assert material["payment_account_fingerprint"] == GIFT.payment_account_fingerprint
    assert material["template_facts"]["cost"] == 0
    assert material["template_facts"]["value"] == 1000
    assert material["message_body_sha256"]
    # Two different products can never produce the same signed bytes.
    gift_bytes = json.dumps(material, sort_keys=True).encode()
    paid_bytes = json.dumps(PAID.digest_material(), sort_keys=True).encode()
    assert hashlib.sha256(gift_bytes).hexdigest() != hashlib.sha256(paid_bytes).hexdigest()


@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        ("1", "46d873dffbb2feb9844b66b0dd853f103f59b1e430438138438f63fabedd905f"),
        ("2", "a85783989b3f4585612eba441f8d7940e36f2cd1b85d5d48ce5f98866b2c24b1"),
        ("3", "e7274b1e3814dcb05814c4d8ca3c20440495408e92953dba336198915fa6baa5"),
    ],
)
def test_historical_stage_signed_projections_keep_pre_gift_bytes(schema, expected):
    """Stable 8be7157 fixtures cover nested snapshots, not only the product."""
    from altegio_bot.campaigns.easyweek_voucher_production.composition import BatchApproval, ProductionComposition
    from altegio_bot.campaigns.easyweek_voucher_production.ledger import BatchSnapshot
    from altegio_bot.campaigns.easyweek_voucher_production.readiness import ProductionPrerequisites

    contract = production_contract(schema)
    composition = ProductionComposition(
        proven=True,
        preview_run_id=7,
        schema_version=schema,
        approval=BatchApproval(1, contract.face_value_minor),
    )
    prerequisites = ProductionPrerequisites(stage="create", fence_open=True, schema_version=schema)
    batch = BatchSnapshot(
        exists=True,
        batch_id=11,
        schema_version=schema,
        product_contract_version=contract.version,
        voucher_unit_price_minor=contract.face_value_minor,
    )
    snapshot = {
        "composition": composition.as_safe_dict(),
        "prerequisites": prerequisites.as_safe_dict(),
        "batch": batch.as_safe_dict(),
        "voucher_unit_price_minor": contract.face_value_minor,
    }
    if schema == "3":
        snapshot["product_contract"] = contract.digest_material()
    assert (
        authorisation_module.stage_digest(
            stage="create",
            snapshot=snapshot,
            ledger_state={},
            issued_at=datetime(2026, 10, 9, tzinfo=timezone.utc),
        )
        == expected
    )


def test_a_product_version_cannot_turn_a_paid_schema_into_a_free_one():
    """The version is checked AGAINST the schema, not accepted in place of it."""
    with pytest.raises(ValueError, match="contract_unproven"):
        production_contract("3", contract_version=GIFT.version)
    with pytest.raises(ValueError, match="contract_unproven"):
        production_contract("4", contract_version=PAID.version)
    with pytest.raises(ValueError, match="contract_unproven"):
        production_contract("2", contract_version=GIFT.version)
    # The matching pair is the only way in.
    assert production_contract("4", contract_version=GIFT.version) is GIFT
    assert production_contract("3", contract_version=PAID.version) is PAID


def test_the_new_version_inherits_every_current_contract_guard():
    """Schema 4 is a CURRENT fixed contract, so the schema 3 rules apply to it."""
    assert is_current_fixed_contract("4") is True
    assert is_current_fixed_contract("3") is True
    assert is_current_fixed_contract("2") is False
    assert is_current_fixed_contract("1") is False
    assert CURRENT_SCHEMA_VERSIONS == frozenset({"3", "4"})
    # The voucher MAC domain: schema 1 keeps the legacy domain, everything newer
    # binds the recipient and the snapshot.
    assert BOUND_SCHEMA_VERSIONS == frozenset({"2", "3", "4"})
    legacy = identity_module.binding_material(batch_id=7, slot=1, schema_version="1")
    assert legacy == f"{identity_module.PRODUCTION_SCOPE}:7:1"
    bound = identity_module.binding_material(
        batch_id=7,
        slot=1,
        schema_version="4",
        frozen_digest="f" * 64,
        recipient_basis="earned",
        customer_uuid="cccccccc-1111-4111-8111-cccccccccccc",
        product_contract_version=GIFT.version,
    )
    assert bound.startswith(f"{identity_module.PRODUCTION_SCOPE}:7:1:v4:")
    assert bound != legacy


def test_a_snapshot_names_its_schema_by_its_product_version_not_by_a_key_existing():
    """Presence of ``product_contract`` is not a version. The version is."""
    gift_snapshot = {"product_contract": GIFT.digest_material()}
    paid_snapshot = {"product_contract": PAID.digest_material()}
    assert authorisation_module._schema_version_of(gift_snapshot) == "4"
    assert authorisation_module._schema_version_of(paid_snapshot) == "3"
    # An unrecognised or absent product falls back to the historical answer
    # rather than silently claiming to be the newest contract.
    assert authorisation_module._schema_version_of({}) == identity_module.PRODUCTION_SCHEMA_VERSION
    assert (
        authorisation_module._schema_version_of({"product_contract": {"product_contract_version": "invented"}})
        == identity_module.PRODUCTION_SCHEMA_VERSION
    )


# ---------------------------------------------------------------------------
# 3. Zero is not proof of payment
# ---------------------------------------------------------------------------


def _order(**changes):
    order = {
        "uuid": "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa",
        "status": "open",
        "is_reverted": False,
        "total": 0,
        "subtotal": 0,
        "amount_paid": 0,
        "amount_due": 0,
        "discount_amount": 0,
        "promocode_discount_amount": 0,
        "voucher_paid_amount": 0,
        "account_paid_amount": 0,
    }
    order.update(changes)
    return order


def test_an_open_order_with_zero_sums_is_never_payment_at_issue_price_zero():
    """The hazard this product introduces, stated as a test.

    ``amount_due == 0 and amount_paid == expected_price`` is satisfied by an
    untouched OPEN order the moment the expected price is 0. An OPEN order stays
    OPEN: a zero payment has to be CONFIRMED, not inferred from arithmetic that
    happens to balance.
    """
    state, proof = classify_order(_order(status="open"), expected_price_minor=0)
    assert state == "open"
    assert proof == "none"
    # The same arithmetic still proves a real payment when real money moved.
    paid_state, paid_proof = classify_order(
        _order(status="paid", total=1000, subtotal=1000, amount_paid=1000), expected_price_minor=1000
    )
    assert paid_state == "paid"
    assert paid_proof != "none"
    # And an explicit paid status is accepted for the free product too.
    free_state, free_proof = classify_order(_order(status="paid"), expected_price_minor=0)
    assert free_state == "paid"
    assert free_proof != "none"


@pytest.mark.parametrize("status", [None, "", "pending", "unknown", "issued", 0, False, 1, [], {}])
def test_an_unknown_or_contradictory_status_is_never_a_successful_free_issue(status):
    state, proof = classify_order(_order(status=status), expected_price_minor=0)
    assert state != "paid"
    assert proof == "none"


def test_a_reverted_order_is_not_paid_however_its_sums_add_up():
    state, _ = classify_order(_order(status="paid", is_reverted=True), expected_price_minor=0)
    assert state != "paid"


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "open", "is_paid": True},
        {"status": "unrecognised", "is_paid": True},
        {"status": None, "is_paid": True},
        {"status": "paid", "is_paid": False},
        {"status": "paid", "is_paid": "true"},
        {"status": "paid", "is_reverted": None},
    ],
)
def test_free_issue_rejects_conflicting_or_untyped_status_signals(changes):
    payload = _order(**changes)
    assert classify_order(payload, expected_price_minor=0)[0] == "unknown"
    assert not paid_order_amounts_proven(payload, expected_price_minor=0)


@pytest.mark.parametrize(
    "field", ["discount_amount", "promocode_discount_amount", "voucher_paid_amount", "account_paid_amount"]
)
@pytest.mark.parametrize("value", [1, 1000, -1000, None, "0", False])
def test_zero_totals_do_not_hide_compensating_adjustments(field, value):
    for nested in (False, True):
        payload = _order(status="paid", vouchers=[_voucher()])
        level = payload.setdefault("invoice", {}) if nested else payload
        level[field] = value
        assert not paid_order_amounts_proven(payload, expected_price_minor=0)
        payload["status"] = "open"
        assert payable_order_reasons(
            payload, expected_template_uuid=GIFT.template_uuid, expected_price_minor=0, expected_value_minor=1000
        )


@pytest.mark.parametrize("field", ["discount_amount", "promocode_discount_amount", "voucher_paid_amount"])
def test_a_free_issue_with_a_compensating_discount_is_refused(field):
    """price 0 means price 0, not 1000 with something taken off it."""
    reasons = payable_order_reasons(
        _order(status="open", total=1000, subtotal=1000, vouchers=[_voucher()], **{field: 1000}),
        expected_template_uuid=GIFT.template_uuid,
        expected_price_minor=0,
        expected_value_minor=1000,
    )
    assert reasons


@pytest.mark.parametrize("bad", [True, False, None, "0", "1000", 0.0, 1000.0, [], {}])
def test_a_money_field_that_is_not_an_integer_is_never_zero(bad):
    kwargs = {
        "expected_template_uuid": GIFT.template_uuid,
        "expected_price_minor": 0,
        "expected_value_minor": 1000,
    }
    assert payable_order_reasons(_order(status="open", total=bad, vouchers=[_voucher()]), **kwargs)
    # A correct free order with integer zeros is the one shape that passes, so the
    # refusals above are about the TYPE and not the helper refusing everything.
    assert payable_order_reasons(_order(status="open", vouchers=[_voucher()]), **kwargs) == ()


# ---------------------------------------------------------------------------
# 4. The issued artifact
# ---------------------------------------------------------------------------


def _voucher(**changes):
    voucher = {
        "uuid": "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb",
        "voucher_template_uuid": GIFT.template_uuid,
        "price": 0,
        "value": 1000,
        "code": "SYNTHETIC-CODE",
    }
    voucher.update(changes)
    return voucher


def test_one_correct_issued_certificate_at_price_zero_and_nominal_1000_proves_the_issue():
    """Exactly one artifact, the right template, price 0, value 1000, a code."""
    proof = prove_voucher_line(
        {"vouchers": [_voucher()]},
        expected_template_uuid=GIFT.template_uuid,
        expected_price_minor=0,
        expected_value_minor=1000,
    )
    assert proof.proven is True
    assert proof.quantity_proof != "unproven"


@pytest.mark.parametrize(
    "change",
    [
        {"price": 1000},
        {"price": 1500},
        {"price": -1000},
        {"price": None},
        {"price": "0"},
        {"price": True},
        {"value": 0},
        {"value": 1500},
        {"value": None},
        {"value": "1000"},
        {"voucher_template_uuid": "49bc000c-c3a6-47c7-bdfd-b8ccd3ae2677"},
        {"voucher_template_uuid": None},
        {"code": ""},
        {"code": None},
    ],
)
def test_a_wrong_price_nominal_template_or_missing_code_is_not_a_successful_issue(change):
    proof = prove_voucher_line(
        {"vouchers": [_voucher(**change)]},
        expected_template_uuid=GIFT.template_uuid,
        expected_price_minor=0,
        expected_value_minor=1000,
    )
    assert proof.proven is False


def test_an_absent_price_field_is_not_a_free_certificate():
    """A shape that simply omits the money is unknown, not zero."""
    voucher = _voucher()
    del voucher["price"]
    proof = prove_voucher_line(
        {"vouchers": [voucher]},
        expected_template_uuid=GIFT.template_uuid,
        expected_price_minor=0,
        expected_value_minor=1000,
    )
    assert proof.proven is False


def test_an_explicit_quantity_does_not_prove_an_issued_gifts_missing_nominal():
    voucher = _voucher(quantity=1)
    del voucher["value"]
    assert not prove_voucher_line(
        {"vouchers": [voucher]},
        expected_template_uuid=GIFT.template_uuid,
        expected_price_minor=0,
        expected_value_minor=1000,
    ).proven
    # A draft may prove its count without yet claiming an issued artifact/code.
    del voucher["code"]
    assert prove_voucher_line(
        {"vouchers": [voucher]},
        expected_template_uuid=GIFT.template_uuid,
        expected_price_minor=0,
        expected_value_minor=1000,
    ).proven


@pytest.mark.parametrize(
    "order",
    [
        {"vouchers": []},
        {"vouchers": [_voucher(), _voucher()]},
        {"vouchers": "not-a-list"},
        {"vouchers": None},
        {"vouchers": [None]},
        {},
        # Both containers named at once: one order described twice is a body we
        # do not understand, not a singleton.
        {"vouchers": [_voucher()], "voucher": _voucher()},
    ],
)
def test_the_singleton_proof_survives_a_shape_without_a_quantity(order):
    """One issued artifact, kept as the proof of ``quantity=1``.

    The issued shape has no ``quantity`` of its own, so "exactly one" is proven
    by there being exactly one correct artifact and nothing else.
    """
    proof = prove_voucher_line(
        order,
        expected_template_uuid=GIFT.template_uuid,
        expected_price_minor=0,
        expected_value_minor=1000,
    )
    assert proof.proven is False


# ---------------------------------------------------------------------------
# 5. The till is proven live, per contract
# ---------------------------------------------------------------------------


class _Accounts:
    def __init__(self, payload):
        self.payload = payload
        self.calls = 0

    async def list_location_accounts(self, location_uuid):
        self.calls += 1
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


async def test_the_gift_till_is_proven_and_the_card_till_is_refused_for_it():
    gift = old.GIFT_ACCOUNT_UUID
    card = old.ACCOUNT_UUID
    import unittest.mock as mock

    with mock.patch(
        "altegio_bot.campaigns.easyweek_voucher_production.account.expected_account_fingerprint",
        lambda contract=None: account_fingerprint(gift if contract is not None and contract.free_issue else card),
    ):
        reader = _Accounts([{"uuid": gift}])
        assert await prove_current_account(reader, account_uuid=gift, location_uuid="loc", contract=GIFT) is True
        # No fallback to the Card till for the new product...
        assert await prove_current_account(reader, account_uuid=card, location_uuid="loc", contract=GIFT) is False
        # ...and no fallback to the new till for the paid one.
        assert await prove_current_account(reader, account_uuid=gift, location_uuid="loc", contract=PAID) is False
        # Pinned AND live. A till that no longer belongs to the branch refuses,
        # and so does an unreadable listing: neither invents a membership.
        assert (
            await prove_current_account(
                _Accounts([{"uuid": card}]), account_uuid=gift, location_uuid="l", contract=GIFT
            )
            is False
        )
        assert (
            await prove_current_account(
                _Accounts(RuntimeError("boom")), account_uuid=gift, location_uuid="l", contract=GIFT
            )
            is False
        )
        # A name is not an identity.
        assert (
            await prove_current_account(
                _Accounts([{"uuid": gift, "name": "Aktionsgutscheine"}, {"uuid": card, "name": "Aktionsgutscheine"}]),
                account_uuid=card,
                location_uuid="l",
                contract=GIFT,
            )
            is False
        )


# ---------------------------------------------------------------------------
# 6. The database says the same thing as the contract
# ---------------------------------------------------------------------------


async def test_the_batch_table_refuses_a_free_batch_whose_money_is_not_zero(session_maker):
    """The CHECK constraints, not just the Python, hold the two sums apart."""
    async with session_maker() as session:
        names = set(
            (
                await session.execute(
                    text(
                        "SELECT c.conname FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid"
                        " WHERE t.relname = 'easyweek_voucher_production_batches' AND c.contype = 'c'"
                    )
                )
            )
            .scalars()
            .all()
        )
    assert "ck_ew_voucher_production_batch_issue_price_contract" in names
    assert "ck_ew_voucher_production_batch_issue_price_matches" in names


async def test_the_approval_table_records_both_sums_for_the_new_contract(session_maker):
    async with session_maker() as session:
        names = set(
            (
                await session.execute(
                    text(
                        "SELECT c.conname FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid"
                        " WHERE t.relname = 'easyweek_voucher_production_approvals' AND c.contype = 'c'"
                    )
                )
            )
            .scalars()
            .all()
        )
    assert "ck_ew_voucher_production_approval_issue_price" in names
    assert "ck_ew_voucher_production_approval_issue_price_sign" in names


async def test_no_historical_money_column_was_zeroed_by_the_upgrade(session_maker):
    """Schemas 1-3 keep the nominal they were frozen with in BOTH columns."""
    from altegio_bot.models.models import EasyWeekVoucherProductionBatch

    async with session_maker() as session:
        rows = (
            await session.execute(
                sa.select(
                    EasyWeekVoucherProductionBatch.request_schema_version,
                    EasyWeekVoucherProductionBatch.voucher_unit_price_minor,
                    EasyWeekVoucherProductionBatch.voucher_issue_price_minor,
                ).where(EasyWeekVoucherProductionBatch.request_schema_version != "4")
            )
        ).all()
    for schema, nominal, issue in rows:
        assert nominal == issue, f"schema {schema} was sold, so its two sums must agree"
        assert issue > 0
