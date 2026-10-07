"""The new product cannot inherit historical amounts, approvals or MACs."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from altegio_bot.campaigns.easyweek_manual_voucher.eligibility import ManualRecipientProof
from altegio_bot.campaigns.easyweek_voucher_production.authorisation import stage_digest
from altegio_bot.campaigns.easyweek_voucher_production.baseline import prove_production_baseline
from altegio_bot.campaigns.easyweek_voucher_production.composition import (
    BatchApproval,
    ProductionComposition,
    ProductionMember,
)
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    APPROVAL_EXPOSURE_MISMATCH,
    PRODUCTION_SCOPE,
    binding_material,
)
from altegio_bot.easyweek_voucher_production_contract import (
    CURRENT_PAYMENT_ACCOUNT_FINGERPRINT,
    CURRENT_PRODUCTION_META_BODY,
    LEGACY_PRODUCTION_CONTRACT,
    production_contract,
)
from altegio_bot.easyweek_voucher_production_contract import (
    CURRENT_PRODUCTION_CONTRACT as CURRENT,
)


def product_payload(**changes):
    return {
        "uuid": CURRENT.template_uuid,
        **CURRENT.template_facts(),
        "vouchers_count": 12,
        "activated_vouchers_count": 11,
        **changes,
    }


@pytest.mark.parametrize("schema", ["1", "2"])
def test_historical_versions_keep_original_product(schema):
    contract = production_contract(schema)
    assert contract is LEGACY_PRODUCTION_CONTRACT
    assert contract.unit_price_minor == 1500 and contract.validity_months is None
    with pytest.raises(ValueError, match="contract_unproven"):
        production_contract(schema, contract_version=CURRENT.version)


@pytest.mark.parametrize("schema", ["0", "4", "v3", None, 3])
def test_unknown_schema_never_selects_the_current_contract(schema):
    with pytest.raises(ValueError, match="contract_unproven"):
        production_contract(schema)


def test_fixed_new_product_and_message_are_signed_as_one_contract():
    contract = production_contract("3", contract_version=CURRENT.version)
    assert contract.unit_price_minor == 1000
    assert contract.quantity == 1 and contract.validity_months == 1
    material = contract.digest_material()
    assert material["template_facts"]["is_single_charge"] is True
    assert material["message_contract_code"] == "new_client_voucher_10eur_v2"
    assert material["payment_account_fingerprint"] == CURRENT_PAYMENT_ACCOUNT_FINGERPRINT
    assert "payment_account_fingerprint" not in LEGACY_PRODUCTION_CONTRACT.digest_material()
    assert material["message_body_sha256"] == hashlib.sha256(CURRENT_PRODUCTION_META_BODY.encode()).hexdigest()
    assert "ab Aktivierung einen Monat gültig und einmalig einlösbar" in CURRENT_PRODUCTION_META_BODY
    assert "vouchers_count" not in material["template_facts"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("uuid", LEGACY_PRODUCTION_CONTRACT.template_uuid),
        ("is_single_charge", False),
        ("validity", None),
        ("validity", 2),
        ("validity", True),
        ("validity", "1"),
        ("is_enabled", False),
        ("cost", 1500),
        ("value", 1500),
        ("cost", 1000.0),
        ("forces_activation", False),
        ("activate_after", 1),
        ("activate_at", "2026-10-07"),
        ("is_connected_all_branches", False),
        ("is_connected_all_services", False),
        ("services_count", 42),
    ],
)
def test_new_baseline_rejects_product_drift(field, value):
    proof = prove_production_baseline(product_payload(**{field: value}), contract=CURRENT)
    assert not proof.proven
    assert field in proof.mismatched_fields


def test_counters_can_grow_without_becoming_product_drift():
    before = prove_production_baseline(product_payload(vouchers_count=0, activated_vouchers_count=0), contract=CURRENT)
    after = prove_production_baseline(product_payload(vouchers_count=34, activated_vouchers_count=34), contract=CURRENT)
    assert before.proven and after.proven and before.digest == after.digest
    assert not prove_production_baseline(
        product_payload(vouchers_count=1, activated_vouchers_count=2), contract=CURRENT
    ).proven


def test_34_member_composition_uses_actual_count_and_preserves_historical_amount():
    members = tuple(
        ProductionMember(
            slot=index,
            campaign_recipient_id=index,
            proof=ManualRecipientProof(proven=True, easyweek_customer_uuid=f"00000000-0000-4000-8000-{index:012d}"),
            recipient_basis="earned_first_visit" if index <= 16 else "operator_manual_selection",
        )
        for index in range(1, 35)
    )
    current = ProductionComposition(
        proven=True,
        preview_run_id=987,
        members=members,
        approval=BatchApproval(34, 34000),
        schema_version="3",
    )
    historical = replace(current, schema_version="2", approval=BatchApproval(34, 51000))
    assert current.total_exposure_minor == 34000 and historical.total_exposure_minor == 51000
    assert current.as_safe_dict()["earned_recipient_count"] == 16
    assert current.as_safe_dict()["manual_recipient_count"] == 18
    assert current.digest() != historical.digest()
    assert current.composition_digest() != historical.composition_digest()
    assert not current.approval.reasons_against(34, schema_version="3")
    assert APPROVAL_EXPOSURE_MISMATCH in historical.approval.reasons_against(34, schema_version="3")
    assert replace(current, members=members[:-1]).total_exposure_minor == 33000


def test_historical_binding_bytes_unchanged_and_new_contract_has_a_new_domain():
    args = dict(
        batch_id=97,
        slot=2,
        frozen_digest="a" * 64,
        recipient_basis="earned_first_visit",
        manual_policy=None,
        source_proof_digest="b" * 64,
        customer_uuid="00000000-0000-4000-8000-000000000001",
    )
    legacy = f"{PRODUCTION_SCOPE}:97:2"
    material = {key: value for key, value in args.items() if key not in ("batch_id", "slot")}
    material["schema_version"] = "2"
    expected_v2 = legacy + ":v2:" + hashlib.sha256(json.dumps(material, sort_keys=True).encode()).hexdigest()
    assert binding_material(**args, schema_version="1") == legacy
    assert binding_material(**args, schema_version="2") == expected_v2
    current = binding_material(
        **args, schema_version="3", product_contract_version=CURRENT.version, message_contract_code=CURRENT.message_code
    )
    assert current != expected_v2 and ":v3:" in current
    with pytest.raises(ValueError, match="contract_unproven"):
        binding_material(**args, schema_version="3", product_contract_version=LEGACY_PRODUCTION_CONTRACT.version)
    with pytest.raises(ValueError, match="identity_unproven"):
        binding_material(**args, schema_version="3", message_contract_code="new_client_voucher")


def test_product_contract_changes_execution_authorisation_without_changing_legacy_bytes():
    when = datetime(2026, 10, 7, tzinfo=timezone.utc)
    snapshot = {"target_slots": [1], "unit_price_minor": 1500}
    historical_material = {
        "batch_scope": PRODUCTION_SCOPE,
        "schema_version": "2",
        "stage": "create",
        "snapshot": snapshot,
        "ledger_state": {},
        "plan_issued_at": when.isoformat(),
    }
    legacy = stage_digest(stage="create", snapshot=snapshot, ledger_state={}, issued_at=when)
    assert legacy == hashlib.sha256(json.dumps(historical_material, sort_keys=True, default=str).encode()).hexdigest()
    current = stage_digest(
        stage="create",
        snapshot={**snapshot, "product_contract": CURRENT.digest_material()},
        ledger_state={},
        issued_at=when,
    )
    assert current != legacy


def test_missing_activation_field_does_not_prove_explicit_null():
    payload = product_payload()
    del payload["activate_at"]
    proof = prove_production_baseline(payload, contract=CURRENT)
    assert not proof.proven and "activate_at" in proof.mismatched_fields
