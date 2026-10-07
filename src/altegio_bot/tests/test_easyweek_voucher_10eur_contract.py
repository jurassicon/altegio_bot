"""The new product cannot inherit historical amounts, approvals or MACs."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from altegio_bot.campaigns.easyweek_manual_voucher.eligibility import ManualRecipientProof
from altegio_bot.campaigns.easyweek_voucher_production import runner as production_runner
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
from altegio_bot.tests import easyweek_voucher_10eur_fixtures as new
from altegio_bot.tests import easyweek_voucher_production_fixtures as old
from altegio_bot.tests.test_easyweek_voucher_production_mailing import _apply, _freeze


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


# ===========================================================================
# F3: the diagnostic report's nominal, and what "no baseline" does not mean
# ===========================================================================
# ``StageReport.as_safe_dict`` used to pick the historical contract whenever the
# report carried no baseline — and ``run_status`` carries none. So the one
# read-only command an operator uses to orient themselves printed €15 and
# ``N × 1500`` as the arithmetic of a contract that is €10, both on an empty
# ledger and on the mixed list of every batch. The amount a batch was frozen
# under is a property of that batch; the default is a property of the contract.

LEGACY_ARITHMETIC = "approved_exposure_minor = expected_recipient_count * 1500"
CURRENT_ARITHMETIC = "approved_exposure_minor = expected_recipient_count * 1000"


def _report(**kwargs):
    kwargs.setdefault("stage", "status")
    kwargs.setdefault("outcome", "observed")
    return production_runner.StageReport(**kwargs).as_safe_dict()


@pytest.mark.parametrize("baseline", [None, {}, {"baseline_version": None}, {"baseline_version": "something-else"}])
def test_a_report_with_no_named_baseline_states_the_contract_new_mailings_use(baseline):
    """Absence is not evidence. A report that names no contract is about the current one."""
    payload = _report(batch={"exists": False}, baseline=baseline)
    assert payload["voucher_unit_price_minor"] == 1000
    assert payload["approval_arithmetic"] == CURRENT_ARITHMETIC
    assert payload["default_voucher_unit_price_minor"] == 1000
    assert payload["default_product_contract_version"] == CURRENT.version


@pytest.mark.parametrize(
    ("batch", "baseline", "expected"),
    [
        # One batch: its own frozen amount decides, with or without a baseline.
        ({"exists": True, "voucher_unit_price_minor": 1500}, None, 1500),
        ({"exists": True, "voucher_unit_price_minor": 1000}, None, 1000),
        ({"exists": True, "voucher_unit_price_minor": 1500}, {"baseline_version": CURRENT.baseline_version}, 1500),
        # No batch: the contract the baseline NAMES, and only if it names one.
        ({"exists": False}, {"baseline_version": LEGACY_PRODUCTION_CONTRACT.baseline_version}, 1500),
        ({"exists": False}, {"baseline_version": CURRENT.baseline_version}, 1000),
    ],
)
def test_the_nominal_and_the_arithmetic_always_describe_the_same_subject(batch, baseline, expected):
    """A refusal report has no baseline on some paths; its sum must still be the batch's."""
    payload = _report(outcome="refused", stage="pay", batch=batch, baseline=baseline)
    assert payload["voucher_unit_price_minor"] == expected
    assert payload["approval_arithmetic"] == f"approved_exposure_minor = expected_recipient_count * {expected}"
    # And the default never moves with the subject: a €15 batch is not evidence
    # that a new mailing costs €15.
    assert payload["default_voucher_unit_price_minor"] == 1000
    assert payload["default_product_contract_version"] == CURRENT.version


async def test_status_on_an_empty_production_ledger_states_the_new_contract(session_maker):
    """The first thing an operator runs, before any batch exists."""
    payload = (await production_runner.run_status(session_maker)).as_safe_dict()
    assert payload["outcome"] == "not_started"
    assert payload["batches"] == []
    assert payload["voucher_unit_price_minor"] == 1000
    assert payload["approval_arithmetic"] == CURRENT_ARITHMETIC


async def test_status_keeps_each_version_its_own_amount_and_never_quotes_the_old_one_as_the_new(
    session_maker, production_configuration, binding_key
):
    """One historical batch, one new batch, and the mixed list of both."""
    historical_run, _ = await old.seed_production_preview(session_maker, count=1, offset=0)
    await new.seed_template_and_sender(session_maker)
    historical = await _freeze(
        session_maker, old.FakeReader(indices=[0]), old.production_request(run_id=historical_run), count=1
    )
    assert historical.outcome == "frozen", historical.reasons

    current_run, _ = await old.seed_production_preview(session_maker, count=1, offset=1)
    frozen = await _apply(
        session_maker,
        new.FakeReader(indices=[1]),
        stage="freeze",
        request=new.production_request(run_id=current_run),
        approval=new.approval_for(1),
    )
    assert frozen.outcome == "frozen", frozen.reasons

    old_id = int(historical.batch["batch_id"])
    new_id = int(frozen.batch["batch_id"])

    # A named historical batch keeps €15, in the nominal and in the arithmetic.
    historical_status = (await production_runner.run_status(session_maker, batch_id=old_id)).as_safe_dict()
    assert historical_status["voucher_unit_price_minor"] == 1500
    assert historical_status["approval_arithmetic"] == LEGACY_ARITHMETIC
    assert historical_status["batch"]["voucher_unit_price_minor"] == 1500
    assert historical_status["default_voucher_unit_price_minor"] == 1000

    # A named new batch is €10.
    current_status = (await production_runner.run_status(session_maker, batch_id=new_id)).as_safe_dict()
    assert current_status["voucher_unit_price_minor"] == 1000
    assert current_status["approval_arithmetic"] == CURRENT_ARITHMETIC

    # And the list of both: the headline per batch is that batch's own, while the
    # report's own nominal is the contract a NEW mailing would use.
    listing = (await production_runner.run_status(session_maker)).as_safe_dict()
    amounts = {int(entry["batch_id"]): entry["voucher_unit_price_minor"] for entry in listing["batches"]}
    assert amounts == {old_id: 1500, new_id: 1000}
    assert listing["voucher_unit_price_minor"] == 1000
    assert listing["approval_arithmetic"] == CURRENT_ARITHMETIC
    assert LEGACY_ARITHMETIC not in json.dumps(
        {key: value for key, value in listing.items() if key not in ("batch", "batches")}
    )
