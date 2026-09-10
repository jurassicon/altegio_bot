"""Domain matrix for the non-persistent EasyWeek voucher calculation contract.

Nothing here touches the network: the calculator is a recording fake, and the
reader replays the payload shapes observed on 10.09.2026.
"""

from __future__ import annotations

import inspect
from typing import Any

import pytest

from altegio_bot.campaigns import easyweek_voucher_contract as contract_module
from altegio_bot.campaigns.easyweek_voucher_contract import (
    GIFT_CARD_CALCULATION_AMOUNT_MISMATCH,
    GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE,
    GIFT_CARD_CALCULATION_CONTRACT_UNPROVEN,
    GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL,
    GIFT_CARD_CALCULATION_PRICE_UNPROVEN,
    GIFT_CARD_CALCULATION_REJECTED,
    GIFT_CARD_CALCULATION_RESPONSE_MALFORMED,
    GIFT_CARD_CALCULATION_RETRYABLE_UNCERTAINTY,
    GIFT_CARD_CALCULATION_TEMPLATE_UNPROVEN,
    GIFT_CARD_TEMPLATE_COUNTER_DRIFT,
    evaluate_calculation_invoice,
    evaluate_calculation_prerequisites,
    probe_voucher_calculation_contract,
)
from altegio_bot.easyweek_client import (
    EasyWeekAuthError,
    EasyWeekPermanentError,
    EasyWeekProtocolError,
    EasyWeekRetryableError,
)
from altegio_bot.easyweek_voucher_calculation import (
    EasyWeekCalculationUncertain,
    VoucherCalculationResult,
)
from altegio_bot.tests.easyweek_voucher_evidence_fixtures import (
    KARLSRUHE_UUID,
    LOCATIONS,
    PRICE_MINOR,
    TEMPLATE,
    TEMPLATE_UUID,
    WORKSPACE,
    arbitrary_price_response,
    canonical_response,
    code_without_docstrings,
    fully_discounted_response,
    zero_total_response,
)

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeReader:
    """The four reviewed GETs, with an optional different template afterwards."""

    def __init__(
        self,
        *,
        workspace: Any = None,
        locations: Any = None,
        templates: Any = None,
        template: Any = None,
        template_after: Any = None,
        raise_on_read: Exception | None = None,
    ) -> None:
        self.workspace = WORKSPACE if workspace is None else workspace
        self.locations = LOCATIONS if locations is None else locations
        self.templates = [TEMPLATE] if templates is None else templates
        self.template = TEMPLATE if template is None else template
        self.template_after = self.template if template_after is None else template_after
        self.raise_on_read = raise_on_read
        self.calls: list[str] = []
        self._template_reads = 0

    async def get_workspace(self) -> dict[str, Any]:
        self.calls.append("get_workspace")
        if self.raise_on_read is not None:
            raise self.raise_on_read
        return self.workspace

    async def list_locations(self) -> list[dict[str, Any]]:
        self.calls.append("list_locations")
        return self.locations

    async def list_voucher_templates(self) -> list[dict[str, Any]]:
        self.calls.append("list_voucher_templates")
        return self.templates

    async def get_voucher_template(self, voucher_template_uuid: str) -> dict[str, Any]:
        assert voucher_template_uuid == TEMPLATE_UUID
        self.calls.append("get_voucher_template")
        self._template_reads += 1
        return self.template if self._template_reads == 1 else self.template_after


class FakeCalculator:
    """Records every calculate call; there must never be more than one."""

    def __init__(self, *, response: Any = None, raises: Exception | None = None, http_status: int = 200) -> None:
        self.response = canonical_response() if response is None else response
        self.raises = raises
        self.http_status = http_status
        self.calls: list[dict[str, Any]] = []

    async def calculate_single_voucher(
        self,
        *,
        location_uuid: str,
        voucher_template_uuid: str,
        price_minor: int,
    ) -> VoucherCalculationResult:
        self.calls.append(
            {
                "location_uuid": location_uuid,
                "voucher_template_uuid": voucher_template_uuid,
                "price_minor": price_minor,
            }
        )
        if self.raises is not None:
            raise self.raises
        return VoucherCalculationResult(http_status=self.http_status, payload=self.response)


class RefusingCalculator:
    async def calculate_single_voucher(self, **kwargs: Any) -> VoucherCalculationResult:  # pragma: no cover
        raise AssertionError("no POST may happen when the prerequisites do not hold")


def _prerequisites(**template_changes: Any):
    return evaluate_calculation_prerequisites(
        workspace_payload=WORKSPACE,
        locations_payload=LOCATIONS,
        templates_payload=[{**TEMPLATE, **template_changes}],
        template_payload={**TEMPLATE, **template_changes},
    )


def _invoice(**invoice_changes: Any):
    return evaluate_calculation_invoice(
        http_status=200,
        payload=canonical_response(**invoice_changes),
        expected_price_minor=PRICE_MINOR,
    )


# ---------------------------------------------------------------------------
# The canonical, fully proven case
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_canonical_template_and_invoice_prove_the_calculation_contract() -> None:
    reader = FakeReader()
    calculator = FakeCalculator()

    evidence = await probe_voucher_calculation_contract(reader, calculator)

    assert evidence.calculation_contract_ready is True
    assert evidence.reasons == ()
    assert evidence.workspace_proven is True
    assert evidence.location_proven is True
    assert evidence.template_proven is True
    assert evidence.template_pristine is True
    assert evidence.template_counters_unchanged is True
    assert evidence.template_counters_before == {"vouchers_count": 0, "activated_vouchers_count": 0}
    assert evidence.template_counters_after == evidence.template_counters_before
    # The reads happen before the POST, and the exact template is read again
    # afterwards.
    assert reader.calls == [
        "get_workspace",
        "list_locations",
        "list_voucher_templates",
        "get_voucher_template",
        "get_voucher_template",
    ]
    assert len(calculator.calls) == 1


@pytest.mark.asyncio
async def test_a_proven_calculation_still_authorizes_nothing() -> None:
    evidence = await probe_voucher_calculation_contract(FakeReader(), FakeCalculator())

    assert evidence.calculation_contract_ready is True
    assert evidence.issue_contract_ready is False
    assert evidence.individual_voucher_artifact_proven is False
    assert evidence.customer_binding_proven is False
    assert evidence.write_idempotency_proven is False
    assert evidence.unknown_result_reconciliation_proven is False
    assert evidence.delivery_authorized is False
    assert evidence.ready_for_send is False

    safe = evidence.as_safe_dict()
    assert safe["mode"] == "nonpersistent_calculation_evidence"
    assert safe["issue_contract_ready"] is False
    assert safe["delivery_authorized"] is False
    assert safe["ready_for_send"] is False


@pytest.mark.asyncio
async def test_the_posted_price_comes_only_from_the_fresh_template() -> None:
    calculator = FakeCalculator()
    await probe_voucher_calculation_contract(FakeReader(), calculator)

    assert calculator.calls == [
        {
            "location_uuid": KARLSRUHE_UUID,
            "voucher_template_uuid": TEMPLATE_UUID,
            "price_minor": PRICE_MINOR,
        }
    ]


def test_the_evaluator_exposes_no_caller_supplied_price() -> None:
    signature = inspect.signature(probe_voucher_calculation_contract)
    assert set(signature.parameters) == {"reader", "calculator"}


# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "workspace",
    [
        {**WORKSPACE, "uuid": "00000000-0000-0000-0000-000000000000"},
        {**WORKSPACE, "slug": "another-workspace"},
        {**WORKSPACE, "currency": "USD"},
        {},
    ],
)
async def test_a_wrong_workspace_blocks_the_post(workspace) -> None:
    reader = FakeReader(workspace=workspace)
    evidence = await probe_voucher_calculation_contract(reader, RefusingCalculator())

    assert evidence.calculation_contract_ready is False
    assert GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE in evidence.reasons
    assert GIFT_CARD_CALCULATION_CONTRACT_UNPROVEN in evidence.reasons
    assert reader.calls.count("get_voucher_template") <= 1


@pytest.mark.asyncio
async def test_a_missing_karlsruhe_location_blocks_the_post() -> None:
    reader = FakeReader(locations=[{"uuid": "11111111-2222-4333-8444-555555555555", "name": "Other"}])
    evidence = await probe_voucher_calculation_contract(reader, RefusingCalculator())

    assert evidence.location_proven is False
    assert GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE in evidence.reasons


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "template_changes",
    [
        {"uuid": "11111111-2222-4333-8444-555555555555"},
        {"is_enabled": False},
        {"is_enabled": 1},
        {"is_single_charge": False},
        {"is_single_charge": None},
    ],
)
async def test_an_unproven_template_blocks_the_post(template_changes) -> None:
    template = {**TEMPLATE, **template_changes}
    reader = FakeReader(templates=[template], template=template)
    evidence = await probe_voucher_calculation_contract(reader, RefusingCalculator())

    assert evidence.calculation_contract_ready is False
    assert GIFT_CARD_CALCULATION_TEMPLATE_UNPROVEN in evidence.reasons


@pytest.mark.asyncio
async def test_an_unlisted_template_is_never_fetched_and_blocks_the_post() -> None:
    reader = FakeReader(templates=[])
    evidence = await probe_voucher_calculation_contract(reader, RefusingCalculator())

    assert "get_voucher_template" not in reader.calls
    assert GIFT_CARD_CALCULATION_TEMPLATE_UNPROVEN in evidence.reasons


@pytest.mark.parametrize(
    "money",
    [
        {"cost": None, "value": 1500},
        {"cost": 1500, "value": None},
        {"cost": True, "value": True},
        {"cost": 1500.0, "value": 1500.0},
        {"cost": "1500", "value": "1500"},
        {"cost": 1500, "value": 1000},
        {"cost": 1000, "value": 1000},
        {"cost": 2000, "value": 2000},
    ],
)
def test_only_an_exact_matching_fifteen_euro_nominal_yields_a_price(money) -> None:
    prerequisites = _prerequisites(**money)

    assert prerequisites.price_minor is None
    assert GIFT_CARD_CALCULATION_PRICE_UNPROVEN in prerequisites.reasons
    assert prerequisites.proven is False


def test_the_canonical_template_yields_exactly_the_template_cost() -> None:
    prerequisites = _prerequisites()

    assert prerequisites.proven is True
    assert prerequisites.price_minor == TEMPLATE["cost"]
    # No parameter here could supply a price: it comes from the fresh read.
    assert "price" not in inspect.signature(evaluate_calculation_prerequisites).parameters


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "counters",
    [
        {"vouchers_count": None},
        {"vouchers_count": True},
        {"activated_vouchers_count": "0"},
        {"activated_vouchers_count": 0.0},
    ],
)
async def test_unreadable_counters_block_the_post(counters) -> None:
    template = {**TEMPLATE, **counters}
    reader = FakeReader(templates=[template], template=template)
    evidence = await probe_voucher_calculation_contract(reader, RefusingCalculator())

    assert evidence.template_counters_before is None
    assert evidence.template_pristine is False
    assert GIFT_CARD_TEMPLATE_COUNTER_DRIFT in evidence.reasons


def test_a_non_pristine_template_is_reported_not_hardcoded_as_a_contract() -> None:
    prerequisites = _prerequisites(vouchers_count=7, activated_vouchers_count=3)

    # Zero is evidence about today, not a permanent product rule: a sold
    # voucher must not by itself invalidate the calculation contract.
    assert prerequisites.template_pristine is False
    assert prerequisites.proven is True
    assert prerequisites.counters_before == {"vouchers_count": 7, "activated_vouchers_count": 3}


# ---------------------------------------------------------------------------
# Counter drift after the POST
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "after",
    [
        {**TEMPLATE, "vouchers_count": 1},
        {**TEMPLATE, "activated_vouchers_count": 1},
        {**TEMPLATE, "vouchers_count": None},
    ],
)
async def test_counter_drift_after_the_post_fails_closed(after) -> None:
    reader = FakeReader(template_after=after)
    calculator = FakeCalculator()

    evidence = await probe_voucher_calculation_contract(reader, calculator)

    assert evidence.calculation_contract_ready is False
    assert evidence.template_counters_unchanged is False
    assert GIFT_CARD_TEMPLATE_COUNTER_DRIFT in evidence.reasons
    # Still exactly one POST.
    assert len(calculator.calls) == 1


@pytest.mark.asyncio
async def test_the_template_is_reread_even_when_the_post_failed() -> None:
    reader = FakeReader()
    calculator = FakeCalculator(raises=EasyWeekCalculationUncertain("unknown", attempts=1))

    evidence = await probe_voucher_calculation_contract(reader, calculator)

    assert reader.calls.count("get_voucher_template") == 2
    assert evidence.template_counters_after == {"vouchers_count": 0, "activated_vouchers_count": 0}
    assert GIFT_CARD_CALCULATION_RETRYABLE_UNCERTAINTY in evidence.reasons


# ---------------------------------------------------------------------------
# Transport failures
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,expected_reason",
    [
        (EasyWeekCalculationUncertain("unknown", attempts=1), GIFT_CARD_CALCULATION_RETRYABLE_UNCERTAINTY),
        (EasyWeekRetryableError("retryable", attempts=1), GIFT_CARD_CALCULATION_RETRYABLE_UNCERTAINTY),
        (EasyWeekAuthError("auth", status_code=401), GIFT_CARD_CALCULATION_REJECTED),
        (EasyWeekPermanentError("rejected", status_code=422), GIFT_CARD_CALCULATION_REJECTED),
        (EasyWeekProtocolError("malformed", status_code=200), GIFT_CARD_CALCULATION_RESPONSE_MALFORMED),
    ],
)
async def test_a_failed_post_maps_to_one_stable_reason(error, expected_reason) -> None:
    calculator = FakeCalculator(raises=error)
    evidence = await probe_voucher_calculation_contract(FakeReader(), calculator)

    assert evidence.calculation_contract_ready is False
    assert expected_reason in evidence.reasons
    assert len(calculator.calls) == 1


@pytest.mark.asyncio
async def test_a_failed_prerequisite_read_never_posts() -> None:
    reader = FakeReader(raise_on_read=EasyWeekRetryableError("down", attempts=3))
    evidence = await probe_voucher_calculation_contract(reader, RefusingCalculator())

    assert evidence.calculation_contract_ready is False
    assert GIFT_CARD_CALCULATION_RETRYABLE_UNCERTAINTY in evidence.reasons
    assert evidence.template_counters_before is None


@pytest.mark.asyncio
async def test_a_permanently_failed_prerequisite_read_is_a_configuration_reason() -> None:
    reader = FakeReader(raise_on_read=EasyWeekAuthError("auth", status_code=403))
    evidence = await probe_voucher_calculation_contract(reader, RefusingCalculator())

    assert GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE in evidence.reasons


# ---------------------------------------------------------------------------
# Strict invoice projection
# ---------------------------------------------------------------------------


def test_the_canonical_invoice_is_proven() -> None:
    projection = _invoice()

    assert projection.proven is True
    assert projection.reasons == ()
    assert projection.account_paid_amount_observed == -1500


@pytest.mark.parametrize("status", [201, 202, 204, 302])
def test_only_http_200_can_prove_the_contract(status) -> None:
    projection = evaluate_calculation_invoice(
        http_status=status,
        payload=canonical_response(),
        expected_price_minor=PRICE_MINOR,
    )
    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_RESPONSE_MALFORMED in projection.reasons


@pytest.mark.parametrize(
    "payload",
    [
        None,
        "ok",
        [1, 2, 3],
        {},
        {"invoice": None},
        {"invoice": []},
        {"invoice": "1500"},
    ],
)
def test_a_missing_or_non_object_invoice_is_malformed(payload) -> None:
    projection = evaluate_calculation_invoice(
        http_status=200,
        payload=payload,
        expected_price_minor=PRICE_MINOR,
    )
    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_RESPONSE_MALFORMED in projection.reasons


def test_a_data_object_envelope_is_accepted() -> None:
    projection = evaluate_calculation_invoice(
        http_status=200,
        payload={"data": canonical_response()},
        expected_price_minor=PRICE_MINOR,
    )
    assert projection.proven is True


@pytest.mark.parametrize("field", ["order_uuid", "status"])
def test_a_missing_persistence_field_is_malformed(field) -> None:
    invoice = dict(canonical_response()["invoice"])
    del invoice[field]
    projection = evaluate_calculation_invoice(
        http_status=200,
        payload={"invoice": invoice},
        expected_price_minor=PRICE_MINOR,
    )
    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_RESPONSE_MALFORMED in projection.reasons


@pytest.mark.parametrize(
    "changes",
    [
        {"order_uuid": "11111111-2222-4333-8444-555555555555"},
        {"order_uuid": ""},
        {"status": "draft"},
        {"status": 0},
        {"status": False},
    ],
)
def test_any_non_null_order_identity_is_a_persistence_signal(changes) -> None:
    projection = _invoice(**changes)

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL in projection.reasons


@pytest.mark.parametrize(
    "field",
    [
        "base_amount",
        "base_price",
        "subtotal",
        "total",
        "amount_due",
        "discount_amount",
        "amount_paid",
        "voucher_paid_amount",
    ],
)
@pytest.mark.parametrize("bad", [None, True, False, "1500", 1500.0, {"amount": 1500}])
def test_every_monetary_field_must_be_an_exact_integer(field, bad) -> None:
    projection = _invoice(**{field: bad})

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_RESPONSE_MALFORMED in projection.reasons


@pytest.mark.parametrize("field", ["base_amount", "base_price", "subtotal", "total", "amount_due"])
def test_every_priced_field_must_equal_the_price_we_sent(field) -> None:
    projection = _invoice(**{field: 1499})

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_AMOUNT_MISMATCH in projection.reasons


@pytest.mark.parametrize("field", ["discount_amount", "amount_paid", "voucher_paid_amount"])
@pytest.mark.parametrize("amount", [1, -1, 1500, -1500])
def test_a_nonzero_discount_or_paid_amount_is_never_supported(field, amount) -> None:
    projection = _invoice(**{field: amount})

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_AMOUNT_MISMATCH in projection.reasons


@pytest.mark.parametrize("observed", [-1500, 0, 1500])
def test_account_paid_amount_is_observed_but_never_decides_readiness(observed) -> None:
    projection = _invoice(account_paid_amount=observed)

    assert projection.proven is True
    assert projection.account_paid_amount_observed == observed


@pytest.mark.parametrize("observed", [None, "−1500", -1500.0, True])
def test_a_non_integer_account_paid_amount_is_simply_not_reported(observed) -> None:
    projection = _invoice(account_paid_amount=observed)

    assert projection.proven is True
    assert projection.account_paid_amount_observed is None


@pytest.mark.asyncio
async def test_extra_response_fields_never_reach_the_safe_report() -> None:
    response = canonical_response()
    response["invoice"]["comment"] = "SENTINEL_NOTE_bbb111"
    response["customer"] = {"name": "SENTINEL_NAME_bbb222"}
    response["voucher_code"] = "SENTINEL_CODE_bbb333"
    response["public_url"] = "https://example.invalid/SENTINEL_URL_bbb444"

    evidence = await probe_voucher_calculation_contract(FakeReader(), FakeCalculator(response=response))

    assert evidence.calculation_contract_ready is True
    printed = repr(evidence.as_safe_dict())
    for sentinel in ("SENTINEL_NOTE_bbb111", "SENTINEL_NAME_bbb222", "SENTINEL_CODE_bbb333", "SENTINEL_URL_bbb444"):
        assert sentinel not in printed
    assert set(evidence.as_safe_dict()) <= {
        "mode",
        "workspace_proven",
        "location_proven",
        "template_proven",
        "template_pristine",
        "template_counters_before",
        "template_counters_after",
        "template_counters_unchanged",
        "calculation_contract_ready",
        "issue_contract_ready",
        "individual_voucher_artifact_proven",
        "customer_binding_proven",
        "write_idempotency_proven",
        "unknown_result_reconciliation_proven",
        "delivery_authorized",
        "ready_for_send",
        "account_paid_amount_observed",
        "reasons",
    }


# ---------------------------------------------------------------------------
# Empirical boundary regressions (live matrix cases 2-5)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_missing_price_rejection_names_only_the_field_and_proves_nothing() -> None:
    # Matrix case 2: the API answered 422 on `vouchers.0.price`.
    rejection = EasyWeekPermanentError(
        "calculation rejected as invalid: price,vouchers",
        operation="calculate_voucher_order",
        status_code=422,
        attempts=1,
    )
    reader = FakeReader()
    evidence = await probe_voucher_calculation_contract(reader, FakeCalculator(raises=rejection))

    assert evidence.calculation_contract_ready is False
    assert GIFT_CARD_CALCULATION_REJECTED in evidence.reasons
    assert evidence.template_counters_unchanged is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response_factory",
    [zero_total_response, arbitrary_price_response, fully_discounted_response],
)
async def test_zero_arbitrary_and_discounted_invoices_are_never_supported(response_factory) -> None:
    calculator = FakeCalculator(response=response_factory())
    evidence = await probe_voucher_calculation_contract(FakeReader(), calculator)

    assert evidence.calculation_contract_ready is False
    assert GIFT_CARD_CALCULATION_AMOUNT_MISMATCH in evidence.reasons
    assert GIFT_CARD_CALCULATION_CONTRACT_UNPROVEN in evidence.reasons
    # Not even a happily calculated zero total moves any of these.
    assert evidence.issue_contract_ready is False
    assert evidence.delivery_authorized is False
    assert evidence.ready_for_send is False


def test_a_zero_total_calculation_alone_is_not_a_free_voucher() -> None:
    projection = evaluate_calculation_invoice(
        http_status=200,
        payload=fully_discounted_response(),
        expected_price_minor=PRICE_MINOR,
    )
    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_AMOUNT_MISMATCH in projection.reasons


# ---------------------------------------------------------------------------
# Hygiene
# ---------------------------------------------------------------------------


def test_the_module_names_no_customer_or_discount_parameter() -> None:
    code = code_without_docstrings(contract_module)
    for forbidden in ("customer_uuid", "promocode", "Idempotency-Key"):
        assert forbidden not in code, forbidden


@pytest.mark.parametrize(
    "reason",
    [
        GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE,
        GIFT_CARD_CALCULATION_TEMPLATE_UNPROVEN,
        GIFT_CARD_CALCULATION_PRICE_UNPROVEN,
        GIFT_CARD_CALCULATION_REJECTED,
        GIFT_CARD_CALCULATION_RETRYABLE_UNCERTAINTY,
        GIFT_CARD_CALCULATION_RESPONSE_MALFORMED,
        GIFT_CARD_CALCULATION_AMOUNT_MISMATCH,
        GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL,
        GIFT_CARD_TEMPLATE_COUNTER_DRIFT,
        GIFT_CARD_CALCULATION_CONTRACT_UNPROVEN,
    ],
)
def test_every_reason_is_a_stable_pii_free_slug(reason) -> None:
    assert reason == reason.lower()
    assert reason.replace("_", "").isalnum()
    assert "-" not in reason and " " not in reason
