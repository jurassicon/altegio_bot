"""Domain matrix for the non-persistent EasyWeek voucher calculation contract.

Nothing here touches the network: the calculator is a recording fake, and the
reader replays the payload shapes observed on 10.09.2026.

The matrix is deliberately negative-heavy. Every case that is not the one proven
canonical response must fail closed, and "fail closed" includes shapes that look
harmless — an unexplained extra field, a null invoice-level order id next to a
non-null one on the envelope, a voucher collection that is not empty, a template
that changed underneath us while the POST was in flight.
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
    GIFT_CARD_CALCULATION_TEMPLATE_UNPROVEN,
    GIFT_CARD_CALCULATION_UNCERTAIN,
    GIFT_CARD_TEMPLATE_COUNTER_DRIFT,
    GIFT_CARD_TEMPLATE_STATE_DRIFT,
    GIFT_CARD_TEMPLATE_VERIFICATION_UNCERTAIN,
    UNCERTAIN_REASONS,
    evaluate_calculation_invoice,
    evaluate_calculation_prerequisites,
    probe_voucher_calculation_contract,
    template_counters,
)
from altegio_bot.easyweek_client import (
    EasyWeekAuthError,
    EasyWeekNotFoundError,
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

# Obviously synthetic; used only to prove a refusal.
FOREIGN_UUID = "11111111-2222-4333-8444-555555555555"
ARTIFACT_MARKER = "SENTINEL_ARTIFACT_bbb111"

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
        raise_on_second_template: Exception | None = None,
    ) -> None:
        self.workspace = WORKSPACE if workspace is None else workspace
        self.locations = LOCATIONS if locations is None else locations
        self.templates = [TEMPLATE] if templates is None else templates
        self.template = TEMPLATE if template is None else template
        self.template_after = self.template if template_after is None else template_after
        self.raise_on_read = raise_on_read
        self.raise_on_second_template = raise_on_second_template
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
        if self._template_reads == 1:
            return self.template
        if self.raise_on_second_template is not None:
            raise self.raise_on_second_template
        return self.template_after


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
        return VoucherCalculationResult(http_status=self.http_status, envelope=self.response)


class RefusingCalculator:
    async def calculate_single_voucher(self, **kwargs: Any) -> VoucherCalculationResult:  # pragma: no cover
        raise AssertionError("no POST may happen when the prerequisites do not hold")


def _prerequisites(**template_changes: Any):
    template = {**TEMPLATE, **template_changes}
    return evaluate_calculation_prerequisites(
        workspace_payload=WORKSPACE,
        locations_payload=LOCATIONS,
        templates_payload=[template],
        template_payload=template,
    )


def _invoice(**invoice_changes: Any):
    return evaluate_calculation_invoice(
        http_status=200,
        envelope=canonical_response(**invoice_changes),
        expected_price_minor=PRICE_MINOR,
    )


def _envelope(envelope: Any):
    return evaluate_calculation_invoice(
        http_status=200,
        envelope=envelope,
        expected_price_minor=PRICE_MINOR,
    )


async def _probe(**reader_kwargs: Any):
    """Run the probe with a calculator that refuses to be called at all."""
    reader = FakeReader(**reader_kwargs)
    return await probe_voucher_calculation_contract(reader, RefusingCalculator()), reader


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
    assert evidence.template_state_unchanged is True
    assert evidence.uncertain is False
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


@pytest.mark.asyncio
async def test_the_safe_report_has_exactly_the_expected_keys() -> None:
    evidence = await probe_voucher_calculation_contract(FakeReader(), FakeCalculator())

    assert set(evidence.as_safe_dict()) == {
        "mode",
        "workspace_proven",
        "location_proven",
        "template_proven",
        "template_pristine",
        "template_counters_before",
        "template_counters_after",
        "template_counters_unchanged",
        "template_state_unchanged",
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
# Prerequisites: workspace and branch identity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "workspace",
    [
        {**WORKSPACE, "uuid": FOREIGN_UUID},
        {**WORKSPACE, "slug": "another-workspace"},
        {**WORKSPACE, "currency": "USD"},
        {},
    ],
)
async def test_a_wrong_workspace_blocks_the_post(workspace) -> None:
    evidence, reader = await _probe(workspace=workspace)

    assert evidence.calculation_contract_ready is False
    assert GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE in evidence.reasons
    assert GIFT_CARD_CALCULATION_CONTRACT_UNPROVEN in evidence.reasons
    assert reader.calls.count("get_voucher_template") <= 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "locations",
    [
        [{"uuid": FOREIGN_UUID, "name": "Other"}],
        [],
        # Listed twice: an ambiguous identity is not a proven one.
        [*LOCATIONS, {"uuid": KARLSRUHE_UUID, "name": "KitiLash Karlsruhe (copy)"}],
    ],
)
async def test_karlsruhe_must_appear_exactly_once(locations) -> None:
    evidence, _ = await _probe(locations=locations)

    assert evidence.location_proven is False
    assert GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE in evidence.reasons


@pytest.mark.asyncio
async def test_a_template_listed_twice_is_never_fetched_or_posted() -> None:
    evidence, reader = await _probe(templates=[TEMPLATE, dict(TEMPLATE)])

    assert "get_voucher_template" not in reader.calls
    assert GIFT_CARD_CALCULATION_TEMPLATE_UNPROVEN in evidence.reasons


@pytest.mark.asyncio
async def test_an_unlisted_template_is_never_fetched_and_blocks_the_post() -> None:
    evidence, reader = await _probe(templates=[])

    assert "get_voucher_template" not in reader.calls
    assert GIFT_CARD_CALCULATION_TEMPLATE_UNPROVEN in evidence.reasons


# ---------------------------------------------------------------------------
# Prerequisites: template identity, branch applicability and price
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "template_changes",
    [
        {"uuid": FOREIGN_UUID},
        {"is_enabled": False},
        {"is_enabled": 1},
        {"is_enabled": None},
        {"is_single_charge": False},
        {"is_single_charge": None},
    ],
)
async def test_an_unproven_template_identity_blocks_the_post(template_changes) -> None:
    template = {**TEMPLATE, **template_changes}
    evidence, _ = await _probe(templates=[template], template=template)

    assert evidence.calculation_contract_ready is False
    assert GIFT_CARD_CALCULATION_TEMPLATE_UNPROVEN in evidence.reasons


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "branch_changes",
    [
        # Branch applicability was never proven at all.
        {"is_connected_all_branches": False},
        {"is_connected_all_branches": None},
        {"is_connected_all_branches": 1},
        # Counts missing.
        {"branches_count": None},
        {"all_branches_count": None},
        # Counts of the wrong type.
        {"branches_count": True},
        {"branches_count": "3"},
        {"branches_count": 3.0},
        {"all_branches_count": True},
        # Counts that do not agree, or are impossible.
        {"branches_count": 2},
        {"all_branches_count": 4},
        {"branches_count": -1, "all_branches_count": -1},
        {"all_branches_count": 0, "branches_count": 0},
    ],
)
async def test_unproven_branch_applicability_blocks_the_post(branch_changes) -> None:
    """Karlsruhe being in /locations says nothing about this product reaching it."""
    template = {**TEMPLATE, **branch_changes}
    evidence, reader = await _probe(templates=[template], template=template)

    assert evidence.calculation_contract_ready is False
    assert evidence.template_proven is False
    assert GIFT_CARD_CALCULATION_TEMPLATE_UNPROVEN in evidence.reasons
    # No POST happened, so the template was read exactly once.
    assert reader.calls.count("get_voucher_template") == 1


def test_all_branches_is_not_claimed_as_karlsruhe_only() -> None:
    """The proven fact is all-branches-including-Karlsruhe, and nothing more."""
    code = code_without_docstrings(contract_module)
    assert "karlsruhe_only" not in code.casefold()


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
        {"cost": 1499, "value": 1499},
        {"cost": 0, "value": 0},
        {"cost": -1500, "value": -1500},
    ],
)
def test_only_an_exact_matching_fifteen_euro_nominal_yields_a_price(money) -> None:
    prerequisites = _prerequisites(**money)

    assert prerequisites.price_minor is None
    assert prerequisites.proven is False
    assert (
        GIFT_CARD_CALCULATION_PRICE_UNPROVEN in prerequisites.reasons
        or GIFT_CARD_CALCULATION_TEMPLATE_UNPROVEN in prerequisites.reasons
    )


def test_the_canonical_template_yields_exactly_the_template_cost() -> None:
    prerequisites = _prerequisites()

    assert prerequisites.proven is True
    assert prerequisites.price_minor == TEMPLATE["cost"]
    # No parameter here could supply a price: it comes from the fresh read.
    assert "price" not in inspect.signature(evaluate_calculation_prerequisites).parameters


# ---------------------------------------------------------------------------
# Prerequisites: counters
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "counters",
    [
        {"vouchers_count": None},
        {"vouchers_count": True},
        {"activated_vouchers_count": "0"},
        {"activated_vouchers_count": 0.0},
        {"vouchers_count": -1},
        {"activated_vouchers_count": -1},
        # Logically impossible: more activated than ever issued.
        {"vouchers_count": 1, "activated_vouchers_count": 2},
        {"vouchers_count": 0, "activated_vouchers_count": 1},
    ],
)
async def test_unusable_counters_block_the_post_with_zero_transport_calls(counters) -> None:
    template = {**TEMPLATE, **counters}
    calculator = FakeCalculator()
    reader = FakeReader(templates=[template], template=template)

    evidence = await probe_voucher_calculation_contract(reader, calculator)

    assert evidence.template_counters_before is None
    assert evidence.template_pristine is False
    assert GIFT_CARD_TEMPLATE_COUNTER_DRIFT in evidence.reasons
    # The whole point: not one request left the process.
    assert calculator.calls == []


@pytest.mark.parametrize(
    "counters",
    [
        {"vouchers_count": 0, "activated_vouchers_count": 0},
        {"vouchers_count": 7, "activated_vouchers_count": 3},
        {"vouchers_count": 3, "activated_vouchers_count": 3},
    ],
)
def test_a_possible_counter_pair_is_accepted(counters) -> None:
    assert template_counters({**TEMPLATE, **counters}) == counters


def test_a_non_pristine_template_is_reported_not_hardcoded_as_a_contract() -> None:
    prerequisites = _prerequisites(vouchers_count=7, activated_vouchers_count=3)

    # Zero is evidence about today, not a permanent product rule: a sold
    # voucher must not by itself invalidate the calculation contract.
    assert prerequisites.template_pristine is False
    assert prerequisites.proven is True
    assert prerequisites.counters_before == {"vouchers_count": 7, "activated_vouchers_count": 3}


# ---------------------------------------------------------------------------
# The post-POST template re-check
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "after_changes",
    [
        {"vouchers_count": 1},
        {"activated_vouchers_count": 1},
        {"vouchers_count": None},
        {"vouchers_count": 0, "activated_vouchers_count": 1},
    ],
)
async def test_counter_drift_after_the_post_fails_closed(after_changes) -> None:
    reader = FakeReader(template_after={**TEMPLATE, **after_changes})
    calculator = FakeCalculator()

    evidence = await probe_voucher_calculation_contract(reader, calculator)

    assert evidence.calculation_contract_ready is False
    assert evidence.template_counters_unchanged is False
    assert GIFT_CARD_TEMPLATE_COUNTER_DRIFT in evidence.reasons
    assert evidence.uncertain is False
    # Still exactly one POST.
    assert len(calculator.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "after_changes",
    [
        {"cost": 1600},
        {"value": 1600},
        {"cost": 1500, "value": 1400},
        {"is_enabled": False},
        {"is_single_charge": False},
        {"is_connected_all_branches": False},
        {"branches_count": 2},
        {"all_branches_count": 4},
        {"uuid": FOREIGN_UUID},
    ],
)
async def test_any_normative_template_drift_after_the_post_fails_closed(after_changes) -> None:
    """A template that moved under the POST invalidates the whole observation."""
    reader = FakeReader(template_after={**TEMPLATE, **after_changes})
    calculator = FakeCalculator()

    evidence = await probe_voucher_calculation_contract(reader, calculator)

    assert evidence.calculation_contract_ready is False
    assert evidence.template_state_unchanged is False
    assert GIFT_CARD_TEMPLATE_STATE_DRIFT in evidence.reasons
    assert evidence.uncertain is False
    assert len(calculator.calls) == 1


@pytest.mark.asyncio
async def test_the_template_is_reread_even_when_the_post_failed() -> None:
    reader = FakeReader()
    calculator = FakeCalculator(raises=EasyWeekCalculationUncertain("unknown", attempts=1))

    evidence = await probe_voucher_calculation_contract(reader, calculator)

    assert reader.calls.count("get_voucher_template") == 2
    assert evidence.template_counters_after == {"vouchers_count": 0, "activated_vouchers_count": 0}
    assert GIFT_CARD_CALCULATION_UNCERTAIN in evidence.reasons


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        EasyWeekCalculationUncertain("unknown", attempts=1),
        EasyWeekRetryableError("server down", attempts=3),
    ],
)
async def test_an_unanswered_verification_is_unknown_not_drift(failure) -> None:
    """A 5xx on the re-read means we did not look, not that a counter moved."""
    reader = FakeReader(raise_on_second_template=failure)
    calculator = FakeCalculator()

    evidence = await probe_voucher_calculation_contract(reader, calculator)

    assert evidence.calculation_contract_ready is False
    assert GIFT_CARD_TEMPLATE_VERIFICATION_UNCERTAIN in evidence.reasons
    assert evidence.uncertain is True
    # Not reported as drift: nothing was observed to have changed.
    assert GIFT_CARD_TEMPLATE_COUNTER_DRIFT not in evidence.reasons
    assert GIFT_CARD_TEMPLATE_STATE_DRIFT not in evidence.reasons
    assert evidence.template_counters_after is None
    assert len(calculator.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        EasyWeekAuthError("auth", status_code=403),
        EasyWeekNotFoundError("gone", status_code=404),
        EasyWeekPermanentError("bad", status_code=400),
    ],
)
async def test_a_permanently_failed_verification_is_a_configuration_mismatch(failure) -> None:
    reader = FakeReader(raise_on_second_template=failure)
    calculator = FakeCalculator()

    evidence = await probe_voucher_calculation_contract(reader, calculator)

    assert evidence.calculation_contract_ready is False
    assert GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE in evidence.reasons
    assert evidence.uncertain is False
    assert GIFT_CARD_TEMPLATE_VERIFICATION_UNCERTAIN not in evidence.reasons


@pytest.mark.asyncio
async def test_a_verification_returning_an_unusable_template_fails_closed() -> None:
    reader = FakeReader(template_after={"uuid": TEMPLATE_UUID})
    calculator = FakeCalculator()

    evidence = await probe_voucher_calculation_contract(reader, calculator)

    assert evidence.calculation_contract_ready is False
    assert GIFT_CARD_TEMPLATE_STATE_DRIFT in evidence.reasons
    assert GIFT_CARD_TEMPLATE_COUNTER_DRIFT in evidence.reasons


# ---------------------------------------------------------------------------
# POST failures
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,expected_reason",
    [
        (EasyWeekCalculationUncertain("unknown", attempts=1), GIFT_CARD_CALCULATION_UNCERTAIN),
        (EasyWeekRetryableError("retryable", attempts=1), GIFT_CARD_CALCULATION_UNCERTAIN),
        (EasyWeekAuthError("auth", status_code=401), GIFT_CARD_CALCULATION_REJECTED),
        (EasyWeekPermanentError("rejected", status_code=422), GIFT_CARD_CALCULATION_REJECTED),
        # A refused redirect arrives as a permanent 3xx rejection.
        (EasyWeekPermanentError("redirect", status_code=307), GIFT_CARD_CALCULATION_REJECTED),
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
    assert GIFT_CARD_CALCULATION_UNCERTAIN in evidence.reasons
    assert evidence.uncertain is True
    assert evidence.template_counters_before is None


@pytest.mark.asyncio
async def test_a_permanently_failed_prerequisite_read_is_a_configuration_reason() -> None:
    reader = FakeReader(raise_on_read=EasyWeekAuthError("auth", status_code=403))
    evidence = await probe_voucher_calculation_contract(reader, RefusingCalculator())

    assert GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE in evidence.reasons
    assert evidence.uncertain is False


# ---------------------------------------------------------------------------
# Strict invoice projection: shape
# ---------------------------------------------------------------------------


def test_the_canonical_invoice_is_proven() -> None:
    projection = _invoice()

    assert projection.proven is True
    assert projection.reasons == ()
    assert projection.account_paid_amount_observed == -1500


@pytest.mark.parametrize("status", [201, 202, 204, 302, 307])
def test_only_http_200_can_prove_the_contract(status) -> None:
    projection = evaluate_calculation_invoice(
        http_status=status,
        envelope=canonical_response(),
        expected_price_minor=PRICE_MINOR,
    )
    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_RESPONSE_MALFORMED in projection.reasons


@pytest.mark.parametrize(
    "envelope",
    [
        None,
        "ok",
        [1, 2, 3],
        {},
        {"invoice": None},
        {"invoice": []},
        {"invoice": "1500"},
        {"data": {"invoice": None}},
    ],
)
def test_a_missing_or_non_object_invoice_is_malformed(envelope) -> None:
    projection = _envelope(envelope)

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_RESPONSE_MALFORMED in projection.reasons


def test_a_data_object_envelope_is_accepted() -> None:
    projection = _envelope({"data": canonical_response()})
    assert projection.proven is True


def test_root_and_data_invoices_are_an_ambiguous_malformed_response() -> None:
    """A clean root invoice must not hide a persistence signal in data.invoice."""
    projection = _envelope(
        {
            **canonical_response(),
            "data": canonical_response(status="open", voucher_code=ARTIFACT_MARKER),
        }
    )

    assert projection.proven is False
    assert projection.reasons == (GIFT_CARD_CALCULATION_RESPONSE_MALFORMED,)


@pytest.mark.parametrize(
    "envelope",
    [
        {**canonical_response(), "data": "unexpected"},
        {"invoice": "unexpected", "data": canonical_response()},
    ],
)
def test_an_invalid_container_cannot_be_hidden_by_a_valid_invoice(envelope) -> None:
    projection = _envelope(envelope)

    assert projection.proven is False
    assert projection.reasons == (GIFT_CARD_CALCULATION_RESPONSE_MALFORMED,)


# ---------------------------------------------------------------------------
# Persistence identity, at every level
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["order_uuid", "status"])
def test_a_missing_persistence_field_is_malformed(name) -> None:
    invoice = dict(canonical_response()["invoice"])
    del invoice[name]
    projection = _envelope({"invoice": invoice})

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_RESPONSE_MALFORMED in projection.reasons


@pytest.mark.parametrize(
    "changes",
    [
        {"order_uuid": FOREIGN_UUID},
        {"order_uuid": ""},
        {"order_uuid": 0},
        {"status": "draft"},
        {"status": "open"},
        {"status": 0},
        {"status": False},
    ],
)
def test_any_non_null_order_identity_inside_the_invoice_is_a_persistence_signal(changes) -> None:
    projection = _invoice(**changes)

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL in projection.reasons


def test_a_root_order_uuid_beats_a_null_invoice_order_uuid() -> None:
    """The exact escape this fix closes: the outer level used to be discarded."""
    projection = _envelope({**canonical_response(), "order_uuid": FOREIGN_UUID, "status": None})

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL in projection.reasons


def test_a_root_status_beats_a_null_invoice_status() -> None:
    projection = _envelope({**canonical_response(), "status": "open", "order_uuid": None})

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL in projection.reasons


def test_a_root_identity_outside_a_canonical_data_object_is_still_seen() -> None:
    projection = _envelope(
        {
            "order_uuid": FOREIGN_UUID,
            "status": "open",
            "data": canonical_response(),
        }
    )

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL in projection.reasons


@pytest.mark.parametrize(
    "outer,inner",
    [
        ({"order_uuid": FOREIGN_UUID}, {"order_uuid": None}),
        ({"order_uuid": None}, {"order_uuid": FOREIGN_UUID}),
        ({"status": "open"}, {"status": None}),
        ({"status": None}, {"status": "open"}),
    ],
)
def test_conflicting_levels_are_a_persistence_signal(outer, inner) -> None:
    projection = _envelope({**outer, "data": canonical_response(**inner)})

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL in projection.reasons


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "envelope_factory",
    [
        lambda: {**canonical_response(), "order_uuid": FOREIGN_UUID, "status": None},
        lambda: {**canonical_response(), "status": "open", "order_uuid": None},
        lambda: {"order_uuid": FOREIGN_UUID, "status": "open", "data": canonical_response()},
        lambda: {"order_uuid": None, "data": canonical_response(order_uuid=FOREIGN_UUID)},
    ],
)
async def test_no_outer_persistence_signal_can_produce_a_green_probe(envelope_factory) -> None:
    calculator = FakeCalculator(response=envelope_factory())
    evidence = await probe_voucher_calculation_contract(FakeReader(), calculator)

    assert evidence.calculation_contract_ready is False
    assert GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL in evidence.reasons
    assert evidence.issue_contract_ready is False
    assert evidence.delivery_authorized is False
    assert evidence.ready_for_send is False


# ---------------------------------------------------------------------------
# Voucher artifacts and unexplained fields
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "artifact",
    [
        {"voucher_code": ARTIFACT_MARKER},
        {"code": ARTIFACT_MARKER},
        {"public_url": "https://example.invalid/" + ARTIFACT_MARKER},
        {"public_purchase_url": "https://example.invalid/" + ARTIFACT_MARKER},
        {"customer_url": "https://example.invalid/" + ARTIFACT_MARKER},
        {"url": "https://example.invalid/" + ARTIFACT_MARKER},
        {"voucher_uuid": FOREIGN_UUID},
        {"voucher": {"uuid": FOREIGN_UUID}},
        {"vouchers": [{"uuid": FOREIGN_UUID}]},
        {"customer": {"name": ARTIFACT_MARKER}},
        {"customer_uuid": FOREIGN_UUID},
    ],
)
def test_a_voucher_artifact_at_the_envelope_level_is_a_persistence_signal(artifact) -> None:
    projection = _envelope({**canonical_response(), **artifact})

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL in projection.reasons
    # Its value never reaches a reason.
    assert not any(ARTIFACT_MARKER in reason or FOREIGN_UUID in reason for reason in projection.reasons)


@pytest.mark.parametrize(
    "artifact",
    [
        {"voucher_code": ARTIFACT_MARKER},
        {"code": ARTIFACT_MARKER},
        {"vouchers": [{"code": ARTIFACT_MARKER}]},
        {"customer": {"uuid": FOREIGN_UUID}},
    ],
)
def test_a_voucher_artifact_inside_the_invoice_is_a_persistence_signal(artifact) -> None:
    projection = _invoice(**artifact)

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL in projection.reasons


@pytest.mark.parametrize(
    "empty_slot",
    [
        {"vouchers": []},
        {"voucher": None},
        {"customer": None},
        {"code": ""},
        {"public_url": ""},
    ],
)
def test_an_empty_artifact_slot_is_a_shape_not_an_artifact(empty_slot) -> None:
    """A described-but-empty slot is the API showing a shape, not handing one over."""
    projection = _envelope({**canonical_response(), **empty_slot})

    assert projection.proven is True


@pytest.mark.parametrize(
    "unexplained",
    [
        {"surprise": 1},
        {"meta": {"page": 1}},
        {"loyalty_points": 10},
        {"tips": []},
        {"unknown_total": 1500},
    ],
)
def test_an_unexplained_envelope_field_is_malformed_not_green(unexplained) -> None:
    projection = _envelope({**canonical_response(), **unexplained})

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_RESPONSE_MALFORMED in projection.reasons


@pytest.mark.parametrize(
    "unexplained",
    [
        {"comment": "note"},
        {"tips_amount": 0},
        {"rounding": 0},
    ],
)
def test_an_unexplained_invoice_field_is_malformed_not_green(unexplained) -> None:
    projection = _invoice(**unexplained)

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_RESPONSE_MALFORMED in projection.reasons


@pytest.mark.asyncio
async def test_an_artifact_bearing_response_never_proves_an_artifact_contract() -> None:
    envelope = {
        **canonical_response(),
        "voucher_code": ARTIFACT_MARKER,
        "customer": {"name": ARTIFACT_MARKER},
        "public_url": "https://example.invalid/" + ARTIFACT_MARKER,
    }
    evidence = await probe_voucher_calculation_contract(FakeReader(), FakeCalculator(response=envelope))

    assert evidence.calculation_contract_ready is False
    assert GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL in evidence.reasons
    # Finding something that looks like a voucher is not proof of one.
    assert evidence.individual_voucher_artifact_proven is False
    assert evidence.customer_binding_proven is False
    assert evidence.issue_contract_ready is False
    assert evidence.delivery_authorized is False
    assert evidence.ready_for_send is False

    printed = repr(evidence.as_safe_dict())
    assert ARTIFACT_MARKER not in printed
    assert FOREIGN_UUID not in printed


# ---------------------------------------------------------------------------
# Money, promocode and taxes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "base_amount",
        "base_price",
        "subtotal",
        "total",
        "amount_due",
        "discount_amount",
        "amount_paid",
        "voucher_paid_amount",
        "promocode_discount_amount",
    ],
)
@pytest.mark.parametrize("bad", [None, True, False, "1500", 1500.0, {"amount": 1500}])
def test_every_monetary_field_must_be_an_exact_integer(name, bad) -> None:
    projection = _invoice(**{name: bad})

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_RESPONSE_MALFORMED in projection.reasons


@pytest.mark.parametrize(
    "name",
    [
        "base_amount",
        "base_price",
        "subtotal",
        "total",
        "amount_due",
        "discount_amount",
        "amount_paid",
        "voucher_paid_amount",
        "promocode",
        "promocode_discount_amount",
        "taxes",
        "order_uuid",
        "status",
    ],
)
def test_every_required_invoice_field_must_be_present(name) -> None:
    invoice = dict(canonical_response()["invoice"])
    del invoice[name]
    projection = _envelope({"invoice": invoice})

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_RESPONSE_MALFORMED in projection.reasons


@pytest.mark.parametrize("name", ["base_amount", "base_price", "subtotal", "total", "amount_due"])
def test_every_priced_field_must_equal_the_price_we_sent(name) -> None:
    projection = _invoice(**{name: 1499})

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_AMOUNT_MISMATCH in projection.reasons


@pytest.mark.parametrize(
    "name",
    ["discount_amount", "amount_paid", "voucher_paid_amount", "promocode_discount_amount"],
)
@pytest.mark.parametrize("amount", [1, -1, 1500, -1500, -100, 100])
def test_a_nonzero_discount_or_paid_amount_is_never_supported(name, amount) -> None:
    projection = _invoice(**{name: amount})

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_AMOUNT_MISMATCH in projection.reasons


@pytest.mark.parametrize("promocode", ["PROMO", "", "0", 0, False, {"code": "PROMO"}, []])
def test_any_promocode_at_all_is_never_supported(promocode) -> None:
    projection = _invoice(promocode=promocode)

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_AMOUNT_MISMATCH in projection.reasons
    # The promocode's value never reaches a reason.
    assert all("PROMO" not in reason for reason in projection.reasons)


@pytest.mark.parametrize("bad", [True, "0", 0.0, None, {"amount": 0}])
def test_a_promo_discount_of_the_wrong_type_is_malformed(bad) -> None:
    projection = _invoice(promocode_discount_amount=bad)

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_RESPONSE_MALFORMED in projection.reasons


@pytest.mark.parametrize("taxes", [[{"rate": 19}], [1], ["vat"]])
def test_any_tax_line_is_never_supported(taxes) -> None:
    projection = _invoice(taxes=taxes)

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_AMOUNT_MISMATCH in projection.reasons


@pytest.mark.parametrize("taxes", [None, {}, "", 0, {"vat": 19}])
def test_a_non_list_taxes_field_is_malformed(taxes) -> None:
    projection = _invoice(taxes=taxes)

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_RESPONSE_MALFORMED in projection.reasons


@pytest.mark.parametrize("observed", [-1500, 0, 1500])
def test_account_paid_amount_is_observed_but_never_decides_readiness(observed) -> None:
    projection = _invoice(account_paid_amount=observed)

    assert projection.proven is True
    assert projection.account_paid_amount_observed == observed


@pytest.mark.parametrize("observed", [None, "-1500", -1500.0, True])
def test_a_non_integer_account_paid_amount_is_simply_not_reported(observed) -> None:
    projection = _invoice(account_paid_amount=observed)

    assert projection.proven is True
    assert projection.account_paid_amount_observed is None


@pytest.mark.asyncio
async def test_the_observed_bookkeeping_figure_does_not_block_the_canonical_case() -> None:
    evidence = await probe_voucher_calculation_contract(FakeReader(), FakeCalculator())

    assert evidence.calculation_contract_ready is True
    assert evidence.account_paid_amount_observed == -1500
    assert evidence.as_safe_dict()["account_paid_amount_observed"] == -1500


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
    evidence = await probe_voucher_calculation_contract(FakeReader(), FakeCalculator(raises=rejection))

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
    projection = _envelope(fully_discounted_response())

    assert projection.proven is False
    assert GIFT_CARD_CALCULATION_AMOUNT_MISMATCH in projection.reasons


# ---------------------------------------------------------------------------
# Hygiene
# ---------------------------------------------------------------------------


def test_the_module_implements_no_write_operation() -> None:
    code = code_without_docstrings(contract_module)
    for forbidden in ("Idempotency-Key", "create_order", "pay_order", "refund_order"):
        assert forbidden not in code, forbidden


@pytest.mark.parametrize(
    "reason",
    [
        GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE,
        GIFT_CARD_CALCULATION_TEMPLATE_UNPROVEN,
        GIFT_CARD_CALCULATION_PRICE_UNPROVEN,
        GIFT_CARD_CALCULATION_REJECTED,
        GIFT_CARD_CALCULATION_UNCERTAIN,
        GIFT_CARD_CALCULATION_RESPONSE_MALFORMED,
        GIFT_CARD_CALCULATION_AMOUNT_MISMATCH,
        GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL,
        GIFT_CARD_TEMPLATE_COUNTER_DRIFT,
        GIFT_CARD_TEMPLATE_STATE_DRIFT,
        GIFT_CARD_TEMPLATE_VERIFICATION_UNCERTAIN,
        GIFT_CARD_CALCULATION_CONTRACT_UNPROVEN,
    ],
)
def test_every_reason_is_a_stable_pii_free_slug(reason) -> None:
    assert reason == reason.lower()
    assert reason.replace("_", "").isalnum()
    assert "-" not in reason and " " not in reason


def test_no_reason_still_advertises_an_automatic_retry() -> None:
    """`exit 3` means UNKNOWN, and a name suggesting "retryable" invites a re-run."""
    assert UNCERTAIN_REASONS == {
        GIFT_CARD_CALCULATION_UNCERTAIN,
        GIFT_CARD_TEMPLATE_VERIFICATION_UNCERTAIN,
    }
    assert all("retry" not in reason for reason in UNCERTAIN_REASONS)
