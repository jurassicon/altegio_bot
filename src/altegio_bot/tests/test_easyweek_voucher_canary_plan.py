"""Plan, template freeze and safe artifact projection for the canary (§35).

No network and no database: the reader is a fake replaying synthetic payloads,
and every identity is obviously fabricated.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from altegio_bot.easyweek_client import EasyWeekAuthError, EasyWeekRetryableError
from altegio_bot.easyweek_voucher_canary import plan as plan_module
from altegio_bot.easyweek_voucher_canary.artifact import (
    ARTIFACT_EMPTY_COLLECTION,
    MAX_NODES,
    observe_artifact,
)
from altegio_bot.easyweek_voucher_canary.plan import (
    CANARY_ACCOUNT_UNPROVEN,
    CANARY_API_UNAVAILABLE,
    CANARY_API_UNCERTAIN,
    CANARY_CUSTOMER_UNPROVEN,
    CANARY_DISABLED_BY_ENV,
    CANARY_EXISTING_MARKER_ORDER,
    CANARY_LOCATION_UNPROVEN,
    CANARY_PLAN_DIGEST_MISMATCH,
    CANARY_PLAN_EXPIRED,
    CANARY_RUNTIME_IDENTITY_INVALID,
    CANARY_RUNTIME_IDENTITY_MISSING,
    CANARY_STAFFER_UNPROVEN,
    CANARY_TEMPLATE_COUNTERS_UNUSABLE,
    CANARY_TEMPLATE_UNFROZEN,
    CANARY_TEMPLATE_UNPROVEN,
    CANARY_WORKSPACE_MISMATCH,
    MUTATION_STAGES,
    PLAN_MAX_AGE,
    STAGE_CREATE,
    STAGE_PAY,
    RuntimeIdentity,
    build_plan,
    canary_marker,
    frozen_template_mismatches,
    identity_fingerprint,
    resolve_runtime_identity,
    template_counters,
    verify_plan_authorisation,
)
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    KARLSRUHE_LOCATION_UUID,
    SUPPORTED_VOUCHER_PRICE_MINOR,
)
from altegio_bot.tests.easyweek_voucher_canary_fixtures import (
    ACCOUNT_UUID,
    ACCOUNTS,
    CUSTOMER,
    CUSTOMER_UUID,
    LOCATIONS,
    ORDER_UUID,
    OTHER_UUID,
    STAFFER_UUID,
    STAFFERS,
    TEMPLATE,
    WORKSPACE,
    code_without_docstrings,
    open_order,
    paid_order,
)
from altegio_bot.utils import utcnow

IDENTITY = RuntimeIdentity(
    customer_uuid=CUSTOMER_UUID,
    staffer_uuid=STAFFER_UUID,
    account_uuid=ACCOUNT_UUID,
)
MARKER = canary_marker()
ARTIFACT_SENTINEL = "SENTINEL_CODE_eee111"


class FakeReader:
    """The reviewed GET surface, replaying synthetic payloads."""

    def __init__(
        self,
        *,
        workspace: Any = None,
        locations: Any = None,
        templates: Any = None,
        template: Any = None,
        customer: Any = None,
        staffers: Any = None,
        accounts: Any = None,
        orders: list[list[dict[str, Any]]] | None = None,
        raise_on: Exception | None = None,
    ) -> None:
        self.workspace = WORKSPACE if workspace is None else workspace
        self.locations = LOCATIONS if locations is None else locations
        self.templates = [TEMPLATE] if templates is None else templates
        self.template = TEMPLATE if template is None else template
        self.customer = CUSTOMER if customer is None else customer
        self.staffers = STAFFERS if staffers is None else staffers
        self.accounts = ACCOUNTS if accounts is None else accounts
        self.orders = orders if orders is not None else [[]]
        self.raise_on = raise_on
        self.calls: list[str] = []

    async def get_workspace(self) -> dict[str, Any]:
        self.calls.append("get_workspace")
        if self.raise_on is not None:
            raise self.raise_on
        return self.workspace

    async def list_locations(self) -> list[dict[str, Any]]:
        self.calls.append("list_locations")
        return self.locations

    async def list_voucher_templates(self) -> list[dict[str, Any]]:
        self.calls.append("list_voucher_templates")
        return self.templates

    async def get_voucher_template(self, voucher_template_uuid: str) -> dict[str, Any]:
        assert voucher_template_uuid == EASYWEEK_VOUCHER_TEMPLATE_UUID
        self.calls.append("get_voucher_template")
        return self.template

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        self.calls.append("get_customer")
        return self.customer

    async def list_location_staffers(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        assert location_uuid == KARLSRUHE_LOCATION_UUID
        self.calls.append(f"list_staffers:{page}")
        return self.staffers if page == 1 else {"data": []}

    async def list_location_accounts(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        assert location_uuid == KARLSRUHE_LOCATION_UUID
        self.calls.append(f"list_accounts:{page}")
        return self.accounts if page == 1 else {"data": []}

    async def list_location_orders(
        self, *, location_uuid: str, customer_uuid: str, page: int, per_page: int = 100
    ) -> dict[str, Any]:
        assert location_uuid == KARLSRUHE_LOCATION_UUID
        assert customer_uuid == CUSTOMER_UUID
        self.calls.append(f"list_orders:{page}")
        index = page - 1
        rows = self.orders[index] if index < len(self.orders) else []
        return {"data": rows}


async def _plan(**reader_kwargs: Any):
    return await build_plan(FakeReader(**reader_kwargs), identity=IDENTITY, enabled=True)


# ---------------------------------------------------------------------------
# Runtime identity
# ---------------------------------------------------------------------------


def test_a_complete_canonical_identity_resolves() -> None:
    identity, reasons = resolve_runtime_identity(
        customer_uuid=CUSTOMER_UUID,
        staffer_uuid=STAFFER_UUID,
        account_uuid=ACCOUNT_UUID,
    )
    assert reasons == ()
    assert identity is not None
    assert set(identity.fingerprints) == {"customer", "staffer", "account"}
    assert all(len(value) == 64 for value in identity.fingerprints.values())


@pytest.mark.parametrize("missing", ["customer_uuid", "staffer_uuid", "account_uuid"])
def test_a_missing_runtime_uuid_blocks_without_leaking_anything(missing) -> None:
    kwargs = {"customer_uuid": CUSTOMER_UUID, "staffer_uuid": STAFFER_UUID, "account_uuid": ACCOUNT_UUID}
    kwargs[missing] = ""
    identity, reasons = resolve_runtime_identity(**kwargs)

    assert identity is None
    assert CANARY_RUNTIME_IDENTITY_MISSING in reasons
    assert all(CUSTOMER_UUID not in reason for reason in reasons)


def test_surrounding_whitespace_from_an_env_file_is_trimmed() -> None:
    """A trailing space in `easyweek.env` is an operator slip, not an identity.

    Trimming is safe because what survives must still be EXACTLY canonical, and
    the transport re-checks that independently before the wire.
    """
    identity, reasons = resolve_runtime_identity(
        customer_uuid=f"  {CUSTOMER_UUID}\n",
        staffer_uuid=STAFFER_UUID,
        account_uuid=ACCOUNT_UUID,
    )
    assert reasons == ()
    assert identity is not None and identity.customer_uuid == CUSTOMER_UUID


@pytest.mark.parametrize(
    "bad",
    ["not-a-uuid", CUSTOMER_UUID.upper(), "{" + CUSTOMER_UUID + "}", "urn:uuid:" + CUSTOMER_UUID, 12345],
)
def test_a_noncanonical_runtime_uuid_blocks_without_echoing_the_value(bad) -> None:
    identity, reasons = resolve_runtime_identity(
        customer_uuid=bad,
        staffer_uuid=STAFFER_UUID,
        account_uuid=ACCOUNT_UUID,
    )
    assert identity is None
    # Either refusal is correct; what matters is that it IS a refusal and
    # that the offending value is nowhere in it.
    assert set(reasons) <= {CANARY_RUNTIME_IDENTITY_INVALID, CANARY_RUNTIME_IDENTITY_MISSING}
    assert reasons
    assert all(str(bad) not in reason for reason in reasons)


def test_fingerprints_are_role_salted_and_one_way() -> None:
    as_customer = identity_fingerprint("customer", CUSTOMER_UUID)
    as_staffer = identity_fingerprint("staffer", CUSTOMER_UUID)

    assert as_customer != as_staffer
    assert CUSTOMER_UUID not in as_customer
    assert len(as_customer) == 64


def test_the_marker_is_deterministic_and_non_personal() -> None:
    assert canary_marker() == canary_marker()
    assert set(canary_marker()) <= set("abcdefghijklmnopqrstuvwxyz0123456789-")
    assert len(canary_marker()) <= 64


# ---------------------------------------------------------------------------
# Template freeze
# ---------------------------------------------------------------------------


def test_the_confirmed_template_is_frozen() -> None:
    assert frozen_template_mismatches(TEMPLATE) == ()


@pytest.mark.parametrize(
    "change",
    [
        {"cost": 1600},
        {"value": 1600},
        {"is_enabled": False},
        {"is_online": True},
        {"is_single_charge": False},
        {"forces_activation": False},
        {"activate_after": 1},
        {"activate_at": "2026-09-11"},
        {"validity": 30},
        {"branches_count": 2},
        {"all_branches_count": 4},
        {"services_count": 42},
        {"goods_count": 1},
        {"uuid": OTHER_UUID},
        {"cost": True},
        {"cost": 1500.0},
        {"cost": "1500"},
    ],
)
def test_any_unfrozen_field_is_named_without_its_value(change) -> None:
    mismatched = frozen_template_mismatches({**TEMPLATE, **change})

    assert mismatched
    # Only names, never observed values.
    assert all(str(value) not in " ".join(mismatched) for value in change.values() if value not in (True, False))


@pytest.mark.parametrize(
    "counters",
    [
        {"vouchers_count": None},
        {"vouchers_count": True},
        {"activated_vouchers_count": "0"},
        {"vouchers_count": -1},
        {"vouchers_count": 0, "activated_vouchers_count": 1},
    ],
)
def test_unusable_counters_are_not_evidence(counters) -> None:
    assert template_counters({**TEMPLATE, **counters}) is None


def test_a_possible_counter_pair_is_accepted() -> None:
    assert template_counters({**TEMPLATE, "vouchers_count": 3, "activated_vouchers_count": 2}) == {
        "vouchers_count": 3,
        "activated_vouchers_count": 2,
    }


# ---------------------------------------------------------------------------
# The plan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_complete_plan_is_ready_and_authorises_three_distinct_stages() -> None:
    plan = await _plan()

    assert plan.ready is True
    assert plan.reasons == ()
    assert plan.counters == {"vouchers_count": 0, "activated_vouchers_count": 0}
    assert plan.marker == MARKER
    phrases = {stage: plan.confirmation_phrase(stage) for stage in MUTATION_STAGES}
    assert len(set(phrases.values())) == 3
    assert all(plan.digest[:12] in phrase for phrase in phrases.values())


@pytest.mark.asyncio
async def test_a_plan_creates_no_row_and_sends_no_mutation() -> None:
    reader = FakeReader()
    await build_plan(reader, identity=IDENTITY, enabled=True)

    # GETs only, and every one of them a reviewed read.
    assert all(call.startswith(("get_", "list_")) for call in reader.calls)
    assert not any("create" in call or "pay" in call or "refund" in call for call in reader.calls)


@pytest.mark.asyncio
async def test_the_env_fence_blocks_a_plan_by_default() -> None:
    plan = await build_plan(FakeReader(), identity=IDENTITY, enabled=False)

    assert plan.ready is False
    assert CANARY_DISABLED_BY_ENV in plan.reasons


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs,reason",
    [
        ({"workspace": {**WORKSPACE, "currency": "USD"}}, CANARY_WORKSPACE_MISMATCH),
        ({"workspace": {**WORKSPACE, "slug": "other"}}, CANARY_WORKSPACE_MISMATCH),
        ({"locations": []}, CANARY_LOCATION_UNPROVEN),
        ({"locations": [*LOCATIONS, {"uuid": KARLSRUHE_LOCATION_UUID}]}, CANARY_LOCATION_UNPROVEN),
        ({"templates": []}, CANARY_TEMPLATE_UNPROVEN),
        ({"templates": [TEMPLATE, dict(TEMPLATE)]}, CANARY_TEMPLATE_UNPROVEN),
        ({"template": {**TEMPLATE, "cost": 1600}}, CANARY_TEMPLATE_UNFROZEN),
        ({"template": {**TEMPLATE, "is_online": True}}, CANARY_TEMPLATE_UNFROZEN),
        ({"template": {**TEMPLATE, "vouchers_count": None}}, CANARY_TEMPLATE_COUNTERS_UNUSABLE),
        ({"customer": {**CUSTOMER, "uuid": OTHER_UUID}}, CANARY_CUSTOMER_UNPROVEN),
        ({"customer": {**CUSTOMER, "email": ""}}, CANARY_CUSTOMER_UNPROVEN),
        ({"customer": {**CUSTOMER, "phone": ""}}, CANARY_CUSTOMER_UNPROVEN),
        ({"staffers": {"data": []}}, CANARY_STAFFER_UNPROVEN),
        ({"staffers": {"data": [{"uuid": OTHER_UUID}]}}, CANARY_STAFFER_UNPROVEN),
        ({"accounts": {"data": []}}, CANARY_ACCOUNT_UNPROVEN),
        ({"accounts": {"data": [{"uuid": OTHER_UUID}]}}, CANARY_ACCOUNT_UNPROVEN),
        ({"orders": [[open_order(marker=MARKER)], []]}, CANARY_EXISTING_MARKER_ORDER),
    ],
)
async def test_every_unproven_prerequisite_blocks_the_plan(kwargs, reason) -> None:
    plan = await _plan(**kwargs)

    assert plan.ready is False
    assert reason in plan.reasons


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,reason",
    [
        (EasyWeekRetryableError("down", attempts=3), CANARY_API_UNCERTAIN),
        (EasyWeekAuthError("auth", status_code=403), CANARY_API_UNAVAILABLE),
    ],
)
async def test_an_api_failure_blocks_the_plan_with_its_own_disposition(failure, reason) -> None:
    plan = await _plan(raise_on=failure)

    assert plan.ready is False
    assert reason in plan.reasons


@pytest.mark.asyncio
async def test_the_plan_snapshot_carries_no_runtime_uuid() -> None:
    plan = await _plan()
    printed = repr(plan.as_safe_dict())

    for forbidden in (CUSTOMER_UUID, STAFFER_UUID, ACCOUNT_UUID, "Synthetic", "fixture@example.invalid", "+49"):
        assert forbidden not in printed, forbidden
    # The fingerprints ARE there, and they are one-way.
    assert plan.snapshot["identity_fingerprints"]["customer"] == identity_fingerprint("customer", CUSTOMER_UUID)


@pytest.mark.asyncio
async def test_a_plan_always_repeats_that_nothing_may_be_sent() -> None:
    safe = (await _plan()).as_safe_dict()

    assert safe["campaign_send_authorized"] is False
    assert safe["customer_message_sent"] is False
    assert safe["ready_for_send"] is False


@pytest.mark.asyncio
async def test_drift_changes_the_digest() -> None:
    baseline = await _plan()
    drifted = await _plan(template={**TEMPLATE, "cost": 1600})

    assert drifted.digest != baseline.digest


@pytest.mark.asyncio
async def test_an_incomplete_listing_walk_is_never_read_as_absence() -> None:
    """A staffer list that never ends is unproven, not 'the staffer is missing'."""

    class EndlessStaffers(FakeReader):
        async def list_location_staffers(self, location_uuid: str, *, page: int) -> dict[str, Any]:
            return {"data": [{"uuid": OTHER_UUID}]}

    plan = await build_plan(EndlessStaffers(), identity=IDENTITY, enabled=True)
    assert CANARY_STAFFER_UNPROVEN in plan.reasons


@pytest.mark.asyncio
async def test_an_endless_order_walk_blocks_rather_than_reporting_zero() -> None:
    class EndlessOrders(FakeReader):
        async def list_location_orders(self, **kwargs: Any) -> dict[str, Any]:
            return {"data": [open_order(marker="something-else")]}

    plan = await build_plan(EndlessOrders(), identity=IDENTITY, enabled=True)
    assert CANARY_EXISTING_MARKER_ORDER in plan.reasons


# ---------------------------------------------------------------------------
# Plan authorisation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_matching_digest_phrase_and_fresh_plan_authorise_one_stage() -> None:
    plan = await _plan()
    reasons = verify_plan_authorisation(
        plan,
        stage=STAGE_CREATE,
        supplied_digest=plan.digest,
        supplied_issued_at=plan.issued_at,
        supplied_phrase=plan.confirmation_phrase(STAGE_CREATE),
    )
    assert reasons == ()


@pytest.mark.asyncio
async def test_a_wrong_digest_blocks() -> None:
    plan = await _plan()
    reasons = verify_plan_authorisation(
        plan,
        stage=STAGE_CREATE,
        supplied_digest="0" * 64,
        supplied_issued_at=plan.issued_at,
        supplied_phrase=plan.confirmation_phrase(STAGE_CREATE),
    )
    assert CANARY_PLAN_DIGEST_MISMATCH in reasons


@pytest.mark.asyncio
async def test_another_stages_phrase_never_authorises_this_one() -> None:
    plan = await _plan()
    reasons = verify_plan_authorisation(
        plan,
        stage=STAGE_CREATE,
        supplied_digest=plan.digest,
        supplied_issued_at=plan.issued_at,
        supplied_phrase=plan.confirmation_phrase(STAGE_PAY),
    )
    assert plan_module.CANARY_CONFIRMATION_MISMATCH in reasons


@pytest.mark.asyncio
@pytest.mark.parametrize("age", [PLAN_MAX_AGE + timedelta(seconds=1), timedelta(days=1)])
async def test_a_stale_approval_blocks(age) -> None:
    plan = await _plan()
    reasons = verify_plan_authorisation(
        plan,
        stage=STAGE_CREATE,
        supplied_digest=plan.digest,
        supplied_issued_at=utcnow() - age,
        supplied_phrase=plan.confirmation_phrase(STAGE_CREATE),
    )
    assert CANARY_PLAN_EXPIRED in reasons


@pytest.mark.asyncio
async def test_a_missing_or_future_issued_at_blocks() -> None:
    plan = await _plan()
    for issued_at in (None, utcnow() + timedelta(minutes=5)):
        reasons = verify_plan_authorisation(
            plan,
            stage=STAGE_CREATE,
            supplied_digest=plan.digest,
            supplied_issued_at=issued_at,
            supplied_phrase=plan.confirmation_phrase(STAGE_CREATE),
        )
        assert CANARY_PLAN_EXPIRED in reasons


@pytest.mark.asyncio
async def test_an_unready_plan_never_authorises_even_with_a_matching_digest() -> None:
    plan = await build_plan(FakeReader(), identity=IDENTITY, enabled=False)
    reasons = verify_plan_authorisation(
        plan,
        stage=STAGE_CREATE,
        supplied_digest=plan.digest,
        supplied_issued_at=plan.issued_at,
        supplied_phrase=plan.confirmation_phrase(STAGE_CREATE),
    )
    assert CANARY_DISABLED_BY_ENV in reasons


# ---------------------------------------------------------------------------
# Artifact projection — shape only, bounded, fail-closed
# ---------------------------------------------------------------------------


def _observe(payload: Any, stage: str = "test"):
    return observe_artifact(
        payload,
        stage=stage,
        expected_customer_uuid=CUSTOMER_UUID,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )


def test_an_open_order_proves_the_line_and_the_order_binding_only() -> None:
    observation = _observe(open_order(marker=MARKER))

    assert observation.order_customer_binding_proven is True
    assert observation.voucher_line_proven is True
    # A line item echoed back is not an issued voucher.
    assert observation.individual_voucher_artifact_observed is False
    assert observation.artifact_customer_binding_proven is False
    assert observation.refund_observed is False


def test_an_issued_voucher_is_observed_but_its_value_never_kept() -> None:
    order = paid_order(marker=MARKER)
    order["vouchers"] = [
        {
            "uuid": ORDER_UUID,
            "voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID,
            "code": ARTIFACT_SENTINEL,
            "public_url": "https://example.invalid/" + ARTIFACT_SENTINEL,
        }
    ]
    observation = _observe(order, stage="paid_readback")
    printed = repr(observation.as_safe_dict())

    assert observation.individual_voucher_artifact_observed is True
    assert observation.artifact_kind_observed == "voucher_collection"
    assert ARTIFACT_SENTINEL not in printed
    assert "example.invalid" not in printed
    # A shape was still recorded, so the research question is answerable.
    assert any(field["path"].endswith("code") for field in observation.as_safe_dict()["fields"])
    # And it is never promoted to a contract.
    assert observation.as_safe_dict()["artifact_contract_proven"] is False


def test_a_voucher_inside_a_customer_bound_order_is_not_an_artifact_binding() -> None:
    """The nesting is a weaker fact and is reported as its own, separate one."""
    order = paid_order(marker=MARKER)
    order["vouchers"] = [{"uuid": ORDER_UUID, "code": ARTIFACT_SENTINEL}]
    observation = _observe(order)

    assert observation.order_customer_binding_proven is True
    assert observation.artifact_customer_binding_proven is False
    assert observation.artifact_nested_in_customer_bound_order is True


def test_a_voucher_naming_its_own_owner_is_an_artifact_binding() -> None:
    order = paid_order(marker=MARKER)
    order["vouchers"] = [{"uuid": ORDER_UUID, "code": ARTIFACT_SENTINEL, "customer_uuid": CUSTOMER_UUID}]
    observation = _observe(order)

    assert observation.artifact_customer_binding_proven is True


def test_a_wrong_customer_is_never_a_binding() -> None:
    observation = _observe(open_order(marker=MARKER, customer_uuid=OTHER_UUID))
    assert observation.order_customer_binding_proven is False


@pytest.mark.parametrize(
    "line",
    [
        {"voucher_template_uuid": OTHER_UUID, "price": 1500, "quantity": 1},
        {"voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID, "price": 1499, "quantity": 1},
        {"voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID, "price": 1500, "quantity": 2},
        {"voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID, "price": True, "quantity": 1},
        {"voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID, "price": 1500.0, "quantity": 1},
    ],
)
def test_a_wrong_voucher_line_is_never_proven(line) -> None:
    observation = _observe(open_order(marker=MARKER, vouchers=[line]))
    assert observation.voucher_line_proven is False


def test_an_empty_voucher_collection_is_a_shape_not_an_artifact() -> None:
    observation = _observe(open_order(marker=MARKER, vouchers=[]))

    assert observation.individual_voucher_artifact_observed is False
    assert observation.artifact_kind_observed == ARTIFACT_EMPTY_COLLECTION


def test_a_refunded_order_is_observed_as_rolled_back() -> None:
    observation = _observe(open_order(marker=MARKER, is_reverted=True))
    assert observation.refund_observed is True


def test_a_hostile_deeply_nested_artifact_is_truncated_not_followed() -> None:
    node: Any = {"code": ARTIFACT_SENTINEL}
    for _ in range(50):
        node = {"nested": node}
    order = open_order(marker=MARKER, vouchers=[node])

    observation = _observe(order)

    assert observation.truncated is True
    assert len(observation.fields) <= MAX_NODES
    assert ARTIFACT_SENTINEL not in repr(observation.as_safe_dict())


def test_a_hostile_wide_artifact_is_bounded() -> None:
    order = open_order(marker=MARKER)
    order["vouchers"] = [{f"key_{index}": ARTIFACT_SENTINEL for index in range(5000)}]

    observation = _observe(order)

    assert observation.truncated is True
    assert len(observation.fields) <= MAX_NODES
    assert ARTIFACT_SENTINEL not in repr(observation.as_safe_dict())


def test_a_hostile_key_name_is_reduced_to_a_fingerprint() -> None:
    order = open_order(marker=MARKER)
    hostile_key = "a" + chr(10) + "b" + chr(0) + " " + ARTIFACT_SENTINEL
    order["vouchers"] = [{hostile_key: 1}]

    observation = _observe(order)
    printed = repr(observation.as_safe_dict())

    assert ARTIFACT_SENTINEL not in printed
    assert "<opaque:" in printed


def test_a_huge_string_value_is_measured_not_copied() -> None:
    order = open_order(marker=MARKER)
    order["vouchers"] = [{"code": "x" * 10_000_000}]

    observation = _observe(order)
    field = next(entry for entry in observation.as_safe_dict()["fields"] if entry["path"].endswith("code"))

    assert field["value_length"] <= 4096
    assert "value" not in field or field.get("value") is None


@pytest.mark.parametrize("payload", [None, "text", [1, 2, 3], 7])
def test_a_non_object_response_projects_nothing_and_proves_nothing(payload) -> None:
    observation = _observe(payload)

    assert observation.fields == ()
    assert observation.order_customer_binding_proven is False
    assert observation.voucher_line_proven is False


def test_the_artifact_module_never_returns_a_raw_value() -> None:
    from altegio_bot.easyweek_voucher_canary import artifact as artifact_module

    code = code_without_docstrings(artifact_module)
    # There is no path that puts a value on an ObservedField.
    assert "value=" not in code
    assert "raw" not in code
