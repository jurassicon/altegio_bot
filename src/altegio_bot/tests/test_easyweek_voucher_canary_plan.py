"""Stage plans, template freeze and safe artifact projection for the canary (§35).

No network and no database: the reader is a fake replaying the payload shapes
EasyWeek really returns, and every identity is obviously fabricated.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from altegio_bot.easyweek_client import EasyWeekAuthError, EasyWeekNotFoundError, EasyWeekRetryableError
from altegio_bot.easyweek_voucher_canary import plan as plan_module
from altegio_bot.easyweek_voucher_canary.artifact import (
    ARTIFACT_EMPTY_COLLECTION,
    MAX_NODES,
    observe_artifact,
)
from altegio_bot.easyweek_voucher_canary.orders import (
    CANARY_ORDER_EXTRA_ITEMS,
    CANARY_ORDER_TOTAL_UNPROVEN,
    CANARY_VOUCHER_LINE_UNPROVEN,
    ORDER_OPEN,
    ORDER_PAID,
    matches_canary_order,
    page_meta,
    payable_order_reasons,
    walk_pages,
)
from altegio_bot.easyweek_voucher_canary.plan import (
    CANARY_ACCOUNT_UNPROVEN,
    CANARY_API_UNAVAILABLE,
    CANARY_API_UNCERTAIN,
    CANARY_CUSTOMER_UNPROVEN,
    CANARY_DISABLED_BY_ENV,
    CANARY_EXISTING_MARKER_ORDER,
    CANARY_IDENTITY_BINDING_MISMATCH,
    CANARY_LEDGER_STATE_UNEXPECTED,
    CANARY_LOCATION_UNPROVEN,
    CANARY_MARKER_ORDER_AMBIGUOUS,
    CANARY_MARKER_ORDER_MISSING,
    CANARY_ORDER_WALK_INCOMPLETE,
    CANARY_PLAN_DIGEST_MISMATCH,
    CANARY_PLAN_EXPIRED,
    CANARY_RUNTIME_IDENTITY_INVALID,
    CANARY_RUNTIME_IDENTITY_MISSING,
    CANARY_STAFFER_UNPROVEN,
    CANARY_TARGET_ORDER_NOT_OPEN,
    CANARY_TARGET_ORDER_NOT_PAID,
    CANARY_TARGET_ORDER_UNPROVEN,
    CANARY_TEMPLATE_COUNTERS_UNUSABLE,
    CANARY_TEMPLATE_UNFROZEN,
    CANARY_TEMPLATE_UNPROVEN,
    CANARY_WORKSPACE_MISMATCH,
    PLAN_MAX_AGE,
    STAGE_CREATE,
    STAGE_PAY,
    STAGE_REFUND,
    RuntimeIdentity,
    build_stage_plan,
    canary_marker,
    frozen_template_mismatches,
    identity_binding_matches,
    identity_fingerprint,
    immutable_template_digest,
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
    listed_order,
    open_order,
    orders_page,
    paid_order,
    voucher_line,
)
from altegio_bot.utils import utcnow

IDENTITY = RuntimeIdentity(
    customer_uuid=CUSTOMER_UUID,
    staffer_uuid=STAFFER_UUID,
    account_uuid=ACCOUNT_UUID,
)
MARKER = canary_marker()
ARTIFACT_SENTINEL = "SENTINEL_CODE_eee111"
PHONE_SENTINEL = "+491700000042"
EMAIL_SENTINEL = "canary.person@example.invalid"


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
        order_pages: list[dict[str, Any]] | None = None,
        order: Any = None,
        raise_on: Exception | None = None,
        scope: tuple[str, str] = (CUSTOMER_UUID, STAFFER_UUID),
    ) -> None:
        self.workspace = WORKSPACE if workspace is None else workspace
        self.locations = LOCATIONS if locations is None else locations
        self.templates = [TEMPLATE] if templates is None else templates
        self.template = TEMPLATE if template is None else template
        self.customer = CUSTOMER if customer is None else customer
        self.staffers = STAFFERS if staffers is None else staffers
        self.accounts = ACCOUNTS if accounts is None else accounts
        self.order_pages = order_pages if order_pages is not None else [orders_page([])]
        self.order = order
        self.raise_on = raise_on
        # The customer and staffer the order listing is expected to be scoped
        # to. The documented endpoint takes both as filters, and the observed
        # body echoes neither, so the REQUEST is where that scope is proven.
        self.scope = scope
        self.calls: list[str] = []

    async def get_workspace(self) -> dict[str, Any]:
        self.calls.append("get_workspace")
        if self.raise_on is not None:
            raise self.raise_on
        return self.workspace

    async def list_locations(self) -> list[dict[str, Any]]:
        return self.locations

    async def list_voucher_templates(self) -> list[dict[str, Any]]:
        return self.templates

    async def get_voucher_template(self, voucher_template_uuid: str) -> dict[str, Any]:
        assert voucher_template_uuid == EASYWEEK_VOUCHER_TEMPLATE_UUID
        self.calls.append("get_voucher_template")
        return self.template

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        return self.customer

    async def list_location_staffers(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        assert location_uuid == KARLSRUHE_LOCATION_UUID
        self.calls.append(f"list_staffers:{page}")
        return self.staffers if page == 1 else {"data": [], "meta": {"last_page": 1}}

    async def list_location_accounts(self, location_uuid: str) -> Any:
        assert location_uuid == KARLSRUHE_LOCATION_UUID
        self.calls.append("list_accounts")
        return self.accounts

    async def list_location_orders(
        self,
        *,
        location_uuid: str,
        customer_uuid: str,
        staffer_uuid: str,
        page: int,
        per_page: int = 100,
    ) -> dict[str, Any]:
        assert location_uuid == KARLSRUHE_LOCATION_UUID
        assert (customer_uuid, staffer_uuid) == self.scope
        self.calls.append(f"list_orders:{page}")
        index = page - 1
        return self.order_pages[index] if index < len(self.order_pages) else orders_page([])

    async def get_order(self, order_uuid: str) -> dict[str, Any]:
        self.calls.append("get_order")
        if self.order is None:
            raise EasyWeekNotFoundError("missing", status_code=404)
        return self.order


_PLAN_ONLY_KWARGS = {"ledger_status", "target_order_uuid", "ledger_identity", "identity", "now"}


async def _plan(stage: str = STAGE_CREATE, **kwargs: Any):
    reader_kwargs = {k: v for k, v in kwargs.items() if k not in _PLAN_ONLY_KWARGS}
    return await build_stage_plan(
        FakeReader(**reader_kwargs),
        stage=stage,
        identity=kwargs.get("identity") or IDENTITY,
        enabled=True,
        ledger_status=kwargs.get("ledger_status"),
        target_order_uuid=kwargs.get("target_order_uuid"),
        ledger_identity=kwargs.get("ledger_identity"),
        now=kwargs.get("now"),
    )


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


# ---------------------------------------------------------------------------
# Template: immutable configuration vs counters that legitimately move
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
    assert frozen_template_mismatches({**TEMPLATE, **change})


def test_counters_are_not_part_of_the_immutable_configuration_digest() -> None:
    """A voucher being issued is the product working, not a template edit."""
    baseline = immutable_template_digest(TEMPLATE)
    after_issuance = immutable_template_digest({**TEMPLATE, "vouchers_count": 1, "activated_vouchers_count": 1})
    assert after_issuance == baseline

    edited = immutable_template_digest({**TEMPLATE, "cost": 1600})
    assert edited != baseline


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


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


def test_metadata_is_only_trusted_when_it_answers_the_page_we_asked_for() -> None:
    page_one = orders_page([], page=1, last_page=3)

    meta = page_meta(page_one, expected_page=1, expected_per_page=100)
    assert meta is not None
    assert (meta.current_page, meta.last_page, meta.per_page) == (1, 3, 100)

    # The same body, offered as the answer to a request for page 2.
    assert page_meta(page_one, expected_page=2, expected_per_page=100) is None


@pytest.mark.parametrize(
    "meta",
    [
        None,
        [],
        "meta",
        {},
        {"last_page": 2},
        {"current_page": 1},
        {"current_page": "1", "last_page": 2},
        {"current_page": 1, "last_page": "2"},
        {"current_page": True, "last_page": 2},
        {"current_page": 1, "last_page": True},
        {"current_page": 2, "last_page": 1},
        {"current_page": 1, "last_page": 0},
        {"current_page": 1, "last_page": 1, "per_page": 50},
        {"current_page": 1, "last_page": 1, "per_page": "100"},
    ],
)
def test_malformed_or_disagreeing_metadata_proves_nothing(meta) -> None:
    payload: dict[str, Any] = {"data": []}
    if meta is not None:
        payload["meta"] = meta

    assert page_meta(payload, expected_page=1, expected_per_page=100) is None


def test_a_page_size_the_response_omits_is_not_invented() -> None:
    meta = page_meta(
        {"data": [], "meta": {"current_page": 1, "last_page": 1}},
        expected_page=1,
        expected_per_page=100,
    )
    assert meta is not None
    assert meta.per_page is None


@pytest.mark.asyncio
async def test_a_walk_follows_last_page_even_past_an_empty_page() -> None:
    pages = {
        1: orders_page([listed_order(marker="a")], page=1, last_page=3),
        2: orders_page([], page=2, last_page=3),
        3: orders_page([listed_order(marker="b", uuid=OTHER_UUID)], page=3, last_page=3),
    }

    async def fetch(page: int) -> Any:
        return pages[page]

    walk = await walk_pages(fetch)
    assert walk.complete is True
    assert len(walk.rows) == 2


@pytest.mark.asyncio
async def test_a_walk_without_metadata_is_never_complete() -> None:
    """An empty page is not an end marker. Only a published `last_page` is.

    A listing that stops answering — an edge serving a cached empty body, a
    filter the server quietly ignored — looks exactly like "there is nothing
    more". Treating that as a finished walk is how an unresolved create gets
    closed as "no order exists" while a real one sits in the dashboard.
    """
    pages = {1: {"data": [listed_order(marker="a")]}, 2: {"data": []}}

    async def fetch(page: int) -> Any:
        return pages[page]

    walk = await walk_pages(fetch)
    assert walk.complete is False
    assert len(walk.rows) == 0


@pytest.mark.asyncio
async def test_a_server_that_answers_page_two_with_page_one_never_completes() -> None:
    """The current reproduction: page 1 says 1 of 2, and so does page 2.

    Without a `current_page` check the walk counts that repeat as the final
    page, "completes", and reports whatever page 1 happened to contain as the
    whole of this customer's orders.
    """
    first = orders_page([listed_order(marker="a")], page=1, last_page=2)
    calls: list[int] = []

    async def fetch(page: int) -> Any:
        calls.append(page)
        # Page 2 is requested; page 1 comes back, metadata and all.
        return first

    walk = await walk_pages(fetch)

    assert walk.complete is False
    assert calls == [1, 2]


@pytest.mark.asyncio
async def test_a_page_size_other_than_the_fixed_one_is_not_a_proven_page() -> None:
    async def fetch(page: int) -> Any:
        return {
            "data": [listed_order(marker="a")],
            "meta": {"current_page": page, "last_page": 1, "per_page": 25},
        }

    walk = await walk_pages(fetch)
    assert walk.complete is False


@pytest.mark.asyncio
async def test_a_last_page_that_changes_mid_walk_is_incomplete() -> None:
    pages = {
        1: orders_page([listed_order(marker="a")], page=1, last_page=3),
        2: orders_page([listed_order(marker="b")], page=2, last_page=9),
    }

    async def fetch(page: int) -> Any:
        return pages[page]

    walk = await walk_pages(fetch)
    assert walk.complete is False


@pytest.mark.asyncio
async def test_a_walk_that_hits_the_ceiling_is_incomplete() -> None:
    async def fetch(page: int) -> Any:
        return {"data": [listed_order(marker="x")]}

    walk = await walk_pages(fetch, max_pages=3)
    assert walk.complete is False


# ---------------------------------------------------------------------------
# Matching a listing row against the REAL order shape
# ---------------------------------------------------------------------------


def _matches(row: dict[str, Any]) -> bool:
    now = utcnow()
    return matches_canary_order(
        row,
        customer_uuid=CUSTOMER_UUID,
        marker=MARKER,
        window_start=now - timedelta(days=2),
        window_end=now + timedelta(days=2),
    )


def test_a_row_without_location_or_staffer_still_matches() -> None:
    """The observed body carries neither; the documented request scoped them."""
    row = listed_order(marker=MARKER, created_at=utcnow().isoformat())
    assert "location_uuid" not in row
    assert "staffer_uuid" not in row
    assert _matches(row) is True


def test_an_unfamiliar_voucher_shape_does_not_disqualify_our_own_order() -> None:
    row = listed_order(marker=MARKER, created_at=utcnow().isoformat(), vouchers=[{"surprise": 1}])
    assert _matches(row) is True


def test_a_missing_vouchers_field_does_not_disqualify_our_own_order() -> None:
    row = listed_order(marker=MARKER, created_at=utcnow().isoformat())
    row.pop("vouchers")
    assert _matches(row) is True


@pytest.mark.parametrize(
    "wrong",
    [
        {"comment": "somebody-elses-marker"},
        {"customer": {"uuid": OTHER_UUID}},
        {"created_at": "2020-01-01T00:00:00+00:00"},
        {"created_at": "not-a-timestamp"},
        {"created_at": None},
    ],
)
def test_a_row_that_is_not_ours_never_matches(wrong) -> None:
    row = listed_order(marker=MARKER, created_at=utcnow().isoformat())
    row.update(wrong)
    assert _matches(row) is False


def test_a_row_that_names_no_customer_is_matched_on_marker_and_window() -> None:
    row = listed_order(marker=MARKER, created_at=utcnow().isoformat())
    row.pop("customer")
    assert _matches(row) is True


# ---------------------------------------------------------------------------
# The create stage plan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_complete_create_plan_is_ready() -> None:
    plan = await _plan(STAGE_CREATE)

    assert plan.ready is True
    assert plan.reasons == ()
    assert plan.stage == STAGE_CREATE
    assert plan.counters_observed == {"vouchers_count": 0, "activated_vouchers_count": 0}
    assert plan.marker == MARKER
    assert plan.confirmation_phrase.startswith("create-voucher-canary-")


@pytest.mark.asyncio
async def test_each_stage_has_its_own_digest_and_phrase() -> None:
    create = await _plan(STAGE_CREATE)
    pay = await _plan(
        STAGE_PAY,
        ledger_status="created",
        target_order_uuid=ORDER_UUID,
        order=open_order(marker=MARKER),
        order_pages=[orders_page([listed_order(marker=MARKER, created_at=utcnow().isoformat())])],
    )
    refund = await _plan(
        STAGE_REFUND, ledger_status="paid", target_order_uuid=ORDER_UUID, order=paid_order(marker=MARKER)
    )

    assert len({create.digest, pay.digest, refund.digest}) == 3
    assert len({create.confirmation_phrase, pay.confirmation_phrase, refund.confirmation_phrase}) == 3


@pytest.mark.asyncio
async def test_the_env_fence_blocks_a_plan_by_default() -> None:
    plan = await build_stage_plan(
        FakeReader(), stage=STAGE_CREATE, identity=IDENTITY, enabled=False, ledger_status=None
    )
    assert plan.ready is False
    assert CANARY_DISABLED_BY_ENV in plan.reasons


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs,reason",
    [
        ({"workspace": {**WORKSPACE, "currency": "USD"}}, CANARY_WORKSPACE_MISMATCH),
        ({"locations": []}, CANARY_LOCATION_UNPROVEN),
        ({"templates": []}, CANARY_TEMPLATE_UNPROVEN),
        ({"template": {**TEMPLATE, "cost": 1600}}, CANARY_TEMPLATE_UNFROZEN),
        ({"template": {**TEMPLATE, "vouchers_count": None}}, CANARY_TEMPLATE_COUNTERS_UNUSABLE),
        ({"customer": {**CUSTOMER, "uuid": OTHER_UUID}}, CANARY_CUSTOMER_UNPROVEN),
        ({"customer": {**CUSTOMER, "email": ""}}, CANARY_CUSTOMER_UNPROVEN),
        ({"staffers": {"data": [], "meta": {"last_page": 1}}}, CANARY_STAFFER_UNPROVEN),
        ({"accounts": []}, CANARY_ACCOUNT_UNPROVEN),
        ({"accounts": [{"uuid": OTHER_UUID}]}, CANARY_ACCOUNT_UNPROVEN),
    ],
)
async def test_every_unproven_prerequisite_blocks_the_create_plan(kwargs, reason) -> None:
    plan = await _plan(STAGE_CREATE, **kwargs)

    assert plan.ready is False
    assert reason in plan.reasons


@pytest.mark.asyncio
async def test_an_existing_marker_order_blocks_a_create_plan() -> None:
    row = listed_order(marker=MARKER, created_at=utcnow().isoformat())
    plan = await _plan(STAGE_CREATE, order_pages=[orders_page([row])])

    assert CANARY_EXISTING_MARKER_ORDER in plan.reasons


@pytest.mark.asyncio
async def test_an_incomplete_order_walk_blocks_a_create_plan() -> None:
    class Endless(FakeReader):
        async def list_location_orders(self, **kwargs: Any) -> dict[str, Any]:
            return {"data": [listed_order(marker="unrelated")]}

    plan = await build_stage_plan(Endless(), stage=STAGE_CREATE, identity=IDENTITY, enabled=True, ledger_status=None)
    assert CANARY_ORDER_WALK_INCOMPLETE in plan.reasons


@pytest.mark.asyncio
async def test_a_create_plan_needs_an_empty_ledger() -> None:
    plan = await _plan(STAGE_CREATE, ledger_status="created", target_order_uuid=ORDER_UUID)
    assert CANARY_LEDGER_STATE_UNEXPECTED in plan.reasons


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,reason",
    [
        (EasyWeekRetryableError("down", attempts=3), CANARY_API_UNCERTAIN),
        (EasyWeekAuthError("auth", status_code=403), CANARY_API_UNAVAILABLE),
    ],
)
async def test_an_api_failure_blocks_the_plan_with_its_own_disposition(failure, reason) -> None:
    plan = await _plan(STAGE_CREATE, raise_on=failure)
    assert reason in plan.reasons


# ---------------------------------------------------------------------------
# The pay stage plan
# ---------------------------------------------------------------------------


async def _pay_plan(**kwargs: Any):
    defaults: dict[str, Any] = {
        "ledger_status": "created",
        "target_order_uuid": ORDER_UUID,
        "order": open_order(marker=MARKER),
        "order_pages": [orders_page([listed_order(marker=MARKER, created_at=utcnow().isoformat())])],
    }
    defaults.update(kwargs)
    return await _plan(STAGE_PAY, **defaults)


@pytest.mark.asyncio
async def test_a_complete_pay_plan_is_ready() -> None:
    plan = await _pay_plan()

    assert plan.ready is True
    assert plan.order_state == ORDER_OPEN
    assert plan.ledger_state["status"] == "created"
    assert plan.counters_observed is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [None, "create_unknown", "pay_claimed", "paid", "refunded"])
async def test_a_pay_plan_requires_exactly_the_created_ledger_state(status) -> None:
    plan = await _pay_plan(ledger_status=status)
    assert CANARY_LEDGER_STATE_UNEXPECTED in plan.reasons


@pytest.mark.asyncio
async def test_a_pay_plan_needs_a_target_order() -> None:
    plan = await _pay_plan(target_order_uuid=None)
    assert CANARY_TARGET_ORDER_UNPROVEN in plan.reasons


@pytest.mark.asyncio
async def test_a_pay_plan_refuses_an_order_that_is_no_longer_open() -> None:
    plan = await _pay_plan(order=paid_order(marker=MARKER))
    assert CANARY_TARGET_ORDER_NOT_OPEN in plan.reasons


@pytest.mark.asyncio
async def test_an_open_order_with_our_marker_but_no_voucher_line_is_not_payable() -> None:
    """The current reproduction: right marker, right customer, wrong contents.

    A payment settles whatever the order happens to hold, so "is this OUR
    order?" is not the same question as "is this the order we authorised?".
    Without the second question this plan came back ready, and the €15 approval
    would have settled a sum nobody approved.
    """
    wrong = open_order(
        marker=MARKER,
        vouchers=[],
        invoice={"total": 9900, "subtotal": 9900, "amount_due": 9900, "amount_paid": 0},
    )

    plan = await _pay_plan(order=wrong)

    assert plan.ready is False
    assert CANARY_VOUCHER_LINE_UNPROVEN in plan.reasons
    assert CANARY_ORDER_TOTAL_UNPROVEN in plan.reasons
    assert plan.snapshot["payable_order_proven"] is False


@pytest.mark.parametrize(
    "order_changes,expected",
    [
        ({"vouchers": []}, CANARY_VOUCHER_LINE_UNPROVEN),
        ({"vouchers": [voucher_line(), voucher_line()]}, CANARY_VOUCHER_LINE_UNPROVEN),
        ({"vouchers": [voucher_line(voucher_template_uuid=OTHER_UUID)]}, CANARY_VOUCHER_LINE_UNPROVEN),
        ({"vouchers": [voucher_line(price=1501)]}, CANARY_VOUCHER_LINE_UNPROVEN),
        ({"vouchers": [voucher_line(price="1500")]}, CANARY_VOUCHER_LINE_UNPROVEN),
        # `True == 1` in Python, and a boolean price is not a price.
        ({"vouchers": [voucher_line(price=True)]}, CANARY_VOUCHER_LINE_UNPROVEN),
        ({"vouchers": [voucher_line(quantity=2)]}, CANARY_VOUCHER_LINE_UNPROVEN),
        ({"vouchers": [voucher_line(quantity=True)]}, CANARY_VOUCHER_LINE_UNPROVEN),
        ({"vouchers": [voucher_line(quantity=1.0)]}, CANARY_VOUCHER_LINE_UNPROVEN),
        ({"services": [{"uuid": OTHER_UUID}]}, CANARY_ORDER_EXTRA_ITEMS),
        ({"goods": [{"uuid": OTHER_UUID}]}, CANARY_ORDER_EXTRA_ITEMS),
        ({"invoice": {"total": 3000, "amount_due": 3000, "amount_paid": 0}}, CANARY_ORDER_TOTAL_UNPROVEN),
        ({"invoice": {"total": 1500, "subtotal": 1600, "amount_due": 1500}}, CANARY_ORDER_TOTAL_UNPROVEN),
        ({"invoice": {"total": "1500", "amount_due": 1500}}, CANARY_ORDER_TOTAL_UNPROVEN),
    ],
)
def test_only_the_exact_one_voucher_order_is_payable(order_changes, expected) -> None:
    order = open_order(marker=MARKER)
    order.update(order_changes)

    reasons = payable_order_reasons(
        order,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    assert expected in reasons


def test_the_authorised_order_produces_no_payment_objection() -> None:
    assert (
        payable_order_reasons(
            open_order(marker=MARKER),
            expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
            expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
        )
        == ()
    )


def test_a_total_the_body_does_not_publish_is_not_invented() -> None:
    """Absence is not a mismatch: not every read carries an invoice."""
    order = open_order(marker=MARKER)
    del order["invoice"]

    assert (
        payable_order_reasons(
            order,
            expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
            expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
        )
        == ()
    )


@pytest.mark.asyncio
async def test_a_refund_plan_never_consults_the_voucher_contents() -> None:
    """An unreadable artifact is a research disappointment, not a reason to
    leave a real payment standing."""
    odd = paid_order(marker=MARKER, vouchers=[], invoice={"total": 9900, "amount_due": 0, "amount_paid": 1500})

    plan = await _refund_plan(order=odd)

    assert plan.ready is True
    for reason in (CANARY_VOUCHER_LINE_UNPROVEN, CANARY_ORDER_TOTAL_UNPROVEN, CANARY_ORDER_EXTRA_ITEMS):
        assert reason not in plan.reasons


@pytest.mark.asyncio
async def test_a_pay_plan_refuses_a_missing_marker_order() -> None:
    plan = await _pay_plan(order_pages=[orders_page([])])
    assert CANARY_MARKER_ORDER_MISSING in plan.reasons


@pytest.mark.asyncio
async def test_a_pay_plan_refuses_two_marker_orders() -> None:
    now = utcnow().isoformat()
    plan = await _pay_plan(
        order_pages=[
            orders_page(
                [
                    listed_order(marker=MARKER, created_at=now),
                    listed_order(marker=MARKER, created_at=now, uuid=OTHER_UUID),
                ]
            )
        ]
    )
    assert CANARY_MARKER_ORDER_AMBIGUOUS in plan.reasons


@pytest.mark.asyncio
async def test_a_pay_plan_refuses_an_order_with_the_wrong_customer() -> None:
    plan = await _pay_plan(order=open_order(marker=MARKER, customer={"uuid": OTHER_UUID}))
    assert CANARY_CUSTOMER_UNPROVEN in plan.reasons


@pytest.mark.asyncio
async def test_a_pay_plan_refuses_an_order_with_the_wrong_marker() -> None:
    plan = await _pay_plan(order=open_order(marker="somebody-elses"))
    assert CANARY_TARGET_ORDER_UNPROVEN in plan.reasons


@pytest.mark.asyncio
async def test_counters_that_moved_do_not_block_a_pay_plan() -> None:
    """Issuance moves counters; that is the product working, not an edit."""
    moved = {**TEMPLATE, "vouchers_count": 1, "activated_vouchers_count": 0}
    plan = await _pay_plan(template=moved)

    assert plan.ready is True
    assert plan.counters_observed == {"vouchers_count": 1, "activated_vouchers_count": 0}


# ---------------------------------------------------------------------------
# The refund stage plan
# ---------------------------------------------------------------------------


async def _refund_plan(**kwargs: Any):
    defaults: dict[str, Any] = {
        "ledger_status": "paid",
        "target_order_uuid": ORDER_UUID,
        "order": paid_order(marker=MARKER),
    }
    defaults.update(kwargs)
    return await _plan(STAGE_REFUND, **defaults)


@pytest.mark.asyncio
async def test_a_complete_refund_plan_is_ready() -> None:
    plan = await _refund_plan()

    assert plan.ready is True
    assert plan.order_state == ORDER_PAID


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [None, "created", "pay_unknown", "refund_claimed", "refunded"])
async def test_a_refund_plan_requires_exactly_the_paid_ledger_state(status) -> None:
    plan = await _refund_plan(ledger_status=status)
    assert CANARY_LEDGER_STATE_UNEXPECTED in plan.reasons


@pytest.mark.asyncio
async def test_a_refund_plan_refuses_an_order_that_is_not_paid() -> None:
    plan = await _refund_plan(order=open_order(marker=MARKER))
    assert CANARY_TARGET_ORDER_NOT_PAID in plan.reasons


@pytest.mark.asyncio
async def test_a_malformed_artifact_never_blocks_a_refund_plan() -> None:
    """Cleanup beats research: a broken voucher must not strand a real payment."""
    unreadable = paid_order(marker=MARKER, vouchers=[{"???": {"deep": [1, 2, 3]}}])
    plan = await _refund_plan(order=unreadable)

    assert plan.ready is True
    assert plan.order_state == ORDER_PAID


@pytest.mark.asyncio
async def test_counter_drift_never_blocks_a_refund_plan() -> None:
    drifted = {**TEMPLATE, "vouchers_count": 5, "activated_vouchers_count": 3}
    plan = await _refund_plan(template=drifted)

    assert plan.ready is True
    assert plan.counters_observed == {"vouchers_count": 5, "activated_vouchers_count": 3}


@pytest.mark.asyncio
async def test_a_template_edit_still_blocks_a_refund_plan() -> None:
    plan = await _refund_plan(template={**TEMPLATE, "cost": 1600})
    assert CANARY_TEMPLATE_UNFROZEN in plan.reasons


# ---------------------------------------------------------------------------
# Trying again after a rejection that provably did not act
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_proven_rejection_can_be_planned_again() -> None:
    """Otherwise a fixable 422 is a dead end with a draft order left behind."""
    plan = await _pay_plan(ledger_status="pay_rejected")

    assert plan.ready is True
    assert CANARY_LEDGER_STATE_UNEXPECTED not in plan.reasons
    assert plan.snapshot["retry_after_proven_rejection"] is True


@pytest.mark.asyncio
async def test_a_normal_first_attempt_is_not_marked_as_a_retry() -> None:
    plan = await _pay_plan()

    assert plan.ready is True
    assert plan.snapshot["retry_after_proven_rejection"] is False


@pytest.mark.asyncio
async def test_a_rejected_refund_can_be_planned_again() -> None:
    plan = await _refund_plan(ledger_status="refund_rejected")

    assert plan.ready is True
    assert plan.snapshot["retry_after_proven_rejection"] is True


@pytest.mark.asyncio
async def test_a_rejected_create_can_be_planned_again() -> None:
    plan = await _plan(STAGE_CREATE, ledger_status="create_rejected", ledger_identity=None)

    assert plan.ready is True
    assert plan.snapshot["retry_after_proven_rejection"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [
        "pay_claimed",
        "pay_unknown",
        "paid",
        "refund_claimed",
        "refund_unknown",
        "refunded",
        "ambiguous",
        "manually_cleaned",
        "create_unknown",
        "refund_rejected",
    ],
)
async def test_nothing_but_this_stages_own_rejection_reopens_a_pay(status) -> None:
    """A claimed or unknown stage may already have acted. It stays shut."""
    plan = await _pay_plan(ledger_status=status)

    assert plan.ready is False
    assert CANARY_LEDGER_STATE_UNEXPECTED in plan.reasons


@pytest.mark.asyncio
async def test_a_retry_still_has_to_prove_everything_a_first_attempt_does() -> None:
    """ "Rejected" reopens the door; it does not walk through it."""
    plan = await _pay_plan(ledger_status="pay_rejected", order=paid_order(marker=MARKER))

    assert plan.ready is False
    assert CANARY_TARGET_ORDER_NOT_OPEN in plan.reasons


# ---------------------------------------------------------------------------
# One canary, one identity
# ---------------------------------------------------------------------------


def _stored(identity: RuntimeIdentity) -> dict[str, str]:
    return dict(identity.fingerprints)


OTHER_ACCOUNT_IDENTITY = RuntimeIdentity(
    customer_uuid=CUSTOMER_UUID,
    staffer_uuid=STAFFER_UUID,
    account_uuid=OTHER_UUID,
)
OTHER_CUSTOMER_IDENTITY = RuntimeIdentity(
    customer_uuid=OTHER_UUID,
    staffer_uuid=STAFFER_UUID,
    account_uuid=ACCOUNT_UUID,
)
OTHER_STAFFER_IDENTITY = RuntimeIdentity(
    customer_uuid=CUSTOMER_UUID,
    staffer_uuid=OTHER_UUID,
    account_uuid=ACCOUNT_UUID,
)


def test_no_stored_identity_yet_is_not_a_mismatch() -> None:
    assert identity_binding_matches(IDENTITY, None) is True


@pytest.mark.parametrize(
    "identity",
    [OTHER_ACCOUNT_IDENTITY, OTHER_CUSTOMER_IDENTITY, OTHER_STAFFER_IDENTITY],
)
def test_any_role_differing_breaks_the_binding(identity) -> None:
    assert identity_binding_matches(identity, _stored(IDENTITY)) is False


def test_a_partially_recorded_identity_never_matches() -> None:
    stored = _stored(IDENTITY)
    stored["account"] = None  # type: ignore[assignment]
    assert identity_binding_matches(IDENTITY, stored) is False


@pytest.mark.asyncio
async def test_another_real_account_of_the_same_branch_gives_no_ready_pay_plan() -> None:
    """The most dangerous near-miss: everything real, everything wrong.

    A second Karlsruhe account passes the branch proof, the account proof, the
    template freeze and the workspace check. Only the binding to the row this
    canary opened can tell it from the approved one.
    """
    plan = await _pay_plan(
        identity=OTHER_ACCOUNT_IDENTITY,
        accounts=[{"uuid": ACCOUNT_UUID}, {"uuid": OTHER_UUID}],
        ledger_identity=_stored(IDENTITY),
    )

    assert plan.ready is False
    assert CANARY_IDENTITY_BINDING_MISMATCH in plan.reasons
    assert CANARY_ACCOUNT_UNPROVEN not in plan.reasons
    assert plan.snapshot["identity_binding_proven"] is False
    assert plan.ledger_state["identity_binding_proven"] is False


@pytest.mark.asyncio
async def test_another_customer_gives_no_ready_pay_plan() -> None:
    plan = await _pay_plan(
        identity=OTHER_CUSTOMER_IDENTITY,
        scope=(OTHER_UUID, STAFFER_UUID),
        customer={**CUSTOMER, "uuid": OTHER_UUID},
        ledger_identity=_stored(IDENTITY),
    )

    assert plan.ready is False
    assert CANARY_IDENTITY_BINDING_MISMATCH in plan.reasons


@pytest.mark.asyncio
async def test_another_staffer_gives_no_ready_pay_plan() -> None:
    plan = await _pay_plan(
        identity=OTHER_STAFFER_IDENTITY,
        scope=(CUSTOMER_UUID, OTHER_UUID),
        staffers={"data": [{"uuid": OTHER_UUID}], "meta": {"current_page": 1, "last_page": 1, "per_page": 100}},
        ledger_identity=_stored(IDENTITY),
    )

    assert plan.ready is False
    assert CANARY_IDENTITY_BINDING_MISMATCH in plan.reasons


@pytest.mark.asyncio
async def test_a_matching_identity_is_reported_as_a_boolean_and_nothing_else() -> None:
    plan = await _pay_plan(ledger_identity=_stored(IDENTITY))

    assert plan.ready is True
    printed = repr(plan.as_safe_dict())
    for uuid in (CUSTOMER_UUID, STAFFER_UUID, ACCOUNT_UUID, ORDER_UUID):
        assert uuid not in printed


@pytest.mark.asyncio
async def test_the_binding_changes_the_stage_digest() -> None:
    """Not merely reported: signed. An approval is for one identity."""
    bound = await _pay_plan(ledger_identity=_stored(IDENTITY))
    broken = await _pay_plan(ledger_identity=_stored(OTHER_ACCOUNT_IDENTITY))

    assert bound.digest_for(bound.issued_at) != broken.digest_for(bound.issued_at)


# ---------------------------------------------------------------------------
# Plan authorisation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_matching_digest_phrase_and_fresh_plan_authorise_the_stage() -> None:
    plan = await _plan(STAGE_CREATE)
    assert (
        verify_plan_authorisation(
            plan,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )
        == ()
    )


@pytest.mark.asyncio
async def test_a_create_approval_never_authorises_a_pay_plan() -> None:
    create = await _plan(STAGE_CREATE)
    pay = await _pay_plan()

    reasons = verify_plan_authorisation(
        pay,
        supplied_digest=create.digest,
        supplied_issued_at=pay.issued_at,
        supplied_phrase=create.confirmation_phrase,
    )
    assert CANARY_PLAN_DIGEST_MISMATCH in reasons
    assert plan_module.CANARY_CONFIRMATION_MISMATCH in reasons


@pytest.mark.asyncio
@pytest.mark.parametrize("age", [PLAN_MAX_AGE + timedelta(seconds=1), timedelta(days=1)])
async def test_a_stale_approval_blocks(age) -> None:
    plan = await _plan(STAGE_CREATE)
    reasons = verify_plan_authorisation(
        plan,
        supplied_digest=plan.digest,
        supplied_issued_at=utcnow() - age,
        supplied_phrase=plan.confirmation_phrase,
    )
    assert CANARY_PLAN_EXPIRED in reasons


@pytest.mark.asyncio
async def test_an_unready_plan_never_authorises_even_with_a_matching_digest() -> None:
    plan = await build_stage_plan(
        FakeReader(), stage=STAGE_CREATE, identity=IDENTITY, enabled=False, ledger_status=None
    )
    reasons = verify_plan_authorisation(
        plan,
        supplied_digest=plan.digest,
        supplied_issued_at=plan.issued_at,
        supplied_phrase=plan.confirmation_phrase,
    )
    assert CANARY_DISABLED_BY_ENV in reasons


# ---------------------------------------------------------------------------
# The moment is part of the approval
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_issue_time_is_signed_not_merely_printed() -> None:
    """A microsecond apart is a different approval."""
    plan = await _plan(STAGE_CREATE)
    moment = plan.issued_at

    assert plan.digest == plan.digest_for(moment)
    assert plan.digest_for(moment + timedelta(microseconds=1)) != plan.digest


@pytest.mark.asyncio
async def test_an_old_digest_cannot_be_revived_with_a_freshly_typed_timestamp() -> None:
    """The attack the age check alone cannot see.

    An operator (or a script) holding yesterday's approved digest only has to
    type today's timestamp next to it for the freshness test to pass. Binding
    the timestamp INTO the digest is what makes the pair inseparable.
    """
    old = await _plan(STAGE_CREATE, now=utcnow() - timedelta(hours=3))
    live = await _plan(STAGE_CREATE)

    reasons = verify_plan_authorisation(
        live,
        supplied_digest=old.digest,
        supplied_issued_at=utcnow(),
        supplied_phrase=old.confirmation_phrase,
    )

    assert CANARY_PLAN_DIGEST_MISMATCH in reasons
    assert plan_module.CANARY_CONFIRMATION_MISMATCH in reasons


@pytest.mark.asyncio
async def test_the_same_approval_still_works_when_the_plan_is_rebuilt_seconds_later() -> None:
    """The legitimate path: the mutation command recomputes the plan itself."""
    issued = await _plan(STAGE_CREATE)
    rebuilt = await _plan(STAGE_CREATE, now=issued.issued_at + timedelta(seconds=4))

    assert (
        verify_plan_authorisation(
            rebuilt,
            supplied_digest=issued.digest,
            supplied_issued_at=issued.issued_at,
            supplied_phrase=issued.confirmation_phrase,
        )
        == ()
    )


@pytest.mark.asyncio
async def test_an_approval_with_no_timestamp_authorises_nothing() -> None:
    plan = await _plan(STAGE_CREATE)

    reasons = verify_plan_authorisation(
        plan,
        supplied_digest=plan.digest,
        supplied_issued_at=None,
        supplied_phrase=plan.confirmation_phrase,
    )

    assert CANARY_PLAN_EXPIRED in reasons
    assert CANARY_PLAN_DIGEST_MISMATCH in reasons


@pytest.mark.asyncio
async def test_a_create_digest_is_invalid_for_a_pay_and_for_a_refund() -> None:
    create = await _plan(STAGE_CREATE)

    for rebuilt in (await _pay_plan(now=create.issued_at), await _refund_plan(now=create.issued_at)):
        reasons = verify_plan_authorisation(
            rebuilt,
            supplied_digest=create.digest,
            supplied_issued_at=create.issued_at,
            supplied_phrase=create.confirmation_phrase,
        )
        assert CANARY_PLAN_DIGEST_MISMATCH in reasons
        assert plan_module.CANARY_CONFIRMATION_MISMATCH in reasons


@pytest.mark.asyncio
async def test_counters_that_moved_do_not_invalidate_an_approval() -> None:
    """A voucher being issued is the product working, not a template edit.

    If the counters were signed, the refund would become unreachable at exactly
    the moment a payment succeeded — which is when it matters most.
    """
    issued = await _refund_plan()
    after_issuance = await _refund_plan(
        template={**TEMPLATE, "vouchers_count": 1, "activated_vouchers_count": 1},
        now=issued.issued_at,
    )

    assert after_issuance.counters_observed != issued.counters_observed
    assert after_issuance.digest_for(issued.issued_at) == issued.digest


@pytest.mark.asyncio
async def test_the_plan_snapshot_carries_no_runtime_uuid_or_contact_detail() -> None:
    reader_customer = {**CUSTOMER, "phone": PHONE_SENTINEL, "email": EMAIL_SENTINEL}
    plan = await _plan(STAGE_CREATE, customer=reader_customer)
    printed = repr(plan.as_safe_dict())

    for forbidden in (CUSTOMER_UUID, STAFFER_UUID, ACCOUNT_UUID, PHONE_SENTINEL, EMAIL_SENTINEL, "Synthetic"):
        assert forbidden not in printed, forbidden
    assert plan.snapshot["identity_fingerprints"]["customer"] == identity_fingerprint("customer", CUSTOMER_UUID)


@pytest.mark.asyncio
async def test_a_plan_always_repeats_that_nothing_may_be_sent() -> None:
    safe = (await _plan(STAGE_CREATE)).as_safe_dict()

    assert safe["campaign_send_authorized"] is False
    assert safe["customer_message_sent"] is False
    assert safe["ready_for_send"] is False


@pytest.mark.asyncio
async def test_the_plan_separates_configuration_counters_ledger_and_authorisation() -> None:
    safe = (await _pay_plan()).as_safe_dict()

    assert set(safe) >= {"immutable_template_digest", "counters_observed", "ledger_state", "plan_digest"}
    assert safe["immutable_template_digest"] != safe["plan_digest"]


# ---------------------------------------------------------------------------
# Artifact projection — shape only, bounded, no value digests
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
    assert observation.individual_voucher_artifact_observed is False
    assert observation.artifact_customer_binding_proven is False


def test_an_issued_voucher_is_observed_without_any_value_or_digest() -> None:
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
    safe = observation.as_safe_dict()

    assert observation.individual_voucher_artifact_observed is True
    assert safe["artifact_contract_proven"] is False
    # No value comparison is possible, and the report says so rather than
    # pretending with a weak hash.
    assert safe["cross_stage_equality_proven"] is False
    # The shape is still there: the research question stays answerable.
    code_field = next(entry for entry in safe["fields"] if entry["path"].endswith("code"))
    assert code_field["json_type"] == "string"
    assert code_field["value_length"] == len(ARTIFACT_SENTINEL)
    assert "value_fingerprint" not in code_field


def test_a_voucher_inside_a_customer_bound_order_is_not_an_artifact_binding() -> None:
    order = paid_order(marker=MARKER)
    order["vouchers"] = [{"uuid": ORDER_UUID, "code": ARTIFACT_SENTINEL}]
    observation = _observe(order)

    assert observation.order_customer_binding_proven is True
    assert observation.artifact_customer_binding_proven is False
    assert observation.artifact_nested_in_customer_bound_order is True


def test_a_voucher_naming_its_own_owner_is_an_artifact_binding() -> None:
    order = paid_order(marker=MARKER)
    order["vouchers"] = [{"uuid": ORDER_UUID, "code": ARTIFACT_SENTINEL, "customer_uuid": CUSTOMER_UUID}]
    assert _observe(order).artifact_customer_binding_proven is True


def test_an_empty_voucher_collection_is_a_shape_not_an_artifact() -> None:
    observation = _observe(open_order(marker=MARKER, vouchers=[]))
    assert observation.individual_voucher_artifact_observed is False
    assert observation.artifact_kind_observed == ARTIFACT_EMPTY_COLLECTION


def test_a_refunded_order_is_observed_as_rolled_back() -> None:
    assert _observe(open_order(marker=MARKER, is_reverted=True)).refund_observed is True


def test_a_hostile_deeply_nested_artifact_is_truncated_not_followed() -> None:
    node: Any = {"code": ARTIFACT_SENTINEL}
    for _ in range(50):
        node = {"nested": node}
    observation = _observe(open_order(marker=MARKER, vouchers=[node]))

    assert observation.truncated is True
    assert len(observation.fields) <= MAX_NODES
    assert ARTIFACT_SENTINEL not in repr(observation.as_safe_dict())


def test_a_hostile_wide_artifact_is_bounded() -> None:
    order = open_order(marker=MARKER)
    order["vouchers"] = [{f"key_{index}": ARTIFACT_SENTINEL for index in range(5000)}]
    observation = _observe(order)

    assert observation.truncated is True
    assert len(observation.fields) <= MAX_NODES


def test_a_hostile_key_name_becomes_a_placeholder_not_a_digest() -> None:
    """A key can BE a value; hashing one would be the same mistake as hashing it."""
    order = open_order(marker=MARKER)
    hostile_key = "a" + chr(10) + "b " + PHONE_SENTINEL
    order["vouchers"] = [{hostile_key: 1}]

    printed = repr(_observe(order).as_safe_dict())
    assert PHONE_SENTINEL not in printed
    assert "<opaque-key>" in printed


@pytest.mark.parametrize("payload", [None, "text", [1, 2, 3], 7])
def test_a_non_object_response_projects_nothing_and_proves_nothing(payload) -> None:
    observation = _observe(payload)
    assert observation.fields == ()
    assert observation.order_customer_binding_proven is False


def test_the_artifact_module_computes_no_digest_at_all() -> None:
    from altegio_bot.easyweek_voucher_canary import artifact as artifact_module

    code = code_without_docstrings(artifact_module)
    for forbidden in ("hashlib", "sha256", "fingerprint", "md5", "blake2"):
        assert forbidden not in code, forbidden
