"""Stage flows and unknown-result reconciliation for the voucher canary (§35).

The reader and the mutator are recording fakes, and the ledger is the real table
on PostgreSQL — because "did this stage send a second request?" is only a real
question when the durable state is real.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest

from altegio_bot.easyweek_client import (
    EasyWeekAuthError,
    EasyWeekNotFoundError,
    EasyWeekPermanentError,
    EasyWeekRetryableError,
)
from altegio_bot.easyweek_voucher_canary import ledger as ledger_module
from altegio_bot.easyweek_voucher_canary.orders import (
    ORDER_CANCELLED,
    ORDER_MALFORMED,
    ORDER_OPEN,
    ORDER_PAID,
    ORDER_REFUNDED,
    ORDER_UNKNOWN,
    PAYMENT_PROOF_AMOUNTS,
    PAYMENT_PROOF_NONE,
    PAYMENT_PROOF_STATUS,
    classify_order,
    find_marker_orders,
)
from altegio_bot.easyweek_voucher_canary.plan import (
    CANARY_DISABLED_BY_ENV,
    CANARY_PLAN_DIGEST_MISMATCH,
    CANARY_PLAN_EXPIRED,
    STAGE_CREATE,
    STAGE_PAY,
    STAGE_REFUND,
    RuntimeIdentity,
    build_stage_plan,
    canary_marker,
)
from altegio_bot.easyweek_voucher_canary.runner import (
    OUTCOME_AMBIGUOUS,
    OUTCOME_CONTRACT_MISMATCH,
    OUTCOME_MANUAL_CLEANUP,
    OUTCOME_PROVEN,
    OUTCOME_REFUSED,
    OUTCOME_ROLLBACK_UNPROVEN,
    OUTCOME_UNKNOWN_MUTATION,
    REASON_IDENTITY_BINDING_MISMATCH,
    REASON_MUTATION_REJECTED,
    REASON_MUTATION_UNKNOWN,
    REASON_ORDER_CUSTOMER_UNPROVEN,
    REASON_ORDER_MARKER_UNPROVEN,
    REASON_ORDER_NOT_OPEN,
    REASON_ORDER_READBACK_FAILED,
    REASON_ORDER_SHAPE_UNPROVEN,
    REASON_ORDER_UUID_UNCANONICAL,
    REASON_RECONCILE_AMBIGUOUS,
    REASON_RECONCILE_UNRESOLVED,
    REASON_ROLLBACK_UNPROVEN,
    REASON_TEMPLATE_CONFIG_DRIFT,
    REASON_TEMPLATE_READBACK_FAILED,
    reconcile_transition,
    run_create,
    run_pay,
    run_reconcile,
    run_refund,
    run_status,
)
from altegio_bot.easyweek_voucher_identity import EASYWEEK_VOUCHER_TEMPLATE_UUID, KARLSRUHE_LOCATION_UUID
from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationUnknown, VoucherMutationResponse
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
    cancelled_order,
    listed_order,
    open_order,
    orders_page,
    paid_order,
    refunded_order,
)
from altegio_bot.utils import utcnow

IDENTITY = RuntimeIdentity(
    customer_uuid=CUSTOMER_UUID,
    staffer_uuid=STAFFER_UUID,
    account_uuid=ACCOUNT_UUID,
)
MARKER = canary_marker()
ARTIFACT_SENTINEL = "SENTINEL_VOUCHER_fff111"
PHONE_SENTINEL = "+491700000042"


class FakeReader:
    """Plan GETs plus the POS reads the reconciler needs."""

    def __init__(
        self,
        *,
        template: Any = None,
        order: Any = None,
        order_pages: list[dict[str, Any]] | None = None,
        existing_orders: list[dict[str, Any]] | None = None,
        order_error: Exception | None = None,
    ) -> None:
        self.template = TEMPLATE if template is None else template
        self.order = order
        # Pages served once a claim exists (the reconciliation walk).
        self.order_pages = order_pages
        # Pages served to the plan's "is there already a marker order?" walk.
        self.existing_orders = existing_orders if existing_orders is not None else [orders_page([])]
        self.order_error = order_error
        self.calls: list[str] = []
        self._claimed = False

    # -- plan surface ------------------------------------------------------

    async def get_workspace(self) -> dict[str, Any]:
        self.calls.append("get_workspace")
        return WORKSPACE

    async def list_locations(self) -> list[dict[str, Any]]:
        return LOCATIONS

    async def list_voucher_templates(self) -> list[dict[str, Any]]:
        return [TEMPLATE]

    async def get_voucher_template(self, voucher_template_uuid: str) -> dict[str, Any]:
        self.calls.append("get_voucher_template")
        if isinstance(self.template, Exception):
            raise self.template
        return self.template

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        return CUSTOMER

    async def list_location_staffers(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        return STAFFERS if page == 1 else {"data": [], "meta": {"last_page": 1}}

    async def list_location_accounts(self, location_uuid: str) -> Any:
        return ACCOUNTS

    # -- POS surface -------------------------------------------------------

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
        assert customer_uuid == CUSTOMER_UUID
        assert staffer_uuid == STAFFER_UUID
        self.calls.append(f"list_orders:{page}")
        pages = self.order_pages if (self._claimed and self.order_pages is not None) else self.existing_orders
        index = page - 1
        return pages[index] if index < len(pages) else orders_page([])

    async def get_order(self, order_uuid: str) -> dict[str, Any]:
        self.calls.append("get_order")
        if self.order_error is not None:
            raise self.order_error
        if self.order is None:
            raise EasyWeekNotFoundError("missing", status_code=404)
        return self.order

    def reconciliation_mode(self) -> None:
        """From now on, the order listing serves the post-claim pages."""
        self._claimed = True


class FakeMutator:
    """Records every mutation, and applies the effect a real API would.

    The reader it is given is mutated on success, because the whole point of the
    stage plans is that they re-read the live order immediately before each
    claim: a fake that left the order frozen would be testing a world where a
    payment changes nothing.
    """

    def __init__(
        self,
        reader: "FakeReader | None" = None,
        *,
        create_response: Any = None,
        pay_response: Any = None,
        refund_response: Any = None,
        create_error: Exception | None = None,
        pay_error: Exception | None = None,
        refund_error: Exception | None = None,
        create_effect: bool = True,
        pay_effect: bool = True,
        refund_effect: bool = True,
    ) -> None:
        self.reader = reader
        self.create_response = create_response
        self.pay_response = pay_response
        self.refund_response = refund_response
        self.create_error = create_error
        self.pay_error = pay_error
        self.refund_error = refund_error
        self.create_effect = create_effect
        self.pay_effect = pay_effect
        self.refund_effect = refund_effect
        self.calls: list[str] = []
        self.pay_kwargs: list[dict[str, Any]] = []

    async def create_voucher_order(self, **kwargs: Any) -> VoucherMutationResponse:
        self.calls.append("create")
        assert kwargs["location_uuid"] == KARLSRUHE_LOCATION_UUID
        assert kwargs["voucher_template_uuid"] == EASYWEEK_VOUCHER_TEMPLATE_UUID
        assert kwargs["price_minor"] == 1500
        assert kwargs["marker"] == MARKER
        if self.create_error is not None:
            raise self.create_error
        if self.reader is not None and self.create_effect:
            self.reader.order = open_order(marker=MARKER)
            self.reader.existing_orders = _marker_pages()
        body = self.create_response if self.create_response is not None else open_order(marker=MARKER)
        return VoucherMutationResponse(http_status=200, envelope=body)

    async def pay_voucher_order(self, **kwargs: Any) -> VoucherMutationResponse:
        self.calls.append("pay")
        self.pay_kwargs.append(dict(kwargs))
        assert kwargs["account_uuid"] == ACCOUNT_UUID
        if self.pay_error is not None:
            raise self.pay_error
        if self.reader is not None and self.pay_effect:
            self.reader.order = paid_order(marker=MARKER)
        body = self.pay_response if self.pay_response is not None else paid_order(marker=MARKER)
        return VoucherMutationResponse(http_status=200, envelope=body)

    async def refund_voucher_order(self, **kwargs: Any) -> VoucherMutationResponse:
        self.calls.append("refund")
        if self.refund_error is not None:
            raise self.refund_error
        if self.reader is not None and self.refund_effect:
            self.reader.order = refunded_order(marker=MARKER)
        body = self.refund_response if self.refund_response is not None else refunded_order(marker=MARKER)
        return VoucherMutationResponse(http_status=200, envelope=body)


class RefusingMutator:
    async def create_voucher_order(self, **kwargs: Any):  # pragma: no cover
        raise AssertionError("no mutation may be sent")

    async def pay_voucher_order(self, **kwargs: Any):  # pragma: no cover
        raise AssertionError("no mutation may be sent")

    async def refund_voucher_order(self, **kwargs: Any):  # pragma: no cover
        raise AssertionError("no mutation may be sent")


async def _operator_plan(
    reader: FakeReader,
    stage: str,
    session_maker,
    *,
    now=None,
):
    snapshot = await ledger_module.load(session_maker)
    return await build_stage_plan(
        reader,
        stage=stage,
        identity=IDENTITY,
        enabled=True,
        ledger_status=snapshot.status,
        target_order_uuid=snapshot.target_order_uuid,
        ledger_identity=snapshot.identity_fingerprints if snapshot.exists else None,
        create_window_start=snapshot.create_window_start,
        create_window_end=snapshot.create_window_end,
        now=now,
    )


async def _authorised(reader: FakeReader, stage: str, session_maker) -> dict[str, Any]:
    plan = await _operator_plan(reader, stage, session_maker)
    return {
        "plan_digest": plan.digest,
        "plan_issued_at": plan.issued_at,
        "confirmation_phrase": plan.confirmation_phrase,
    }


async def _create(session_maker, reader: FakeReader, mutator: Any, **overrides: Any):
    kwargs = await _authorised(reader, STAGE_CREATE, session_maker)
    kwargs.update(overrides)
    return await run_create(session_maker, reader, mutator, identity=IDENTITY, enabled=True, **kwargs)


async def _pay(session_maker, reader: FakeReader, mutator: Any, **overrides: Any):
    kwargs = await _authorised(reader, STAGE_PAY, session_maker)
    kwargs.update(overrides)
    return await run_pay(session_maker, reader, mutator, identity=IDENTITY, enabled=True, **kwargs)


async def _refund(session_maker, reader: FakeReader, mutator: Any, **overrides: Any):
    kwargs = await _authorised(reader, STAGE_REFUND, session_maker)
    kwargs.update(overrides)
    return await run_refund(session_maker, reader, mutator, identity=IDENTITY, enabled=True, **kwargs)


def _marker_pages() -> list[dict[str, Any]]:
    return [orders_page([listed_order(marker=MARKER, created_at=utcnow().isoformat())])]


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_proven_create_claims_first_then_sends_exactly_one_request(session_maker) -> None:
    reader = FakeReader(order=open_order(marker=MARKER))
    mutator = FakeMutator()

    report = await _create(session_maker, reader, mutator)

    assert report.outcome == OUTCOME_PROVEN
    assert mutator.calls == ["create"]
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == ledger_module.STATUS_CREATED
    assert snapshot.target_order_uuid == ORDER_UUID
    assert snapshot.stage_plan_digests["create"] is not None
    assert report.manual_cleanup_required is True
    assert report.order_state == ORDER_OPEN


@pytest.mark.asyncio
async def test_every_stage_report_repeats_that_nothing_may_be_sent(session_maker) -> None:
    report = await _create(session_maker, FakeReader(order=open_order(marker=MARKER)), FakeMutator())
    safe = report.as_safe_dict()

    assert safe["campaign_send_authorized"] is False
    assert safe["customer_message_sent"] is False
    assert safe["ready_for_send"] is False
    assert safe["raw_identifiers_omitted"] is True


@pytest.mark.asyncio
async def test_a_disabled_env_fence_blocks_before_any_claim(session_maker) -> None:
    reader = FakeReader()
    snapshot = await ledger_module.load(session_maker)
    plan = await build_stage_plan(
        reader, stage=STAGE_CREATE, identity=IDENTITY, enabled=False, ledger_status=snapshot.status
    )

    report = await run_create(
        session_maker,
        reader,
        RefusingMutator(),
        identity=IDENTITY,
        enabled=False,
        plan_digest=plan.digest,
        plan_issued_at=plan.issued_at,
        confirmation_phrase=plan.confirmation_phrase,
    )

    assert report.outcome == OUTCOME_REFUSED
    assert CANARY_DISABLED_BY_ENV in report.reasons
    assert (await ledger_module.load(session_maker)).exists is False


@pytest.mark.asyncio
async def test_a_digest_mismatch_blocks_before_any_claim(session_maker) -> None:
    report = await _create(session_maker, FakeReader(), RefusingMutator(), plan_digest="0" * 64)

    assert report.outcome == OUTCOME_REFUSED
    assert CANARY_PLAN_DIGEST_MISMATCH in report.reasons
    assert (await ledger_module.load(session_maker)).exists is False


@pytest.mark.asyncio
async def test_an_expired_approval_blocks_before_any_claim(session_maker) -> None:
    from datetime import timedelta

    report = await _create(session_maker, FakeReader(), RefusingMutator(), plan_issued_at=utcnow() - timedelta(days=1))

    assert report.outcome == OUTCOME_REFUSED
    assert CANARY_PLAN_EXPIRED in report.reasons
    assert (await ledger_module.load(session_maker)).exists is False


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [{"cost": 1600}, {"is_enabled": False}, {"is_online": True}, {"branches_count": 2}])
async def test_template_configuration_drift_blocks_before_any_claim(session_maker, drift) -> None:
    clean = FakeReader()
    authorised = await _authorised(clean, STAGE_CREATE, session_maker)

    drifted = FakeReader(template={**TEMPLATE, **drift})
    report = await run_create(session_maker, drifted, RefusingMutator(), identity=IDENTITY, enabled=True, **authorised)

    assert report.outcome == OUTCOME_REFUSED
    assert (await ledger_module.load(session_maker)).exists is False


@pytest.mark.asyncio
async def test_an_unknown_create_is_recorded_and_never_repeated(session_maker) -> None:
    reader = FakeReader()
    mutator = FakeMutator(create_error=EasyWeekVoucherMutationUnknown("lost", attempts=1))

    report = await _create(session_maker, reader, mutator)

    assert report.outcome == OUTCOME_UNKNOWN_MUTATION
    assert REASON_MUTATION_UNKNOWN in report.reasons
    assert report.reconciliation_required is True
    assert mutator.calls == ["create"]

    again = await _create(session_maker, reader, mutator)
    assert again.outcome == OUTCOME_REFUSED
    assert mutator.calls == ["create"]


@pytest.mark.asyncio
async def test_a_rejected_create_is_a_contract_mismatch_not_an_unknown(session_maker) -> None:
    mutator = FakeMutator(create_error=EasyWeekAuthError("auth", status_code=403))
    report = await _create(session_maker, FakeReader(), mutator)

    assert report.outcome == OUTCOME_CONTRACT_MISMATCH
    assert REASON_MUTATION_REJECTED in report.reasons
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_CREATE_REJECTED


# ---------------------------------------------------------------------------
# A create is never "proven" without an exact readback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reader_kwargs,create_response,expected_reason",
    [
        # The exact GET never answered.
        ({"order_error": EasyWeekRetryableError("down", attempts=3)}, None, REASON_ORDER_READBACK_FAILED),
        ({"order_error": EasyWeekNotFoundError("gone", status_code=404)}, None, REASON_ORDER_READBACK_FAILED),
        # The exact order is unreadable.
        ({"order": {"not": "an order"}}, None, REASON_ORDER_SHAPE_UNPROVEN),
        # Somebody else's order.
        ({"order": open_order(marker=MARKER, customer={"uuid": OTHER_UUID})}, None, REASON_ORDER_CUSTOMER_UNPROVEN),
        ({"order": open_order(marker="somebody-elses")}, None, REASON_ORDER_MARKER_UNPROVEN),
        # Not the state a fresh create should be in.
        ({"order": paid_order(marker=MARKER)}, None, REASON_ORDER_NOT_OPEN),
        ({"order": cancelled_order(marker=MARKER)}, None, REASON_ORDER_NOT_OPEN),
        # The template read failed or the frozen configuration moved.
    ],
)
async def test_a_create_without_a_clean_readback_is_unknown(
    session_maker, reader_kwargs, create_response, expected_reason
) -> None:
    reader = FakeReader(**reader_kwargs)
    mutator = FakeMutator(create_response=create_response)

    report = await _create(session_maker, reader, mutator)

    assert report.outcome == OUTCOME_UNKNOWN_MUTATION
    assert expected_reason in report.reasons
    assert report.reconciliation_required is True
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == ledger_module.STATUS_CREATE_UNKNOWN
    assert snapshot.stage_timestamps["create_verified_at"] is None


@pytest.mark.asyncio
async def test_a_noncanonical_returned_uuid_is_never_created(session_maker) -> None:
    mutator = FakeMutator(create_response={"uuid": ORDER_UUID.upper(), "status": "open"})
    report = await _create(session_maker, FakeReader(order=open_order(marker=MARKER)), mutator)

    assert report.outcome == OUTCOME_UNKNOWN_MUTATION
    assert REASON_ORDER_UUID_UNCANONICAL in report.reasons
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == ledger_module.STATUS_CREATE_UNKNOWN
    assert snapshot.target_order_uuid is None


@pytest.mark.asyncio
async def test_a_2xx_without_an_order_identity_is_unknown(session_maker) -> None:
    mutator = FakeMutator(create_response={"ok": True})
    report = await _create(session_maker, FakeReader(), mutator)

    assert report.outcome == OUTCOME_UNKNOWN_MUTATION
    assert REASON_ORDER_UUID_UNCANONICAL in report.reasons
    assert (await ledger_module.load(session_maker)).target_order_uuid is None


class _LateTemplateReader(FakeReader):
    """Behaves normally until the post-POST readback, then misbehaves.

    Two template reads happen before the POST — one for the operator's plan and
    one for the plan the command recomputes — so the interesting read is the
    third.
    """

    def __init__(self, *, late: Any, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.late = late
        self._template_reads = 0

    async def get_voucher_template(self, voucher_template_uuid: str) -> dict[str, Any]:
        self._template_reads += 1
        self.calls.append("get_voucher_template")
        if self._template_reads <= 2:
            return TEMPLATE
        if isinstance(self.late, Exception):
            raise self.late
        return self.late


@pytest.mark.asyncio
async def test_a_template_read_that_fails_after_the_post_leaves_it_unknown(session_maker) -> None:
    reader = _LateTemplateReader(late=EasyWeekRetryableError("down", attempts=3), order=open_order(marker=MARKER))
    report = await _create(session_maker, reader, FakeMutator())

    assert report.outcome == OUTCOME_UNKNOWN_MUTATION
    assert REASON_TEMPLATE_READBACK_FAILED in report.reasons
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_CREATE_UNKNOWN


@pytest.mark.asyncio
async def test_a_template_edit_during_create_leaves_it_unknown(session_maker) -> None:
    """The frozen configuration is re-read after the POST, not only before it."""
    reader = _LateTemplateReader(late={**TEMPLATE, "cost": 1600}, order=open_order(marker=MARKER))
    report = await _create(session_maker, reader, FakeMutator())

    assert report.outcome == OUTCOME_UNKNOWN_MUTATION
    assert REASON_TEMPLATE_CONFIG_DRIFT in report.reasons
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_CREATE_UNKNOWN


@pytest.mark.asyncio
async def test_counters_that_moved_during_create_do_not_block_it(session_maker) -> None:
    """Issuance moves counters. That is the product, not a template edit."""

    reader = _LateTemplateReader(late={**TEMPLATE, "vouchers_count": 1}, order=open_order(marker=MARKER))
    report = await _create(session_maker, reader, FakeMutator())

    assert report.outcome == OUTCOME_PROVEN
    assert report.template_counters == {"vouchers_count": 1, "activated_vouchers_count": 0}


# ---------------------------------------------------------------------------
# Create reconciliation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_one_marker_match_resolves_an_unknown_create_after_a_clean_readback(session_maker) -> None:
    reader = FakeReader()
    mutator = FakeMutator(create_error=EasyWeekVoucherMutationUnknown("lost", attempts=1))
    await _create(session_maker, reader, mutator)

    reader.order_pages = _marker_pages()
    reader.order = open_order(marker=MARKER)
    reader.reconciliation_mode()
    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_PROVEN
    assert (await ledger_module.load(session_maker)).target_order_uuid == ORDER_UUID
    assert mutator.calls == ["create"]


@pytest.mark.asyncio
async def test_one_match_whose_readback_is_dirty_stays_unresolved(session_maker) -> None:
    """Finding a candidate is not the same as proving it."""
    reader = FakeReader()
    await _create(session_maker, reader, FakeMutator(create_error=EasyWeekVoucherMutationUnknown("lost", attempts=1)))

    reader.order_pages = _marker_pages()
    reader.order = open_order(marker=MARKER, customer={"uuid": OTHER_UUID})
    reader.reconciliation_mode()
    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_UNKNOWN_MUTATION
    assert REASON_ORDER_CUSTOMER_UNPROVEN in report.reasons
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_CREATE_UNKNOWN


@pytest.mark.asyncio
async def test_zero_matches_stay_unresolved_and_never_mean_not_created(session_maker) -> None:
    reader = FakeReader()
    await _create(session_maker, reader, FakeMutator(create_error=EasyWeekVoucherMutationUnknown("lost", attempts=1)))

    reader.order_pages = [orders_page([])]
    reader.reconciliation_mode()
    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_UNKNOWN_MUTATION
    assert REASON_RECONCILE_UNRESOLVED in report.reasons
    assert report.manual_cleanup_required is True
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_CREATE_UNKNOWN


@pytest.mark.asyncio
async def test_two_marker_matches_are_ambiguous_and_stop_everything(session_maker) -> None:
    reader = FakeReader()
    await _create(session_maker, reader, FakeMutator(create_error=EasyWeekVoucherMutationUnknown("lost", attempts=1)))

    now = utcnow().isoformat()
    reader.order_pages = [
        orders_page(
            [
                listed_order(marker=MARKER, created_at=now),
                listed_order(marker=MARKER, created_at=now, uuid=OTHER_UUID),
            ]
        )
    ]
    reader.reconciliation_mode()
    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_AMBIGUOUS
    assert REASON_RECONCILE_AMBIGUOUS in report.reasons
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_AMBIGUOUS


@pytest.mark.asyncio
async def test_the_walk_follows_published_pagination(session_maker) -> None:
    reader = FakeReader()
    await _create(session_maker, reader, FakeMutator(create_error=EasyWeekVoucherMutationUnknown("lost", attempts=1)))

    now = utcnow().isoformat()
    reader.order_pages = [
        orders_page([listed_order(marker="unrelated-one")], page=1, last_page=3),
        orders_page([], page=2, last_page=3),
        orders_page([listed_order(marker=MARKER, created_at=now)], page=3, last_page=3),
    ]
    reader.order = open_order(marker=MARKER)
    reader.reconciliation_mode()
    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_PROVEN
    assert "list_orders:3" in reader.calls


@pytest.mark.asyncio
async def test_an_incomplete_walk_is_unresolved_not_absent(session_maker) -> None:
    class Endless(FakeReader):
        async def list_location_orders(self, **kwargs: Any) -> dict[str, Any]:
            if not self._claimed:
                return orders_page([])
            return {"data": [listed_order(marker="unrelated")]}

    reader = Endless()
    await _create(session_maker, reader, FakeMutator(create_error=EasyWeekVoucherMutationUnknown("lost", attempts=1)))
    reader.reconciliation_mode()

    report = await run_reconcile(session_maker, reader, identity=IDENTITY)
    assert report.outcome == OUTCOME_UNKNOWN_MUTATION


@pytest.mark.asyncio
async def test_find_marker_orders_reports_an_incomplete_walk() -> None:
    from datetime import timedelta

    class Endless(FakeReader):
        async def list_location_orders(self, **kwargs: Any) -> dict[str, Any]:
            return {"data": [listed_order(marker="unrelated")]}

    now = utcnow()
    match = await find_marker_orders(
        Endless(),
        location_uuid=KARLSRUHE_LOCATION_UUID,
        customer_uuid=CUSTOMER_UUID,
        staffer_uuid=STAFFER_UUID,
        marker=MARKER,
        window_start=now - timedelta(days=1),
        window_end=now + timedelta(days=1),
    )
    assert match.complete is False
    assert match.resolved is False


# ---------------------------------------------------------------------------
# Pay
# ---------------------------------------------------------------------------


async def _created(session_maker, reader: FakeReader, **mutator_kwargs: Any) -> FakeMutator:
    mutator = FakeMutator(reader, **mutator_kwargs)
    reader.order = open_order(marker=MARKER)
    await _create(session_maker, reader, mutator)
    return mutator


@pytest.mark.asyncio
async def test_a_proven_pay_sends_exactly_one_request_with_no_amount(session_maker) -> None:
    reader = FakeReader()
    mutator = await _created(session_maker, reader)

    report = await _pay(session_maker, reader, mutator)

    assert report.outcome == OUTCOME_PROVEN, report.reasons
    assert report.order_state == ORDER_PAID
    assert report.payment_proof == PAYMENT_PROOF_STATUS
    assert mutator.calls == ["create", "pay"]
    # The documented pay body carries an account and nothing else.
    assert set(mutator.pay_kwargs[0]) == {"order_uuid", "account_uuid"}
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_PAID


@pytest.mark.asyncio
async def test_pay_is_refused_while_the_create_is_unresolved(session_maker) -> None:
    reader = FakeReader()
    mutator = FakeMutator(create_error=EasyWeekVoucherMutationUnknown("lost", attempts=1))
    await _create(session_maker, reader, mutator)

    report = await _pay(session_maker, reader, RefusingMutator())

    assert report.outcome == OUTCOME_REFUSED
    assert report.reconciliation_required is True


@pytest.mark.asyncio
async def test_an_unknown_pay_is_never_repeated(session_maker) -> None:
    reader = FakeReader()
    mutator = await _created(session_maker, reader)
    mutator.pay_error = EasyWeekVoucherMutationUnknown("lost", attempts=1)

    report = await _pay(session_maker, reader, mutator)
    assert report.outcome == OUTCOME_UNKNOWN_MUTATION
    assert mutator.calls == ["create", "pay"]

    again = await _pay(session_maker, reader, mutator)
    assert again.outcome == OUTCOME_REFUSED
    assert mutator.calls == ["create", "pay"]


@pytest.mark.asyncio
async def test_a_2xx_pay_whose_readback_is_not_paid_is_unknown(session_maker) -> None:
    reader = FakeReader()
    # The POST answers 2xx but the order never actually settles.
    mutator = await _created(session_maker, reader, pay_effect=False)

    report = await _pay(session_maker, reader, mutator)

    assert report.outcome == OUTCOME_UNKNOWN_MUTATION
    assert REASON_RECONCILE_UNRESOLVED in report.reasons
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_PAY_UNKNOWN


@pytest.mark.asyncio
async def test_a_rejected_pay_leaves_an_open_draft_to_clean_up(session_maker) -> None:
    reader = FakeReader()
    mutator = await _created(session_maker, reader)
    mutator.pay_error = EasyWeekAuthError("auth", status_code=403)

    report = await _pay(session_maker, reader, mutator)

    assert report.outcome == OUTCOME_CONTRACT_MISMATCH
    assert report.manual_cleanup_required is True
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_PAY_REJECTED


# ---------------------------------------------------------------------------
# Monotonic reconciliation — no POST is ever handed back
# ---------------------------------------------------------------------------


def test_the_transition_table_never_walks_a_stage_backwards() -> None:
    # A pay in flight reads open: that is unknown, never `created`.
    assert reconcile_transition(ledger_module.STATUS_PAY_CLAIMED, ORDER_OPEN).status == (
        ledger_module.STATUS_PAY_UNKNOWN
    )
    # An unknown pay reading open stays exactly where it is.
    assert reconcile_transition(ledger_module.STATUS_PAY_UNKNOWN, ORDER_OPEN).status is None
    # A refund in flight reads paid: that is not a reason to become payable again.
    assert reconcile_transition(ledger_module.STATUS_REFUND_CLAIMED, ORDER_PAID).status is None
    assert reconcile_transition(ledger_module.STATUS_REFUND_UNKNOWN, ORDER_PAID).status is None
    # Forward moves are allowed.
    assert reconcile_transition(ledger_module.STATUS_PAY_CLAIMED, ORDER_PAID).status == ledger_module.STATUS_PAID
    assert reconcile_transition(ledger_module.STATUS_REFUND_UNKNOWN, ORDER_REFUNDED).status == (
        ledger_module.STATUS_REFUNDED
    )
    # A malformed body changes nothing at all.
    assert reconcile_transition(ledger_module.STATUS_PAID, ORDER_MALFORMED).status is None


# Every (ledger status, remote state) pair, and what it is allowed to do. This
# is the whole contract in one place: a transition absent from here must be a
# no-op, and a transition present here must land exactly where it says.
#
# `None` = observe and leave the row alone. The third column is the verification
# timestamp, which is stamped only where THIS canary's operation may have had
# the observed effect — a later dashboard action is observation, not our proof.
_EXPECTED_TRANSITIONS: dict[tuple[str, str], tuple[str | None, str | None]] = {
    # --- created ----------------------------------------------------------
    (ledger_module.STATUS_CREATED, ORDER_OPEN): (None, None),
    (ledger_module.STATUS_CREATED, ORDER_PAID): (ledger_module.STATUS_PAID, None),
    (ledger_module.STATUS_CREATED, ORDER_REFUNDED): (ledger_module.STATUS_REFUNDED, None),
    (ledger_module.STATUS_CREATED, ORDER_CANCELLED): (ledger_module.STATUS_MANUALLY_CLEANED, None),
    # --- a payment that may be in flight -----------------------------------
    (ledger_module.STATUS_PAY_CLAIMED, ORDER_OPEN): (ledger_module.STATUS_PAY_UNKNOWN, None),
    (ledger_module.STATUS_PAY_CLAIMED, ORDER_PAID): (ledger_module.STATUS_PAID, "pay_verified_at"),
    (ledger_module.STATUS_PAY_CLAIMED, ORDER_REFUNDED): (ledger_module.STATUS_REFUNDED, None),
    (ledger_module.STATUS_PAY_CLAIMED, ORDER_CANCELLED): (None, None),
    (ledger_module.STATUS_PAY_UNKNOWN, ORDER_OPEN): (None, None),
    (ledger_module.STATUS_PAY_UNKNOWN, ORDER_PAID): (ledger_module.STATUS_PAID, "pay_verified_at"),
    # The one the review called out: an unknown payment over an order that has
    # since been refunded must not stay unknown forever.
    (ledger_module.STATUS_PAY_UNKNOWN, ORDER_REFUNDED): (ledger_module.STATUS_REFUNDED, None),
    (ledger_module.STATUS_PAY_UNKNOWN, ORDER_CANCELLED): (None, None),
    # --- a payment that provably did not happen ----------------------------
    (ledger_module.STATUS_PAY_REJECTED, ORDER_OPEN): (None, None),
    (ledger_module.STATUS_PAY_REJECTED, ORDER_PAID): (ledger_module.STATUS_PAID, None),
    (ledger_module.STATUS_PAY_REJECTED, ORDER_REFUNDED): (ledger_module.STATUS_REFUNDED, None),
    (ledger_module.STATUS_PAY_REJECTED, ORDER_CANCELLED): (ledger_module.STATUS_MANUALLY_CLEANED, None),
    # --- paid --------------------------------------------------------------
    (ledger_module.STATUS_PAID, ORDER_OPEN): (None, None),
    (ledger_module.STATUS_PAID, ORDER_PAID): (None, None),
    (ledger_module.STATUS_PAID, ORDER_REFUNDED): (ledger_module.STATUS_REFUNDED, None),
    (ledger_module.STATUS_PAID, ORDER_CANCELLED): (None, None),
    # --- a refund that may be in flight ------------------------------------
    (ledger_module.STATUS_REFUND_CLAIMED, ORDER_OPEN): (None, None),
    (ledger_module.STATUS_REFUND_CLAIMED, ORDER_PAID): (None, None),
    (ledger_module.STATUS_REFUND_CLAIMED, ORDER_REFUNDED): (
        ledger_module.STATUS_REFUNDED,
        "refund_verified_at",
    ),
    (ledger_module.STATUS_REFUND_CLAIMED, ORDER_CANCELLED): (None, None),
    (ledger_module.STATUS_REFUND_UNKNOWN, ORDER_OPEN): (None, None),
    (ledger_module.STATUS_REFUND_UNKNOWN, ORDER_PAID): (None, None),
    (ledger_module.STATUS_REFUND_UNKNOWN, ORDER_REFUNDED): (
        ledger_module.STATUS_REFUNDED,
        "refund_verified_at",
    ),
    (ledger_module.STATUS_REFUND_UNKNOWN, ORDER_CANCELLED): (None, None),
    # --- a refund that provably did not happen -----------------------------
    (ledger_module.STATUS_REFUND_REJECTED, ORDER_OPEN): (None, None),
    (ledger_module.STATUS_REFUND_REJECTED, ORDER_PAID): (None, None),
    (ledger_module.STATUS_REFUND_REJECTED, ORDER_REFUNDED): (
        ledger_module.STATUS_REFUNDED,
        None,
    ),
    (ledger_module.STATUS_REFUND_REJECTED, ORDER_CANCELLED): (None, None),
}

_TERMINAL_STATUSES = (
    ledger_module.STATUS_REFUNDED,
    ledger_module.STATUS_MANUALLY_CLEANED,
    ledger_module.STATUS_AMBIGUOUS,
)
_REMOTE_STATES = (ORDER_OPEN, ORDER_PAID, ORDER_REFUNDED, ORDER_CANCELLED, ORDER_UNKNOWN, ORDER_MALFORMED)


@pytest.mark.parametrize("pair,expected", sorted(_EXPECTED_TRANSITIONS.items()))
def test_every_declared_transition_lands_exactly_where_it_says(pair, expected) -> None:
    status, remote = pair
    target, verified = expected

    transition = reconcile_transition(status, remote)

    assert transition.status == target
    assert transition.verified_field == verified
    if target is not None:
        # A compare-and-set, always: the write is valid only from the state it
        # was decided on, so a row that moved meanwhile is not overwritten.
        assert transition.expected_from == frozenset({status})
        assert ledger_module.STATUS_RANK[target] > ledger_module.STATUS_RANK[status]


@pytest.mark.parametrize("status", ledger_module.ALL_STATUSES)
@pytest.mark.parametrize("remote", _REMOTE_STATES)
def test_no_transition_outside_the_declared_table_exists(status, remote) -> None:
    """Whatever is not written down above must do nothing at all."""
    expected = _EXPECTED_TRANSITIONS.get((status, remote), (None, None))
    assert reconcile_transition(status, remote).status == expected[0]


@pytest.mark.parametrize("status", _TERMINAL_STATUSES)
@pytest.mark.parametrize("remote", _REMOTE_STATES)
def test_nothing_moves_out_of_a_terminal_state(status, remote) -> None:
    assert reconcile_transition(status, remote).status is None


@pytest.mark.parametrize("status", ledger_module.ALL_STATUSES)
@pytest.mark.parametrize("remote", _REMOTE_STATES)
def test_no_reading_ever_returns_a_stage_to_a_claimable_state(status, remote) -> None:
    """The invariant the whole table exists for: no second POST, ever.

    A row whose payment may already have gone out must never land back where
    that same payment could be claimed again. Moving FORWARD into a state that
    opens the NEXT stage is the opposite thing and exactly what should happen:
    a proven payment has to leave the refund reachable.
    """
    target = reconcile_transition(status, remote).status
    if target is None:
        return

    reclaimable = {
        "create": ledger_module.CREATE_CLAIMABLE_FROM,
        "pay": ledger_module.PAY_CLAIMABLE_FROM,
        "refund": ledger_module.REFUND_CLAIMABLE_FROM,
    }
    for stage, claimable in reclaimable.items():
        if status.startswith(stage):
            assert target not in claimable


@pytest.mark.parametrize("status", ledger_module.ALL_STATUSES)
@pytest.mark.parametrize("remote", _REMOTE_STATES)
def test_a_verification_stamp_only_appears_where_our_operation_may_have_acted(status, remote) -> None:
    """`pay_verified_at` means "the pay WE sent is confirmed" and nothing else.

    Stamping it because somebody settled the order in the dashboard would
    attribute a stranger's action to this canary in the durable record.
    """
    field = reconcile_transition(status, remote).verified_field
    if field is None:
        return

    effect_possible_from = {
        "pay_verified_at": {ledger_module.STATUS_PAY_CLAIMED, ledger_module.STATUS_PAY_UNKNOWN},
        "refund_verified_at": {
            ledger_module.STATUS_REFUND_CLAIMED,
            ledger_module.STATUS_REFUND_UNKNOWN,
        },
    }
    assert status in effect_possible_from[field]


@pytest.mark.asyncio
async def test_a_reconcile_during_an_in_flight_pay_never_makes_pay_claimable(session_maker) -> None:
    """The POST is out; the order still reads open. A retry must stay impossible."""
    reader = FakeReader()
    await _created(session_maker, reader)

    # Simulate the claim being committed and the POST still in flight.
    await ledger_module.claim_pay(
        session_maker,
        pay_plan_digest="a" * 64,
        template_config_digest=(await ledger_module.load(session_maker)).template_config_digest or "",
        identity_fingerprints=IDENTITY.fingerprints,
    )
    reader.order = open_order(marker=MARKER)

    report = await run_reconcile(session_maker, reader, identity=IDENTITY)
    snapshot = await ledger_module.load(session_maker)

    assert snapshot.status == ledger_module.STATUS_PAY_UNKNOWN
    assert report.reconciliation_required is True
    # The attempt is still on the record, and pay is not claimable again.
    assert snapshot.stage_timestamps["pay_attempted_at"] is not None
    refused = await ledger_module.claim_pay(
        session_maker,
        pay_plan_digest="a" * 64,
        template_config_digest=snapshot.template_config_digest or "",
        identity_fingerprints=IDENTITY.fingerprints,
    )
    assert refused.granted is False


@pytest.mark.asyncio
async def test_a_reconcile_during_an_in_flight_refund_never_makes_refund_claimable(session_maker) -> None:
    reader = FakeReader()
    await _paid(session_maker, reader)

    config = (await ledger_module.load(session_maker)).template_config_digest or ""
    await ledger_module.claim_refund(
        session_maker,
        refund_plan_digest="b" * 64,
        template_config_digest=config,
        identity_fingerprints=IDENTITY.fingerprints,
    )

    # The refund POST is in flight; the order still reads paid.
    report = await run_reconcile(session_maker, reader, identity=IDENTITY)
    snapshot = await ledger_module.load(session_maker)

    assert snapshot.status == ledger_module.STATUS_REFUND_CLAIMED
    assert report.outcome == OUTCOME_ROLLBACK_UNPROVEN
    assert report.remote_rollback_proven is False
    refused = await ledger_module.claim_refund(
        session_maker,
        refund_plan_digest="b" * 64,
        template_config_digest=config,
        identity_fingerprints=IDENTITY.fingerprints,
    )
    assert refused.granted is False


@pytest.mark.asyncio
async def test_a_stale_original_response_cannot_overwrite_a_newer_state(session_maker) -> None:
    """A slow `created` write must not undo a reconciliation that moved on."""
    reader = FakeReader()
    await _created(session_maker, reader)
    config = (await ledger_module.load(session_maker)).template_config_digest or ""
    await ledger_module.claim_pay(
        session_maker,
        pay_plan_digest="a" * 64,
        template_config_digest=config,
        identity_fingerprints=IDENTITY.fingerprints,
    )

    stale = await ledger_module.record_outcome(
        session_maker,
        status=ledger_module.STATUS_CREATED,
        expected_statuses=frozenset({ledger_module.STATUS_CREATE_CLAIMED}),
        verified_field="create_verified_at",
    )

    assert stale.applied is False
    assert stale.reason == ledger_module.RECORD_STALE_STATE
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_PAY_CLAIMED


@pytest.mark.asyncio
async def test_a_write_that_would_lower_the_rank_is_refused(session_maker) -> None:
    reader = FakeReader()
    await _created(session_maker, reader)
    config = (await ledger_module.load(session_maker)).template_config_digest or ""
    await ledger_module.claim_pay(
        session_maker,
        pay_plan_digest="a" * 64,
        template_config_digest=config,
        identity_fingerprints=IDENTITY.fingerprints,
    )

    regressive = await ledger_module.record_outcome(
        session_maker,
        status=ledger_module.STATUS_CREATED,
        # Even a caller that names the right expected state cannot go back.
        expected_statuses=frozenset({ledger_module.STATUS_PAY_CLAIMED}),
    )

    assert regressive.applied is False
    assert regressive.reason == ledger_module.RECORD_WOULD_REGRESS


@pytest.mark.asyncio
async def test_two_processes_together_send_at_most_one_pay(session_maker) -> None:
    reader = FakeReader()
    await _created(session_maker, reader)
    config = (await ledger_module.load(session_maker)).template_config_digest or ""

    first, second = await asyncio.gather(
        ledger_module.claim_pay(
            session_maker,
            pay_plan_digest="a" * 64,
            template_config_digest=config,
            identity_fingerprints=IDENTITY.fingerprints,
        ),
        ledger_module.claim_pay(
            session_maker,
            pay_plan_digest="a" * 64,
            template_config_digest=config,
            identity_fingerprints=IDENTITY.fingerprints,
        ),
    )
    assert sorted([first.granted, second.granted]) == [False, True]


@pytest.mark.asyncio
async def test_a_pay_timeout_then_reconcile_open_never_sends_a_second_pay(session_maker) -> None:
    reader = FakeReader()
    mutator = await _created(session_maker, reader)
    mutator.pay_error = EasyWeekVoucherMutationUnknown("lost", attempts=1)
    await _pay(session_maker, reader, mutator)

    reader.order = open_order(marker=MARKER)
    await run_reconcile(session_maker, reader, identity=IDENTITY)

    again = await _pay(session_maker, reader, mutator)
    assert again.outcome == OUTCOME_REFUSED
    assert mutator.calls == ["create", "pay"]


@pytest.mark.asyncio
async def test_a_refund_timeout_then_reconcile_paid_never_sends_a_second_refund(session_maker) -> None:
    reader = FakeReader()
    mutator = await _paid(session_maker, reader)

    mutator.refund_error = EasyWeekVoucherMutationUnknown("lost", attempts=1)
    await _refund(session_maker, reader, mutator)
    assert mutator.calls == ["create", "pay", "refund"]

    report = await run_reconcile(session_maker, reader, identity=IDENTITY)
    assert report.outcome == OUTCOME_ROLLBACK_UNPROVEN
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_REFUND_UNKNOWN

    again = await _refund(session_maker, reader, mutator)
    assert again.outcome == OUTCOME_REFUSED
    assert mutator.calls == ["create", "pay", "refund"]


# ---------------------------------------------------------------------------
# Refund
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_order_that_is_not_the_authorised_one_is_refused_before_the_claim(session_maker) -> None:
    """Our marker, our customer, the wrong contents. Nothing may be sent.

    The refusal has to land before the claim, not after: a claim taken on an
    order we then decline to pay for would burn the one payment this canary is
    allowed to attempt.
    """
    reader = FakeReader()
    mutator = await _created(session_maker, reader)
    reader.order = open_order(
        marker=MARKER,
        vouchers=[],
        invoice={"total": 9900, "subtotal": 9900, "amount_due": 9900, "amount_paid": 0},
    )

    report = await _pay(session_maker, reader, mutator)

    assert report.outcome == OUTCOME_REFUSED
    assert report.external_mutation_attempted is False
    assert mutator.calls == ["create"]
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == ledger_module.STATUS_CREATED
    assert snapshot.stage_timestamps["pay_claimed_at"] is None


@pytest.mark.asyncio
async def test_a_payment_rejected_by_the_endpoint_can_be_tried_again_by_hand(session_maker) -> None:
    """A fixable rejection must not strand an open draft order.

    The retry is manual all the way: a new plan, a new digest, a new issued_at
    and a new phrase. What this proves is that the path EXISTS — that a proven
    rejection leaves the ledger somewhere a fresh authorisation can act on.
    """
    reader = FakeReader()
    mutator = await _created(session_maker, reader)
    mutator.pay_error = EasyWeekPermanentError("rejected", status_code=422, attempts=1)
    first_plan = await _operator_plan(reader, STAGE_PAY, session_maker, now=utcnow() - timedelta(seconds=2))

    first = await run_pay(
        session_maker,
        reader,
        mutator,
        identity=IDENTITY,
        enabled=True,
        plan_digest=first_plan.digest,
        plan_issued_at=first_plan.issued_at,
        confirmation_phrase=first_plan.confirmation_phrase,
    )

    assert first.outcome == OUTCOME_CONTRACT_MISMATCH
    rejected = await ledger_module.load(session_maker)
    assert rejected.status == ledger_module.STATUS_PAY_REJECTED
    assert rejected.stage_plan_digests[STAGE_PAY] == first_plan.digest

    # The operator fixes the cause and authorises the stage again, from scratch.
    mutator.pay_error = None
    second_plan = await _operator_plan(reader, STAGE_PAY, session_maker, now=utcnow() - timedelta(seconds=1))
    assert second_plan.digest != first_plan.digest
    second = await run_pay(
        session_maker,
        reader,
        mutator,
        identity=IDENTITY,
        enabled=True,
        plan_digest=second_plan.digest,
        plan_issued_at=second_plan.issued_at,
        confirmation_phrase=second_plan.confirmation_phrase,
    )

    assert second.outcome == OUTCOME_PROVEN
    assert mutator.calls == ["create", "pay", "pay"]
    retried = await ledger_module.load(session_maker)
    assert retried.status == ledger_module.STATUS_PAID
    assert retried.stage_plan_digests[STAGE_PAY] == second_plan.digest


@pytest.mark.asyncio
async def test_a_retry_still_needs_its_own_fresh_authorisation(session_maker) -> None:
    """The first attempt's approval is spent. Nothing is retried by itself."""
    reader = FakeReader()
    mutator = await _created(session_maker, reader)
    spent = await _authorised(reader, STAGE_PAY, session_maker)
    mutator.pay_error = EasyWeekPermanentError("rejected", status_code=422, attempts=1)
    await _pay(session_maker, reader, mutator, **spent)

    mutator.pay_error = None
    replayed = await run_pay(session_maker, reader, mutator, identity=IDENTITY, enabled=True, **spent)

    assert replayed.outcome == OUTCOME_REFUSED
    assert CANARY_PLAN_DIGEST_MISMATCH in replayed.reasons
    assert mutator.calls == ["create", "pay"]


@pytest.mark.asyncio
async def test_an_unknown_payment_is_never_offered_a_retry(session_maker) -> None:
    """The difference the whole rejection classification exists to keep."""
    reader = FakeReader()
    mutator = await _created(session_maker, reader)
    mutator.pay_error = EasyWeekVoucherMutationUnknown("lost", attempts=1)

    await _pay(session_maker, reader, mutator)
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_PAY_UNKNOWN

    mutator.pay_error = None
    again = await _pay(session_maker, reader, mutator)

    assert again.outcome == OUTCOME_REFUSED
    assert mutator.calls == ["create", "pay"]


async def _paid(session_maker, reader: FakeReader, **mutator_kwargs: Any) -> FakeMutator:
    mutator = await _created(session_maker, reader, **mutator_kwargs)
    await _pay(session_maker, reader, mutator)
    return mutator


@pytest.mark.asyncio
async def test_each_delayed_stage_claim_stores_the_exact_operator_digest(session_maker) -> None:
    """The live T2 plan proves T1 facts; the ledger keeps T1's approval."""
    reader = FakeReader()
    mutator = FakeMutator(reader)

    for stage, runner in (
        (STAGE_CREATE, run_create),
        (STAGE_PAY, run_pay),
        (STAGE_REFUND, run_refund),
    ):
        t1 = utcnow() - timedelta(seconds=2)
        operator_plan = await _operator_plan(reader, stage, session_maker, now=t1)
        fresh_plan = await _operator_plan(reader, stage, session_maker, now=t1 + timedelta(microseconds=1))

        assert operator_plan.ready is True
        assert fresh_plan.ready is True
        assert fresh_plan.digest != operator_plan.digest
        assert fresh_plan.digest_for(operator_plan.issued_at) == operator_plan.digest

        report = await runner(
            session_maker,
            reader,
            mutator,
            identity=IDENTITY,
            enabled=True,
            plan_digest=operator_plan.digest,
            plan_issued_at=operator_plan.issued_at,
            confirmation_phrase=operator_plan.confirmation_phrase,
        )

        assert report.outcome == OUTCOME_PROVEN, report.reasons
        snapshot = await ledger_module.load(session_maker)
        assert snapshot.stage_plan_digests[stage] == operator_plan.digest


async def _ready_stage(session_maker, stage: str) -> tuple[FakeReader, FakeMutator]:
    reader = FakeReader()
    if stage == STAGE_CREATE:
        return reader, FakeMutator(reader)

    mutator = await _created(session_maker, reader)
    if stage == STAGE_REFUND:
        await _pay(session_maker, reader, mutator)
    return reader, mutator


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", [STAGE_CREATE, STAGE_PAY, STAGE_REFUND])
@pytest.mark.parametrize("denial", ["phrase", "digest", "expired"])
async def test_a_denied_approval_never_claims_or_mutates_any_stage(session_maker, stage, denial) -> None:
    reader, mutator = await _ready_stage(session_maker, stage)
    issued_at = utcnow() - timedelta(days=1) if denial == "expired" else utcnow() - timedelta(seconds=1)
    operator_plan = await _operator_plan(reader, stage, session_maker, now=issued_at)
    inputs = {
        "plan_digest": operator_plan.digest,
        "plan_issued_at": operator_plan.issued_at,
        "confirmation_phrase": operator_plan.confirmation_phrase,
    }
    if denial == "phrase":
        inputs["confirmation_phrase"] = "not-the-authorised-phrase"
    elif denial == "digest":
        inputs["plan_digest"] = "0" * 64

    before = (await ledger_module.load(session_maker)).as_safe_dict()
    calls_before = list(mutator.calls)
    runner = {STAGE_CREATE: run_create, STAGE_PAY: run_pay, STAGE_REFUND: run_refund}[stage]

    report = await runner(
        session_maker,
        reader,
        mutator,
        identity=IDENTITY,
        enabled=True,
        **inputs,
    )

    assert report.outcome == OUTCOME_REFUSED
    assert report.external_mutation_attempted is False
    assert mutator.calls == calls_before
    assert (await ledger_module.load(session_maker)).as_safe_dict() == before


@pytest.mark.asyncio
async def test_a_proven_refund_sends_exactly_one_request(session_maker) -> None:
    reader = FakeReader()
    mutator = await _paid(session_maker, reader)

    report = await _refund(session_maker, reader, mutator)

    assert report.outcome == OUTCOME_PROVEN, report.reasons
    assert report.remote_rollback_proven is True
    assert mutator.calls == ["create", "pay", "refund"]
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_REFUNDED


@pytest.mark.asyncio
async def test_a_refund_stays_available_after_an_unreadable_artifact(session_maker) -> None:
    """Cleanup beats research: a broken voucher must not strand a real payment."""
    unreadable = paid_order(marker=MARKER, vouchers=[{"???": {"nested": [1, 2, 3]}}])
    reader = FakeReader()
    mutator = await _created(session_maker, reader, pay_effect=False)

    async def settle_unreadably(**kwargs: Any) -> VoucherMutationResponse:
        mutator.calls.append("pay")
        reader.order = unreadable
        return VoucherMutationResponse(http_status=200, envelope=unreadable)

    mutator.pay_voucher_order = settle_unreadably  # type: ignore[method-assign]
    pay_report = await _pay(session_maker, reader, mutator)
    assert pay_report.outcome == OUTCOME_PROVEN, pay_report.reasons

    refund_report = await _refund(session_maker, reader, mutator)

    assert refund_report.outcome == OUTCOME_PROVEN, refund_report.reasons
    assert refund_report.remote_rollback_proven is True


@pytest.mark.asyncio
async def test_a_refund_whose_readback_is_still_paid_is_rollback_unproven(session_maker) -> None:
    reader = FakeReader()
    mutator = await _paid(session_maker, reader, refund_effect=False)

    report = await _refund(session_maker, reader, mutator)

    assert report.outcome == OUTCOME_ROLLBACK_UNPROVEN
    assert REASON_ROLLBACK_UNPROVEN in report.reasons
    assert report.remote_rollback_proven is False


# ---------------------------------------------------------------------------
# Manual cleanup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_open_draft_asks_for_a_manual_cleanup(session_maker) -> None:
    reader = FakeReader()
    await _created(session_maker, reader)

    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_MANUAL_CLEANUP
    assert report.manual_cleanup_required is True
    assert report.ledger["reconciliation_marker"] == MARKER


@pytest.mark.asyncio
async def test_a_manual_cancellation_is_observed_never_attributed_to_the_tool(session_maker) -> None:
    reader = FakeReader()
    await _created(session_maker, reader)

    reader.order = cancelled_order(marker=MARKER)
    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_PROVEN
    assert report.order_state == ORDER_CANCELLED
    assert report.remote_rollback_proven is False
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == ledger_module.STATUS_MANUALLY_CLEANED
    assert snapshot.manual_cleanup_observed_at is not None
    assert snapshot.stage_timestamps["refund_attempted_at"] is None


@pytest.mark.asyncio
async def test_the_canary_never_invents_a_cancel_endpoint() -> None:
    from altegio_bot import easyweek_voucher_mutation as mutation_module
    from altegio_bot.easyweek_voucher_canary import runner as runner_module
    from altegio_bot.tests.easyweek_voucher_canary_fixtures import code_without_docstrings

    for module in (mutation_module, runner_module):
        code = code_without_docstrings(module)
        for forbidden in ('"DELETE"', "'DELETE'", '"PATCH"', "'PATCH'", "/cancel"):
            assert forbidden not in code, (module.__name__, forbidden)


# ---------------------------------------------------------------------------
# Reconcile and status are reads
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reconcile_is_safe_to_repeat(session_maker) -> None:
    reader = FakeReader()
    mutator = await _paid(session_maker, reader)
    reader.order = refunded_order(marker=MARKER)

    first = await run_reconcile(session_maker, reader, identity=IDENTITY)
    second = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert first.outcome == second.outcome == OUTCOME_PROVEN
    assert mutator.calls == ["create", "pay"]


@pytest.mark.asyncio
async def test_reconcile_with_no_row_refuses_rather_than_guessing(session_maker) -> None:
    report = await run_reconcile(session_maker, FakeReader(), identity=IDENTITY)
    assert report.outcome == OUTCOME_REFUSED


@pytest.mark.asyncio
async def test_status_is_database_only(session_maker) -> None:
    reader = FakeReader()
    await _created(session_maker, reader)
    before = list(reader.calls)

    report = await run_status(session_maker)

    assert report.outcome == OUTCOME_PROVEN
    assert report.manual_cleanup_required is True
    assert reader.calls == before


@pytest.mark.asyncio
async def test_a_reconcile_read_failure_is_unknown_not_a_verdict(session_maker) -> None:
    reader = FakeReader()
    await _created(session_maker, reader)
    reader.order_error = EasyWeekRetryableError("down", attempts=3)

    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_UNKNOWN_MUTATION
    assert report.reconciliation_required is True


async def _claim_pay(session_maker, outcome_status: str) -> None:
    """Take the pay claim the way a real run does, then record its outcome.

    Going through the claim matters: it is what stamps `pay_attempted_at`, and
    the database refuses a verification timestamp on a stage that was never
    attempted — which is the whole point of that constraint.
    """
    snapshot = await ledger_module.load(session_maker)
    await ledger_module.claim_pay(
        session_maker,
        pay_plan_digest="a" * 64,
        template_config_digest=snapshot.template_config_digest or "",
        identity_fingerprints=IDENTITY.fingerprints,
    )
    await ledger_module.record_outcome(
        session_maker,
        status=outcome_status,
        expected_statuses=frozenset({ledger_module.STATUS_PAY_CLAIMED}),
    )


async def _claim_refund(session_maker, outcome_status: str) -> None:
    snapshot = await ledger_module.load(session_maker)
    await ledger_module.claim_refund(
        session_maker,
        refund_plan_digest="b" * 64,
        template_config_digest=snapshot.template_config_digest or "",
        identity_fingerprints=IDENTITY.fingerprints,
    )
    await ledger_module.record_outcome(
        session_maker,
        status=outcome_status,
        expected_statuses=frozenset({ledger_module.STATUS_REFUND_CLAIMED}),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ledger_status,remote_state,expected_status,pay_verified,refund_verified",
    [
        (ledger_module.STATUS_CREATED, ORDER_PAID, ledger_module.STATUS_PAID, False, False),
        (ledger_module.STATUS_PAY_CLAIMED, ORDER_PAID, ledger_module.STATUS_PAID, True, False),
        (ledger_module.STATUS_PAY_UNKNOWN, ORDER_PAID, ledger_module.STATUS_PAID, True, False),
        (ledger_module.STATUS_PAY_REJECTED, ORDER_PAID, ledger_module.STATUS_PAID, False, False),
        (ledger_module.STATUS_PAID, ORDER_REFUNDED, ledger_module.STATUS_REFUNDED, True, False),
        (ledger_module.STATUS_REFUND_CLAIMED, ORDER_REFUNDED, ledger_module.STATUS_REFUNDED, True, True),
        (ledger_module.STATUS_REFUND_UNKNOWN, ORDER_REFUNDED, ledger_module.STATUS_REFUNDED, True, True),
        (ledger_module.STATUS_REFUND_REJECTED, ORDER_REFUNDED, ledger_module.STATUS_REFUNDED, True, False),
    ],
)
async def test_reconcile_verification_timestamps_preserve_operation_provenance(
    session_maker,
    ledger_status,
    remote_state,
    expected_status,
    pay_verified,
    refund_verified,
) -> None:
    reader = FakeReader()
    if ledger_status == ledger_module.STATUS_CREATED:
        await _created(session_maker, reader)
    elif ledger_status in {
        ledger_module.STATUS_PAY_CLAIMED,
        ledger_module.STATUS_PAY_UNKNOWN,
        ledger_module.STATUS_PAY_REJECTED,
    }:
        await _created(session_maker, reader)
        await _claim_pay(session_maker, ledger_status)
    else:
        await _paid(session_maker, reader)
        if ledger_status != ledger_module.STATUS_PAID:
            await _claim_refund(session_maker, ledger_status)

    reader.order = paid_order(marker=MARKER) if remote_state == ORDER_PAID else refunded_order(marker=MARKER)

    await run_reconcile(session_maker, reader, identity=IDENTITY)
    snapshot = await ledger_module.load(session_maker)

    assert snapshot.status == expected_status
    assert (snapshot.stage_timestamps["pay_verified_at"] is not None) is pay_verified
    assert (snapshot.stage_timestamps["refund_verified_at"] is not None) is refund_verified


@pytest.mark.asyncio
async def test_a_reconcile_under_another_identity_reads_nothing_at_all(session_maker) -> None:
    """Not "finds nothing" — reads nothing.

    Reconciling under a different customer or staffer would walk a stranger's
    orders looking for our marker, and would then judge whatever it found
    against this ledger row. The refusal has to come before the first GET.
    """
    reader = FakeReader()
    await _created(session_maker, reader)
    before = list(reader.calls)

    foreign = RuntimeIdentity(customer_uuid=OTHER_UUID, staffer_uuid=OTHER_UUID, account_uuid=ACCOUNT_UUID)
    report = await run_reconcile(session_maker, reader, identity=foreign)

    assert report.outcome == OUTCOME_REFUSED
    assert report.reasons == [REASON_IDENTITY_BINDING_MISMATCH]
    assert reader.calls == before
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_CREATED


@pytest.mark.asyncio
async def test_a_reconcile_under_another_identity_still_refuses_while_unresolved(session_maker) -> None:
    reader = FakeReader()
    await _created(session_maker, reader)
    await _claim_pay(session_maker, ledger_module.STATUS_PAY_UNKNOWN)
    before = list(reader.calls)

    foreign = RuntimeIdentity(customer_uuid=CUSTOMER_UUID, staffer_uuid=STAFFER_UUID, account_uuid=OTHER_UUID)
    report = await run_reconcile(session_maker, reader, identity=foreign)

    assert report.outcome == OUTCOME_REFUSED
    assert report.reconciliation_required is True
    assert reader.calls == before


@pytest.mark.asyncio
async def test_a_refunded_order_resolves_a_payment_that_never_came_back(session_maker) -> None:
    """`pay_unknown` over a refunded order must not stay unknown forever."""
    reader = FakeReader()
    await _created(session_maker, reader)
    await _claim_pay(session_maker, ledger_module.STATUS_PAY_UNKNOWN)
    reader.order = refunded_order(marker=MARKER)

    report = await run_reconcile(session_maker, reader, identity=IDENTITY)
    snapshot = await ledger_module.load(session_maker)

    assert snapshot.status == ledger_module.STATUS_REFUNDED
    assert report.outcome == OUTCOME_PROVEN
    assert report.remote_rollback_proven is True
    # The refund was somebody else's doing, so it carries no verification of ours.
    assert snapshot.stage_timestamps["refund_verified_at"] is None


@pytest.mark.asyncio
async def test_a_rejected_payment_that_turns_out_to_have_landed_becomes_paid(session_maker) -> None:
    reader = FakeReader()
    await _created(session_maker, reader)
    await _claim_pay(session_maker, ledger_module.STATUS_PAY_REJECTED)
    reader.order = paid_order(marker=MARKER)

    report = await run_reconcile(session_maker, reader, identity=IDENTITY)
    snapshot = await ledger_module.load(session_maker)

    assert snapshot.status == ledger_module.STATUS_PAID
    assert report.outcome == OUTCOME_PROVEN
    assert report.remote_rollback_proven is False


@pytest.mark.asyncio
async def test_a_ledger_that_contradicts_the_order_is_never_reported_as_proven(session_maker) -> None:
    """A refunded ledger over an order that still reads paid is not resolved.

    One of the two is wrong, and the expensive possibility — the money is still
    out — is the one that has to win.
    """
    reader = FakeReader()
    mutator = await _paid(session_maker, reader)
    await _refund(session_maker, reader, mutator)
    # The order now reads paid again: a reversal that did not stick, or a
    # dashboard edit. The ledger still says refunded.
    reader.order = paid_order(marker=MARKER)

    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome != OUTCOME_PROVEN
    assert report.outcome == OUTCOME_CONTRACT_MISMATCH
    assert report.remote_rollback_proven is False
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_REFUNDED


@pytest.mark.asyncio
async def test_an_unrecognisable_order_state_is_never_reported_as_proven(session_maker) -> None:
    reader = FakeReader()
    await _created(session_maker, reader)
    unfamiliar = open_order(marker=MARKER)
    unfamiliar["status"] = "awaiting_settlement"
    reader.order = unfamiliar

    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.order_state == ORDER_UNKNOWN
    assert report.outcome == OUTCOME_UNKNOWN_MUTATION
    assert report.remote_rollback_proven is False
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_CREATED


@pytest.mark.asyncio
async def test_an_ambiguous_canary_stays_ambiguous_whatever_an_order_reads(session_maker) -> None:
    """Which order is ours was never established. Reading one settles nothing."""
    reader = FakeReader()
    await _created(session_maker, reader)
    await ledger_module.record_outcome(
        session_maker,
        status=ledger_module.STATUS_AMBIGUOUS,
        expected_statuses=frozenset({ledger_module.STATUS_CREATED}),
    )
    reader.order = refunded_order(marker=MARKER)

    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_AMBIGUOUS
    assert report.manual_cleanup_required is True
    assert report.remote_rollback_proven is False
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_AMBIGUOUS


@pytest.mark.asyncio
async def test_a_reconcile_during_an_in_flight_create_never_makes_create_claimable(session_maker) -> None:
    """The create POST is out and nothing has been seen. Claiming again is not
    an option, and neither is deciding the order does not exist."""
    reader = FakeReader()
    config = "c" * 64
    await ledger_module.claim_create(
        session_maker,
        create_plan_digest="a" * 64,
        template_config_digest=config,
        customer_fingerprint=IDENTITY.fingerprints["customer"],
        staffer_fingerprint=IDENTITY.fingerprints["staffer"],
        account_fingerprint=IDENTITY.fingerprints["account"],
        reconciliation_marker=MARKER,
        create_window_start=utcnow() - timedelta(minutes=10),
        create_window_end=utcnow() + timedelta(hours=6),
    )

    report = await run_reconcile(session_maker, reader, identity=IDENTITY)
    snapshot = await ledger_module.load(session_maker)

    assert report.reconciliation_required is True
    assert snapshot.status in {ledger_module.STATUS_CREATE_CLAIMED, ledger_module.STATUS_CREATE_UNKNOWN}
    again = await ledger_module.claim_create(
        session_maker,
        create_plan_digest="a" * 64,
        template_config_digest=config,
        customer_fingerprint=IDENTITY.fingerprints["customer"],
        staffer_fingerprint=IDENTITY.fingerprints["staffer"],
        account_fingerprint=IDENTITY.fingerprints["account"],
        reconciliation_marker=MARKER,
        create_window_start=utcnow() - timedelta(minutes=10),
        create_window_end=utcnow() + timedelta(hours=6),
    )
    assert again.granted is False


# ---------------------------------------------------------------------------
# Order classification
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payload,expected_state,expected_proof",
    [
        (open_order(marker=MARKER), ORDER_OPEN, PAYMENT_PROOF_NONE),
        (paid_order(marker=MARKER), ORDER_PAID, PAYMENT_PROOF_STATUS),
        (refunded_order(marker=MARKER), ORDER_REFUNDED, PAYMENT_PROOF_NONE),
        (cancelled_order(marker=MARKER), ORDER_CANCELLED, PAYMENT_PROOF_NONE),
        ({"not": "an order"}, ORDER_MALFORMED, PAYMENT_PROOF_NONE),
        (None, ORDER_MALFORMED, PAYMENT_PROOF_NONE),
    ],
)
def test_order_states_are_read_from_documented_fields(payload, expected_state, expected_proof) -> None:
    assert classify_order(payload) == (expected_state, expected_proof)


def test_a_settled_invoice_is_the_second_documented_payment_proof() -> None:
    order = open_order(marker=MARKER, invoice={"amount_due": 0, "amount_paid": 1500})
    assert classify_order(order) == (ORDER_PAID, PAYMENT_PROOF_AMOUNTS)


def test_the_opaque_bookkeeping_figure_never_proves_a_payment() -> None:
    order = open_order(marker=MARKER, invoice={"amount_due": 1500, "amount_paid": 0, "account_paid_amount": -1500})
    assert classify_order(order) == (ORDER_OPEN, PAYMENT_PROOF_NONE)


def test_a_reverted_order_is_refunded_even_when_it_still_says_paid() -> None:
    assert classify_order(paid_order(marker=MARKER, is_reverted=True))[0] == ORDER_REFUNDED


@pytest.mark.parametrize(
    "status",
    [
        "draft",
        "pending",
        "processing",
        "partially_paid",
        "on_hold",
        "expired",
        "",
        "   ",
        None,
        123,
        True,
        ["open"],
        {"value": "open"},
    ],
)
def test_a_status_we_do_not_recognise_is_never_read_as_open(status) -> None:
    """An unrecognised state is the one thing that must not become "payable".

    `open` is the single state a payment may be sent from, so any status that
    falls through to it is a state we never reasoned about authorising a real
    payment from — a half-settled order, a draft in some workflow nobody
    documented, a field the API renamed.
    """
    order = open_order(marker=MARKER)
    order["status"] = status

    assert classify_order(order) == (ORDER_UNKNOWN, PAYMENT_PROOF_NONE)


def test_an_order_with_no_status_field_at_all_is_unknown() -> None:
    order = open_order(marker=MARKER)
    del order["status"]

    assert classify_order(order) == (ORDER_UNKNOWN, PAYMENT_PROOF_NONE)


def test_an_empty_uuid_is_as_malformed_as_a_missing_one() -> None:
    assert classify_order({"uuid": "", "status": "open"}) == (ORDER_MALFORMED, PAYMENT_PROOF_NONE)


@pytest.mark.parametrize(
    "changes",
    [
        {"is_canceled": True, "is_paid": True},
        {"is_cancelled": True, "status": "open"},
        {"is_canceled": True, "invoice": {"amount_due": 0, "amount_paid": 1500}},
        {"is_reverted": True, "is_canceled": True},
    ],
)
def test_an_order_whose_own_signals_disagree_is_unknown(changes) -> None:
    """Two contradicting facts are not a state, and not something to act on."""
    order = open_order(marker=MARKER)
    order.update(changes)

    assert classify_order(order)[0] == ORDER_UNKNOWN


def test_payment_evidence_outranks_an_open_label() -> None:
    """The safe reading of "open but paid" is PAID, not OPEN.

    Calling it open would offer it to the pay stage; calling it unknown would
    put the refund out of reach. Both of those lose real money; this does not.
    """
    flagged = open_order(marker=MARKER, is_paid=True)
    assert classify_order(flagged) == (ORDER_PAID, PAYMENT_PROOF_STATUS)

    settled = open_order(marker=MARKER, invoice={"amount_due": 0, "amount_paid": 1500})
    assert classify_order(settled) == (ORDER_PAID, PAYMENT_PROOF_AMOUNTS)


@pytest.mark.parametrize("state", [ORDER_UNKNOWN, ORDER_MALFORMED])
def test_an_unreadable_state_never_moves_the_ledger(state) -> None:
    for status in ledger_module.ALL_STATUSES:
        assert reconcile_transition(status, state).status is None


# ---------------------------------------------------------------------------
# Nothing leaks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_report_or_ledger_row_carries_an_artifact_or_a_person(session_maker) -> None:
    reader = FakeReader()
    mutator = await _created(session_maker, reader, pay_effect=False)
    leaky = paid_order(
        marker=MARKER,
        customer={
            "uuid": CUSTOMER_UUID,
            "first_name": "Synthetic",
            "last_name": "Fixture",
            "phone": PHONE_SENTINEL,
            "email": "person@example.invalid",
        },
        vouchers=[
            {
                "uuid": OTHER_UUID,
                "code": ARTIFACT_SENTINEL,
                "public_url": "https://example.invalid/" + ARTIFACT_SENTINEL,
            }
        ],
    )

    async def settle_leakily(**kwargs: Any) -> VoucherMutationResponse:
        mutator.calls.append("pay")
        reader.order = leaky
        return VoucherMutationResponse(http_status=200, envelope=leaky)

    mutator.pay_voucher_order = settle_leakily  # type: ignore[method-assign]
    report = await _pay(session_maker, reader, mutator)

    printed = repr(report.as_safe_dict())
    snapshot = repr((await ledger_module.load(session_maker)).as_safe_dict())
    for surface in (printed, snapshot):
        for forbidden in (
            ARTIFACT_SENTINEL,
            "example.invalid",
            PHONE_SENTINEL,
            "Synthetic",
            CUSTOMER_UUID,
            STAFFER_UUID,
            ACCOUNT_UUID,
            ORDER_UUID,
        ):
            assert forbidden not in surface, forbidden
    # The customer subtree is recorded as redacted, not walked into.
    assert "subtree_redacted" in printed
