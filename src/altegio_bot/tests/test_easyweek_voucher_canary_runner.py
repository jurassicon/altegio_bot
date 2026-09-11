"""Stage flows and unknown-result reconciliation for the voucher canary (§35).

The reader and the mutator are recording fakes, and the ledger is the real table
on PostgreSQL — because "did this stage send a second request?" is only a real
question when the durable state is real.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from altegio_bot.easyweek_client import EasyWeekAuthError, EasyWeekNotFoundError, EasyWeekRetryableError
from altegio_bot.easyweek_voucher_canary import ledger as ledger_module
from altegio_bot.easyweek_voucher_canary.plan import (
    CANARY_DISABLED_BY_ENV,
    CANARY_PLAN_DIGEST_MISMATCH,
    CANARY_PLAN_EXPIRED,
    STAGE_CREATE,
    STAGE_PAY,
    STAGE_REFUND,
    RuntimeIdentity,
    build_plan,
    canary_marker,
)
from altegio_bot.easyweek_voucher_canary.runner import (
    ORDER_CANCELLED,
    ORDER_MALFORMED,
    ORDER_OPEN,
    ORDER_PAID,
    ORDER_REFUNDED,
    OUTCOME_AMBIGUOUS,
    OUTCOME_CONTRACT_MISMATCH,
    OUTCOME_MANUAL_CLEANUP,
    OUTCOME_PROVEN,
    OUTCOME_REFUSED,
    OUTCOME_ROLLBACK_UNPROVEN,
    OUTCOME_UNKNOWN_MUTATION,
    PAYMENT_PROOF_AMOUNTS,
    PAYMENT_PROOF_NONE,
    PAYMENT_PROOF_STATUS,
    REASON_MUTATION_REJECTED,
    REASON_MUTATION_UNKNOWN,
    REASON_ORDER_SHAPE_UNPROVEN,
    REASON_RECONCILE_AMBIGUOUS,
    REASON_RECONCILE_UNRESOLVED,
    REASON_ROLLBACK_UNPROVEN,
    classify_order,
    find_marker_order,
    run_create,
    run_pay,
    run_reconcile,
    run_refund,
    run_status,
)
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    KARLSRUHE_LOCATION_UUID,
)
from altegio_bot.easyweek_voucher_mutation import (
    EasyWeekVoucherMutationUnknown,
    VoucherMutationResponse,
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
    cancelled_order,
    open_order,
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


class FakeReader:
    """Plan GETs plus the POS reads the reconciler needs."""

    def __init__(
        self,
        *,
        template: Any = None,
        order: Any = None,
        order_pages: list[list[dict[str, Any]]] | None = None,
        existing_orders: list[list[dict[str, Any]]] | None = None,
        order_error: Exception | None = None,
    ) -> None:
        self.template = TEMPLATE if template is None else template
        self.order = order
        # Pages served to the reconciliation walk (after a claim exists).
        self.order_pages = order_pages
        # Pages served to the plan's "is there already a marker order?" walk.
        self.existing_orders = existing_orders if existing_orders is not None else [[]]
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
        return self.template

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        return CUSTOMER

    async def list_location_staffers(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        return STAFFERS if page == 1 else {"data": []}

    async def list_location_accounts(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        return ACCOUNTS if page == 1 else {"data": []}

    # -- POS surface -------------------------------------------------------

    async def list_location_orders(
        self, *, location_uuid: str, customer_uuid: str, page: int, per_page: int = 100
    ) -> dict[str, Any]:
        assert location_uuid == KARLSRUHE_LOCATION_UUID
        assert customer_uuid == CUSTOMER_UUID
        self.calls.append(f"list_orders:{page}")
        pages = self.order_pages if (self._claimed and self.order_pages is not None) else self.existing_orders
        index = page - 1
        return {"data": pages[index] if index < len(pages) else []}

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
    """Records every mutation; there must never be a second one per stage."""

    def __init__(
        self,
        *,
        create_response: Any = None,
        pay_response: Any = None,
        refund_response: Any = None,
        create_error: Exception | None = None,
        pay_error: Exception | None = None,
        refund_error: Exception | None = None,
    ) -> None:
        self.create_response = create_response
        self.pay_response = pay_response
        self.refund_response = refund_response
        self.create_error = create_error
        self.pay_error = pay_error
        self.refund_error = refund_error
        self.calls: list[str] = []

    async def create_voucher_order(self, **kwargs: Any) -> VoucherMutationResponse:
        self.calls.append("create")
        assert kwargs["location_uuid"] == KARLSRUHE_LOCATION_UUID
        assert kwargs["voucher_template_uuid"] == EASYWEEK_VOUCHER_TEMPLATE_UUID
        assert kwargs["price_minor"] == 1500
        assert kwargs["marker"] == MARKER
        if self.create_error is not None:
            raise self.create_error
        body = self.create_response if self.create_response is not None else open_order(marker=MARKER)
        return VoucherMutationResponse(http_status=200, envelope=body)

    async def pay_voucher_order(self, **kwargs: Any) -> VoucherMutationResponse:
        self.calls.append("pay")
        assert kwargs["account_uuid"] == ACCOUNT_UUID
        assert kwargs["amount_minor"] == 1500
        if self.pay_error is not None:
            raise self.pay_error
        body = self.pay_response if self.pay_response is not None else paid_order(marker=MARKER)
        return VoucherMutationResponse(http_status=200, envelope=body)

    async def refund_voucher_order(self, **kwargs: Any) -> VoucherMutationResponse:
        self.calls.append("refund")
        if self.refund_error is not None:
            raise self.refund_error
        body = self.refund_response if self.refund_response is not None else refunded_order(marker=MARKER)
        return VoucherMutationResponse(http_status=200, envelope=body)


class RefusingMutator:
    async def create_voucher_order(self, **kwargs: Any):  # pragma: no cover
        raise AssertionError("no mutation may be sent")

    async def pay_voucher_order(self, **kwargs: Any):  # pragma: no cover
        raise AssertionError("no mutation may be sent")

    async def refund_voucher_order(self, **kwargs: Any):  # pragma: no cover
        raise AssertionError("no mutation may be sent")


async def _authorised(reader: FakeReader, stage: str) -> dict[str, Any]:
    plan = await build_plan(reader, identity=IDENTITY, enabled=True)
    return {
        "plan_digest": plan.digest,
        "plan_issued_at": plan.issued_at,
        "confirmation_phrase": plan.confirmation_phrase(stage),
    }


async def _create(session_maker, reader: FakeReader, mutator: Any, **overrides: Any):
    kwargs = await _authorised(reader, STAGE_CREATE)
    kwargs.update(overrides)
    return await run_create(
        session_maker, reader, mutator, identity=IDENTITY, enabled=overrides.pop("enabled", True), **kwargs
    )


async def _pay(session_maker, reader: FakeReader, mutator: Any, **overrides: Any):
    kwargs = await _authorised(reader, STAGE_PAY)
    kwargs.update(overrides)
    return await run_pay(session_maker, reader, mutator, identity=IDENTITY, enabled=True, **kwargs)


async def _refund(session_maker, reader: FakeReader, mutator: Any, **overrides: Any):
    kwargs = await _authorised(reader, STAGE_REFUND)
    kwargs.update(overrides)
    return await run_refund(session_maker, reader, mutator, identity=IDENTITY, enabled=True, **kwargs)


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_proven_create_claims_first_then_sends_exactly_one_request(session_maker) -> None:
    reader = FakeReader(order=open_order(marker=MARKER))
    mutator = FakeMutator()

    report = await _create(session_maker, reader, mutator)

    assert report.outcome == OUTCOME_PROVEN
    assert report.external_mutation_attempted is True
    assert mutator.calls == ["create"]
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == ledger_module.STATUS_CREATED
    assert snapshot.target_order_uuid == ORDER_UUID
    # An open draft exists, so somebody has to close it one way or another.
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
    assert safe["mutation_stage"] == STAGE_CREATE


@pytest.mark.asyncio
async def test_a_disabled_env_fence_blocks_before_any_claim(session_maker) -> None:
    reader = FakeReader()
    plan = await build_plan(reader, identity=IDENTITY, enabled=False)

    report = await run_create(
        session_maker,
        reader,
        RefusingMutator(),
        identity=IDENTITY,
        enabled=False,
        plan_digest=plan.digest,
        plan_issued_at=plan.issued_at,
        confirmation_phrase=plan.confirmation_phrase(STAGE_CREATE),
    )

    assert report.outcome == OUTCOME_REFUSED
    assert CANARY_DISABLED_BY_ENV in report.reasons
    assert (await ledger_module.load(session_maker)).exists is False


@pytest.mark.asyncio
async def test_a_digest_mismatch_blocks_before_any_claim(session_maker) -> None:
    reader = FakeReader()

    report = await _create(session_maker, reader, RefusingMutator(), plan_digest="0" * 64)

    assert report.outcome == OUTCOME_REFUSED
    assert CANARY_PLAN_DIGEST_MISMATCH in report.reasons
    assert (await ledger_module.load(session_maker)).exists is False


@pytest.mark.asyncio
async def test_an_expired_approval_blocks_before_any_claim(session_maker) -> None:
    reader = FakeReader()

    report = await _create(session_maker, reader, RefusingMutator(), plan_issued_at=utcnow() - timedelta(days=1))

    assert report.outcome == OUTCOME_REFUSED
    assert CANARY_PLAN_EXPIRED in report.reasons
    assert (await ledger_module.load(session_maker)).exists is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift",
    [
        {"cost": 1600},
        {"is_enabled": False},
        {"is_online": True},
        {"vouchers_count": None},
        {"branches_count": 2},
    ],
)
async def test_template_drift_blocks_before_any_claim(session_maker, drift) -> None:
    """The plan is recomputed live, so drift changes the digest and stops it."""
    clean = FakeReader()
    authorised = await _authorised(clean, STAGE_CREATE)

    drifted = FakeReader(template={**TEMPLATE, **drift})
    report = await run_create(
        session_maker,
        drifted,
        RefusingMutator(),
        identity=IDENTITY,
        enabled=True,
        **authorised,
    )

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

    # A second attempt sends nothing at all.
    again = await _create(session_maker, reader, mutator)
    assert again.outcome == OUTCOME_REFUSED
    assert mutator.calls == ["create"]


@pytest.mark.asyncio
async def test_a_rejected_create_is_a_contract_mismatch_not_an_unknown(session_maker) -> None:
    reader = FakeReader()
    mutator = FakeMutator(create_error=EasyWeekAuthError("auth", status_code=403))

    report = await _create(session_maker, reader, mutator)

    assert report.outcome == OUTCOME_CONTRACT_MISMATCH
    assert REASON_MUTATION_REJECTED in report.reasons
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_CREATE_REJECTED


@pytest.mark.asyncio
async def test_a_2xx_without_an_order_identity_is_unknown(session_maker) -> None:
    reader = FakeReader()
    mutator = FakeMutator(create_response={"ok": True})

    report = await _create(session_maker, reader, mutator)

    assert report.outcome == OUTCOME_UNKNOWN_MUTATION
    assert REASON_ORDER_SHAPE_UNPROVEN in report.reasons
    assert report.reconciliation_required is True
    assert (await ledger_module.load(session_maker)).target_order_uuid is None


# ---------------------------------------------------------------------------
# Create reconciliation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_one_marker_match_resolves_an_unknown_create(session_maker) -> None:
    reader = FakeReader(order=open_order(marker=MARKER))
    mutator = FakeMutator(create_error=EasyWeekVoucherMutationUnknown("lost", attempts=1))
    await _create(session_maker, reader, mutator)

    reader.order_pages = [[open_order(marker=MARKER)], []]
    reader.reconciliation_mode()
    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_PROVEN
    assert (await ledger_module.load(session_maker)).target_order_uuid == ORDER_UUID
    assert mutator.calls == ["create"]


@pytest.mark.asyncio
async def test_zero_matches_stay_unresolved_and_never_mean_not_created(session_maker) -> None:
    reader = FakeReader()
    await _create(session_maker, reader, FakeMutator(create_error=EasyWeekVoucherMutationUnknown("lost", attempts=1)))

    reader.order_pages = [[]]
    reader.reconciliation_mode()
    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_UNKNOWN_MUTATION
    assert REASON_RECONCILE_UNRESOLVED in report.reasons
    assert report.reconciliation_required is True
    assert report.manual_cleanup_required is True
    # Still unresolved in the ledger, so no stage can move on.
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_CREATE_UNKNOWN


@pytest.mark.asyncio
async def test_two_marker_matches_are_ambiguous_and_stop_everything(session_maker) -> None:
    reader = FakeReader()
    await _create(session_maker, reader, FakeMutator(create_error=EasyWeekVoucherMutationUnknown("lost", attempts=1)))

    reader.order_pages = [
        [open_order(marker=MARKER), open_order(marker=MARKER, uuid=OTHER_UUID)],
        [],
    ]
    reader.reconciliation_mode()
    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_AMBIGUOUS
    assert REASON_RECONCILE_AMBIGUOUS in report.reasons
    assert report.manual_cleanup_required is True
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_AMBIGUOUS


@pytest.mark.asyncio
async def test_the_walk_covers_every_page(session_maker) -> None:
    reader = FakeReader()
    await _create(session_maker, reader, FakeMutator(create_error=EasyWeekVoucherMutationUnknown("lost", attempts=1)))

    reader.order_pages = [
        [open_order(marker="unrelated-one")],
        [open_order(marker="unrelated-two")],
        [open_order(marker=MARKER)],
        [],
    ]
    reader.reconciliation_mode()
    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_PROVEN
    assert "list_orders:3" in reader.calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "wrong",
    [
        {"comment": "someone-elses-marker"},
        {"customer_uuid": OTHER_UUID},
        {"staffer_uuid": OTHER_UUID},
        {"location_uuid": OTHER_UUID},
        {"vouchers": []},
        {"vouchers": [{"voucher_template_uuid": OTHER_UUID, "price": 1500, "quantity": 1}]},
        {"vouchers": [{"voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID, "price": 1499, "quantity": 1}]},
        {"vouchers": [{"voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID, "price": 1500, "quantity": 2}]},
        {"created_at": "2020-01-01T00:00:00+00:00"},
        {"created_at": "not-a-timestamp"},
        {"created_at": None},
    ],
)
async def test_a_candidate_that_is_not_exactly_ours_never_matches(session_maker, wrong) -> None:
    """Time and customer alone are never enough; every expected fact must hold."""
    reader = FakeReader()
    await _create(session_maker, reader, FakeMutator(create_error=EasyWeekVoucherMutationUnknown("lost", attempts=1)))

    reader.order_pages = [[open_order(marker=MARKER, **wrong)], []]
    reader.reconciliation_mode()
    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_UNKNOWN_MUTATION
    assert REASON_RECONCILE_UNRESOLVED in report.reasons


@pytest.mark.asyncio
async def test_an_endless_listing_is_unresolved_not_absent(session_maker) -> None:
    class EndlessReader(FakeReader):
        async def list_location_orders(self, **kwargs: Any) -> dict[str, Any]:
            if not self._claimed:
                return {"data": []}
            return {"data": [open_order(marker="unrelated")]}

    reader = EndlessReader()
    await _create(session_maker, reader, FakeMutator(create_error=EasyWeekVoucherMutationUnknown("lost", attempts=1)))
    reader.reconciliation_mode()

    report = await run_reconcile(session_maker, reader, identity=IDENTITY)
    assert report.outcome == OUTCOME_UNKNOWN_MUTATION


@pytest.mark.asyncio
async def test_find_marker_order_reports_an_incomplete_walk(session_maker) -> None:
    class EndlessReader(FakeReader):
        async def list_location_orders(self, **kwargs: Any) -> dict[str, Any]:
            return {"data": [open_order(marker="unrelated")]}

    now = utcnow()
    match = await find_marker_order(
        EndlessReader(),
        identity=IDENTITY,
        marker=MARKER,
        window_start=now - timedelta(days=1),
        window_end=now + timedelta(days=1),
    )
    assert match.complete is False
    assert match.count == 0


# ---------------------------------------------------------------------------
# Pay
# ---------------------------------------------------------------------------


async def _created(session_maker, reader: FakeReader) -> FakeMutator:
    mutator = FakeMutator()
    reader.order = open_order(marker=MARKER)
    await _create(session_maker, reader, mutator)
    return mutator


@pytest.mark.asyncio
async def test_a_proven_pay_sends_exactly_one_request(session_maker) -> None:
    reader = FakeReader()
    mutator = await _created(session_maker, reader)

    reader.order = paid_order(marker=MARKER)
    report = await _pay(session_maker, reader, mutator)

    assert report.outcome == OUTCOME_PROVEN
    assert report.order_state == ORDER_PAID
    assert report.payment_proof == PAYMENT_PROOF_STATUS
    assert mutator.calls == ["create", "pay"]
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
async def test_an_unknown_pay_is_resolved_by_reading(session_maker) -> None:
    reader = FakeReader()
    mutator = await _created(session_maker, reader)
    mutator.pay_error = EasyWeekVoucherMutationUnknown("lost", attempts=1)
    await _pay(session_maker, reader, mutator)

    reader.order = paid_order(marker=MARKER)
    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_PROVEN
    assert report.order_state == ORDER_PAID
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_PAID
    assert mutator.calls == ["create", "pay"]


@pytest.mark.asyncio
async def test_a_2xx_pay_whose_readback_is_not_paid_is_unknown(session_maker) -> None:
    reader = FakeReader()
    mutator = await _created(session_maker, reader)
    reader.order = open_order(marker=MARKER)

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
# Refund
# ---------------------------------------------------------------------------


async def _paid(session_maker, reader: FakeReader) -> FakeMutator:
    mutator = await _created(session_maker, reader)
    reader.order = paid_order(marker=MARKER)
    await _pay(session_maker, reader, mutator)
    return mutator


@pytest.mark.asyncio
async def test_a_proven_refund_sends_exactly_one_request(session_maker) -> None:
    reader = FakeReader()
    mutator = await _paid(session_maker, reader)

    reader.order = refunded_order(marker=MARKER)
    report = await _refund(session_maker, reader, mutator)

    assert report.outcome == OUTCOME_PROVEN
    assert report.remote_rollback_proven is True
    assert report.order_state == ORDER_REFUNDED
    assert mutator.calls == ["create", "pay", "refund"]
    assert (await ledger_module.load(session_maker)).status == ledger_module.STATUS_REFUNDED


@pytest.mark.asyncio
async def test_a_refund_stays_available_after_an_unreadable_artifact(session_maker) -> None:
    """Cleanup beats research: a broken voucher must not strand a real payment."""
    reader = FakeReader()
    mutator = await _created(session_maker, reader)
    # The paid readback carries an artifact nobody can parse.
    reader.order = paid_order(marker=MARKER, vouchers=[{"???": {"nested": [1, 2, 3]}}])
    pay_report = await _pay(session_maker, reader, mutator)
    assert pay_report.outcome == OUTCOME_PROVEN

    reader.order = refunded_order(marker=MARKER)
    refund_report = await _refund(session_maker, reader, mutator)

    assert refund_report.outcome == OUTCOME_PROVEN
    assert refund_report.remote_rollback_proven is True


@pytest.mark.asyncio
async def test_an_unknown_refund_is_never_repeated_and_is_resolved_by_reading(session_maker) -> None:
    reader = FakeReader()
    mutator = await _paid(session_maker, reader)
    mutator.refund_error = EasyWeekVoucherMutationUnknown("lost", attempts=1)

    report = await _refund(session_maker, reader, mutator)
    assert report.outcome == OUTCOME_UNKNOWN_MUTATION
    assert mutator.calls == ["create", "pay", "refund"]

    again = await _refund(session_maker, reader, mutator)
    assert again.outcome == OUTCOME_REFUSED
    assert mutator.calls == ["create", "pay", "refund"]

    reader.order = refunded_order(marker=MARKER)
    resolved = await run_reconcile(session_maker, reader, identity=IDENTITY)
    assert resolved.outcome == OUTCOME_PROVEN
    assert resolved.remote_rollback_proven is True
    assert mutator.calls == ["create", "pay", "refund"]


@pytest.mark.asyncio
async def test_a_refund_whose_readback_is_still_paid_is_rollback_unproven(session_maker) -> None:
    reader = FakeReader()
    mutator = await _paid(session_maker, reader)

    report = await _refund(session_maker, reader, mutator)

    assert report.outcome == OUTCOME_ROLLBACK_UNPROVEN
    assert REASON_ROLLBACK_UNPROVEN in report.reasons
    assert report.remote_rollback_proven is False


@pytest.mark.asyncio
async def test_an_unresolved_refund_that_reads_as_paid_stays_rollback_unproven(session_maker) -> None:
    reader = FakeReader()
    mutator = await _paid(session_maker, reader)
    mutator.refund_error = EasyWeekVoucherMutationUnknown("lost", attempts=1)
    await _refund(session_maker, reader, mutator)

    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_ROLLBACK_UNPROVEN
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
    # The marker is what an operator pastes into the dashboard search.
    assert report.ledger["reconciliation_marker"] == MARKER


@pytest.mark.asyncio
async def test_a_manual_cancellation_is_observed_never_attributed_to_the_tool(session_maker) -> None:
    reader = FakeReader()
    await _created(session_maker, reader)

    reader.order = cancelled_order(marker=MARKER)
    report = await run_reconcile(session_maker, reader, identity=IDENTITY)

    assert report.outcome == OUTCOME_PROVEN
    assert report.order_state == ORDER_CANCELLED
    # The tool did not roll anything back, and does not say it did.
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
        ("text", ORDER_MALFORMED, PAYMENT_PROOF_NONE),
    ],
)
def test_order_states_are_read_from_documented_fields(payload, expected_state, expected_proof) -> None:
    assert classify_order(payload) == (expected_state, expected_proof)


def test_a_settled_invoice_is_the_second_documented_payment_proof() -> None:
    order = open_order(marker=MARKER, invoice={"amount_due": 0, "amount_paid": 1500})
    assert classify_order(order) == (ORDER_PAID, PAYMENT_PROOF_AMOUNTS)


def test_the_opaque_bookkeeping_figure_never_proves_a_payment() -> None:
    """`account_paid_amount` is not a payment and is never consulted."""
    order = open_order(marker=MARKER, invoice={"amount_due": 1500, "amount_paid": 0, "account_paid_amount": -1500})
    assert classify_order(order) == (ORDER_OPEN, PAYMENT_PROOF_NONE)


def test_a_reverted_order_is_refunded_even_when_it_still_says_paid() -> None:
    order = paid_order(marker=MARKER, is_reverted=True)
    assert classify_order(order)[0] == ORDER_REFUNDED


# ---------------------------------------------------------------------------
# Nothing leaks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_report_or_ledger_row_carries_an_artifact_or_an_identity(session_maker) -> None:
    reader = FakeReader()
    mutator = await _created(session_maker, reader)
    reader.order = paid_order(
        marker=MARKER,
        vouchers=[
            {
                "uuid": OTHER_UUID,
                "code": ARTIFACT_SENTINEL,
                "public_url": "https://example.invalid/" + ARTIFACT_SENTINEL,
                "customer": {"name": ARTIFACT_SENTINEL, "phone": "+490000000000"},
            }
        ],
    )
    report = await _pay(session_maker, reader, mutator)

    printed = repr(report.as_safe_dict())
    snapshot = repr((await ledger_module.load(session_maker)).as_safe_dict())
    for surface in (printed, snapshot):
        for forbidden in (
            ARTIFACT_SENTINEL,
            "example.invalid",
            "+490000000000",
            CUSTOMER_UUID,
            STAFFER_UUID,
            ACCOUNT_UUID,
            ORDER_UUID,
        ):
            assert forbidden not in surface, forbidden
    # The shape survived, so the research question is still answerable.
    assert any("code" in repr(observation) for observation in report.observations)
