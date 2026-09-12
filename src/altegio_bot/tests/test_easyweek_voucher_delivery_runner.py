"""Stage flows of the controlled voucher delivery canary (§36).

The reader, the mutator and the sender are recording fakes; the ledger is the
real table on PostgreSQL, because "did this stage send a second request?" is
only a real question when the durable state is real.

The sentinel voucher code runs through every one of these flows. Whatever else a
test asserts, the code must appear in exactly one place — the parameters handed
to the Meta transport — and nowhere else at all.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

import pytest

from altegio_bot.campaigns.easyweek_voucher_delivery import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_delivery import runner as runner_module
from altegio_bot.campaigns.easyweek_voucher_delivery.identity import (
    ACTIVE_FUTURE_BOOKING_PRESENT,
    CANARY_DISABLED,
    DELIVERY_ALREADY_ATTEMPTED,
    DELIVERY_OUTCOME_UNKNOWN,
    HMAC_KEY_MISSING,
    MANUAL_CLEANUP_REQUIRED,
    MUTATION_UNKNOWN,
    RECIPIENT_IDENTITY_UNPROVEN,
    REFUND_FORBIDDEN_AFTER_SEND,
    SENDER_UNPROVEN,
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_PAY,
    STAGE_REFUND,
    TEMPLATE_UNPROVEN,
    VOUCHER_ORDER_ALREADY_REFUNDED,
    VOUCHER_ORDER_UNPROVEN,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.runner import (
    OUTCOME_CONTRACT_MISMATCH,
    OUTCOME_PROVEN,
    OUTCOME_REFUSED,
    OUTCOME_UNKNOWN,
)
from altegio_bot.easyweek_client import EasyWeekPermanentError
from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationUnknown
from altegio_bot.models.models import (
    VOUCHER_DELIVERY_CREATE_UNKNOWN,
    VOUCHER_DELIVERY_CREATED,
    VOUCHER_DELIVERY_MANUALLY_CLEANED,
    VOUCHER_DELIVERY_PAID,
    VOUCHER_DELIVERY_PAY_UNKNOWN,
    VOUCHER_DELIVERY_PROVIDER_ACCEPTED,
    VOUCHER_DELIVERY_REFUND_UNKNOWN,
    VOUCHER_DELIVERY_REFUNDED,
    VOUCHER_DELIVERY_SEND_REJECTED,
    VOUCHER_DELIVERY_SEND_UNKNOWN,
)
from altegio_bot.scripts.easyweek_voucher_delivery_canary import (
    _OUTCOME_EXIT_CODES,
    EXIT_MANUAL_CLEANUP,
    EXIT_OK,
)
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_voucher_delivery_fixtures import (  # noqa: F401 - fixtures
    NOW,
    OTHER_UUID,
    PROVIDER_MESSAGE_ID,
    UNKNOWN_OUTCOME,
    VOUCHER_CODE_SENTINEL,
    FakeEasyWeekReader,
    FakeMutator,
    FakeSender,
    RefusingMutator,
    RefusingSender,
    booking_payload,
    canary_request,
    create_window,
    history_page,
    marker_order,
    orders_page,
    seed_recipient,
    seed_template_and_sender,
    voucher_order,
)

BOOKING_LINK = "https://karlsruhe.example.invalid/"


@pytest.fixture
def enabled(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> None:
    """Everything configured and the fence OPEN."""
    request.getfixturevalue("configuration")
    request.getfixturevalue("binding_key")
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_canary_enabled", True, raising=False)


async def _ready(session_maker):
    """A seeded, fully provable recipient and the request that names them."""
    run_id, recipient_id = await seed_recipient(session_maker)
    await seed_template_and_sender(session_maker)
    request = canary_request(run_id=run_id, recipient_id=recipient_id)
    reader = FakeEasyWeekReader(marker=request.marker)
    return request, reader


async def _plan(session_maker, request, reader, stage, **kwargs):
    async with session_maker() as session:
        return await runner_module.build_stage_plan(
            session,
            session_maker,
            stage=stage,
            request=request,
            reader=reader,
            order_reader=reader,
            **kwargs,
        )


async def _create(session_maker, request, reader, mutator, **overrides):
    plan, _, _ = await _plan(session_maker, request, reader, STAGE_CREATE)
    kwargs = {
        "plan_digest": plan.digest,
        "plan_issued_at": plan.issued_at,
        "confirmation_phrase": plan.confirmation_phrase,
        "apply": True,
    }
    kwargs.update(overrides)
    async with session_maker() as session:
        return await runner_module.run_create(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            **kwargs,
        )


async def _pay(session_maker, request, reader, mutator, **overrides):
    plan, _, _ = await _plan(session_maker, request, reader, STAGE_PAY)
    kwargs = {
        "plan_digest": plan.digest,
        "plan_issued_at": plan.issued_at,
        "confirmation_phrase": plan.confirmation_phrase,
        "apply": True,
    }
    kwargs.update(overrides)
    async with session_maker() as session:
        return await runner_module.run_pay(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            **kwargs,
        )


async def _deliver(session_maker, request, reader, sender, **overrides):
    plan, _, _ = await _plan(session_maker, request, reader, STAGE_DELIVER)
    kwargs = {
        "plan_digest": plan.digest,
        "plan_issued_at": plan.issued_at,
        "confirmation_phrase": plan.confirmation_phrase,
        "apply": True,
    }
    kwargs.update(overrides)
    async with session_maker() as session:
        return await runner_module.run_deliver(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            sender=sender,
            booking_link=BOOKING_LINK,
            **kwargs,
        )


async def _refund(session_maker, request, reader, mutator, **overrides):
    plan, _, _ = await _plan(session_maker, request, reader, STAGE_REFUND)
    kwargs = {
        "plan_digest": plan.digest,
        "plan_issued_at": plan.issued_at,
        "confirmation_phrase": plan.confirmation_phrase,
        "apply": True,
    }
    kwargs.update(overrides)
    async with session_maker() as session:
        return await runner_module.run_refund(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            **kwargs,
        )


async def _paid(session_maker):
    """A canary that has created and paid for its voucher."""
    request, reader = await _ready(session_maker)
    mutator = FakeMutator(reader, marker=request.marker)
    await _create(session_maker, request, reader, mutator)
    reader.order = voucher_order(marker=request.marker)
    await _pay(session_maker, request, reader, mutator)
    return request, reader, mutator


# ---------------------------------------------------------------------------
# Prerequisites are proven before the voucher exists
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_closed_fence_blocks_the_first_plan(session_maker, configuration, binding_key) -> None:
    request, reader = await _ready(session_maker)

    plan, _, _ = await _plan(session_maker, request, reader, STAGE_CREATE)

    assert plan.ready is False
    assert CANARY_DISABLED in plan.reasons


@pytest.mark.asyncio
async def test_a_missing_key_is_reported_diagnostically_and_blocks(session_maker, enabled, monkeypatch) -> None:
    from pydantic import SecretStr

    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key", SecretStr(""), raising=False)
    request, reader = await _ready(session_maker)

    plan, _, _ = await _plan(session_maker, request, reader, STAGE_CREATE)

    assert plan.ready is False
    assert HMAC_KEY_MISSING in plan.reasons
    # Diagnostic, not silent: the operator can see WHICH prerequisite failed.
    assert plan.snapshot["prerequisites"]["hmac_key_usable"] is False


@pytest.mark.asyncio
async def test_a_missing_template_blocks_before_any_voucher_is_created(session_maker, enabled) -> None:
    """Paying for a voucher we cannot legally deliver is the expensive mistake."""
    run_id, recipient_id = await seed_recipient(session_maker)
    request = canary_request(run_id=run_id, recipient_id=recipient_id)
    reader = FakeEasyWeekReader(marker=request.marker)

    plan, _, _ = await _plan(session_maker, request, reader, STAGE_CREATE)

    assert plan.ready is False
    assert TEMPLATE_UNPROVEN in plan.reasons


@pytest.mark.asyncio
async def test_a_missing_sender_blocks_before_any_voucher_is_created(session_maker, enabled) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    await seed_template_and_sender(session_maker, with_sender=False)
    request = canary_request(run_id=run_id, recipient_id=recipient_id)
    reader = FakeEasyWeekReader(marker=request.marker)

    plan, _, _ = await _plan(session_maker, request, reader, STAGE_CREATE)

    assert plan.ready is False
    assert SENDER_UNPROVEN in plan.reasons


@pytest.mark.asyncio
async def test_the_old_ten_percent_template_can_never_be_selected(session_maker, enabled) -> None:
    """A row under a different code is not this canary's template."""
    run_id, recipient_id = await seed_recipient(session_maker)
    await seed_template_and_sender(session_maker, code="newsletter_new_clients_monthly")
    request = canary_request(run_id=run_id, recipient_id=recipient_id)
    reader = FakeEasyWeekReader(marker=request.marker)

    plan, _, _ = await _plan(session_maker, request, reader, STAGE_CREATE)

    assert TEMPLATE_UNPROVEN in plan.reasons


# ---------------------------------------------------------------------------
# The recipient is re-proven, every stage, every time
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "seed_changes",
    [
        {"provider": "altegio"},
        {"run_status": "running"},
        {"run_mode": "send-real"},
        {"campaign_code": "some_other_campaign"},
        {"recipient_status": "skipped"},
        {"opted_out": True},
        {"phone": None},
        # The number changed between the preview and now.
        {"client_phone": "+4915100000000"},
    ],
)
async def test_a_recipient_who_no_longer_qualifies_blocks_everything(session_maker, enabled, seed_changes) -> None:
    run_id, recipient_id = await seed_recipient(session_maker, **seed_changes)
    await seed_template_and_sender(session_maker)
    request = canary_request(run_id=run_id, recipient_id=recipient_id)
    reader = FakeEasyWeekReader(marker=request.marker)

    plan, proof, _ = await _plan(session_maker, request, reader, STAGE_CREATE)

    assert plan.ready is False
    assert proof.proven is False


@pytest.mark.asyncio
async def test_an_active_future_booking_blocks_the_gift(session_maker, enabled) -> None:
    """Somebody who already re-booked does not need winning back."""
    request, reader = await _ready(session_maker)
    future = booking_payload(uuid="22222222-2222-4333-8444-555555555555", is_completed=False)
    future["start_time"] = "2099-01-01T10:00:00+00:00"
    reader.pages = {1: history_page([booking_payload(), future])}

    plan, _, _ = await _plan(session_maker, request, reader, STAGE_CREATE)

    assert plan.ready is False
    assert ACTIVE_FUTURE_BOOKING_PRESENT in plan.reasons


@pytest.mark.asyncio
async def test_a_mistyped_recipient_id_proves_nothing(session_maker, enabled) -> None:
    request, reader = await _ready(session_maker)
    wrong = canary_request(run_id=request.preview_run_id, recipient_id=request.campaign_recipient_id + 999)

    plan, _, _ = await _plan(session_maker, wrong, reader, STAGE_CREATE)

    assert plan.ready is False
    assert RECIPIENT_IDENTITY_UNPROVEN in plan.reasons


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_proven_create_claims_first_then_sends_exactly_one_request(session_maker, enabled) -> None:
    request, reader = await _ready(session_maker)
    mutator = FakeMutator(reader, marker=request.marker)

    report = await _create(session_maker, request, reader, mutator)

    assert report.outcome == OUTCOME_PROVEN
    assert mutator.calls == ["create"]
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_DELIVERY_CREATED
    assert snapshot.target_order_uuid is not None
    # The code is bound, and the binding is not the code.
    assert snapshot.voucher_code_hmac is not None
    assert VOUCHER_CODE_SENTINEL not in (snapshot.voucher_code_hmac or "")
    assert snapshot.stage_timestamps["create_claimed_at"] is not None


@pytest.mark.asyncio
async def test_the_create_request_names_the_proven_customer_and_the_marker(session_maker, enabled) -> None:
    request, reader = await _ready(session_maker)
    mutator = FakeMutator(reader, marker=request.marker)

    await _create(session_maker, request, reader, mutator)

    sent = mutator.create_kwargs[0]
    assert sent["marker"] == request.marker
    assert sent["price_minor"] == 1500
    assert sent["staffer_uuid"] == request.staffer_uuid


@pytest.mark.asyncio
async def test_an_unknown_create_is_never_retried(session_maker, enabled) -> None:
    request, reader = await _ready(session_maker)
    mutator = FakeMutator(reader, marker=request.marker, create_error=EasyWeekVoucherMutationUnknown("lost"))

    first = await _create(session_maker, request, reader, mutator)
    mutator.create_error = None
    second = await _create(session_maker, request, reader, mutator)

    assert first.outcome == OUTCOME_UNKNOWN
    assert first.reasons == [MUTATION_UNKNOWN]
    assert second.outcome == OUTCOME_REFUSED
    assert mutator.calls == ["create"]


@pytest.mark.asyncio
async def test_a_stage_without_apply_sends_nothing(session_maker, enabled) -> None:
    request, reader = await _ready(session_maker)
    mutator = RefusingMutator()

    async with session_maker() as session:
        report = await runner_module.run_create(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            plan_digest="whatever",
            plan_issued_at=None,
            confirmation_phrase="whatever",
            apply=False,
        )

    assert report.outcome == OUTCOME_REFUSED
    assert (await ledger_module.load(session_maker)).exists is False


@pytest.mark.asyncio
async def test_a_stale_digest_authorises_nothing(session_maker, enabled) -> None:
    request, reader = await _ready(session_maker)

    report = await _create(session_maker, request, reader, RefusingMutator(), plan_digest="0" * 64)

    assert report.outcome == OUTCOME_REFUSED


# ---------------------------------------------------------------------------
# Pay
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_proven_pay_sends_exactly_one_request(session_maker, enabled) -> None:
    request, reader = await _ready(session_maker)
    mutator = FakeMutator(reader, marker=request.marker)
    await _create(session_maker, request, reader, mutator)
    reader.order = voucher_order(marker=request.marker)

    report = await _pay(session_maker, request, reader, mutator)

    assert report.outcome == OUTCOME_PROVEN
    assert mutator.calls == ["create", "pay"]
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_DELIVERY_PAID
    assert mutator.pay_kwargs[0]["account_uuid"] == request.payment_account_uuid


@pytest.mark.asyncio
async def test_a_pay_is_impossible_before_a_proven_create(session_maker, enabled) -> None:
    request, reader = await _ready(session_maker)

    report = await _pay(session_maker, request, reader, RefusingMutator())

    assert report.outcome == OUTCOME_REFUSED


@pytest.mark.asyncio
async def test_a_voucher_whose_code_no_longer_binds_is_never_paid_for(session_maker, enabled) -> None:
    """A body whose code changed is not the voucher this row was opened on."""
    request, reader = await _ready(session_maker)
    mutator = FakeMutator(reader, marker=request.marker)
    await _create(session_maker, request, reader, mutator)
    reader.order = voucher_order(marker=request.marker)
    reader.order["vouchers"][0]["code"] = "SOMETHING-ELSE-000"

    report = await _pay(session_maker, request, reader, mutator)

    assert report.outcome == OUTCOME_REFUSED
    assert mutator.calls == ["create"]


# ---------------------------------------------------------------------------
# Deliver
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_proven_delivery_sends_one_message_carrying_the_code(session_maker, enabled) -> None:
    request, reader, _ = await _paid(session_maker)
    sender = FakeSender()

    report = await _deliver(session_maker, request, reader, sender)

    assert report.outcome == OUTCOME_PROVEN
    assert len(sender.calls) == 1
    call = sender.calls[0]
    # Three positional parameters, the code in slot two.
    assert call["params"][1] == VOUCHER_CODE_SENTINEL
    assert call["params"][2] == BOOKING_LINK
    assert call["template_name"] == "kitilash_ka_new_client_voucher_v1"
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_DELIVERY_PROVIDER_ACCEPTED
    assert snapshot.provider_message_id == PROVIDER_MESSAGE_ID
    assert snapshot.send_attempt_count == 1


@pytest.mark.asyncio
async def test_acceptance_is_not_delivery(session_maker, enabled) -> None:
    """Meta taking the message is not a phone having shown it."""
    request, reader, _ = await _paid(session_maker)

    await _deliver(session_maker, request, reader, FakeSender())

    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_DELIVERY_PROVIDER_ACCEPTED
    assert snapshot.stage_timestamps["delivered_at"] is None
    assert snapshot.stage_timestamps["read_at"] is None


@pytest.mark.asyncio
async def test_an_unknown_send_is_never_repeated(session_maker, enabled) -> None:
    request, reader, _ = await _paid(session_maker)
    sender = FakeSender(outcome=UNKNOWN_OUTCOME)

    first = await _deliver(session_maker, request, reader, sender)
    second = await _deliver(session_maker, request, reader, FakeSender())

    assert first.outcome == OUTCOME_UNKNOWN
    assert first.reasons == [DELIVERY_OUTCOME_UNKNOWN]
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_SEND_UNKNOWN
    assert second.outcome == OUTCOME_REFUSED
    assert len(sender.calls) == 1


@pytest.mark.asyncio
async def test_a_rejected_send_does_not_reopen_the_send(session_maker, enabled) -> None:
    """Even a proven pre-action refusal spends the one attempt."""
    from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import DELIVERY_REJECTED, DeliveryOutcome

    request, reader, _ = await _paid(session_maker)
    sender = FakeSender(outcome=DeliveryOutcome(outcome=DELIVERY_REJECTED, reason="meta_rejected_request"))

    first = await _deliver(session_maker, request, reader, sender)
    second = await _deliver(session_maker, request, reader, FakeSender())

    assert first.outcome == OUTCOME_CONTRACT_MISMATCH
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_SEND_REJECTED
    assert second.outcome == OUTCOME_REFUSED


@pytest.mark.asyncio
async def test_a_second_delivery_plan_is_never_ready(session_maker, enabled) -> None:
    request, reader, _ = await _paid(session_maker)
    await _deliver(session_maker, request, reader, FakeSender())

    plan, _, _ = await _plan(session_maker, request, reader, STAGE_DELIVER)

    assert plan.ready is False
    assert DELIVERY_ALREADY_ATTEMPTED in plan.reasons


@pytest.mark.asyncio
async def test_an_unpaid_voucher_is_never_delivered(session_maker, enabled) -> None:
    request, reader = await _ready(session_maker)
    mutator = FakeMutator(reader, marker=request.marker)
    await _create(session_maker, request, reader, mutator)
    reader.order = voucher_order(marker=request.marker)

    report = await _deliver(session_maker, request, reader, RefusingSender())

    assert report.outcome == OUTCOME_REFUSED


@pytest.mark.asyncio
async def test_a_recipient_who_re_booked_between_pay_and_send_is_not_messaged(session_maker, enabled) -> None:
    """The second live guard is the whole reason it runs twice."""
    request, reader, _ = await _paid(session_maker)
    future = booking_payload(uuid="22222222-2222-4333-8444-555555555555", is_completed=False)
    future["start_time"] = "2099-01-01T10:00:00+00:00"
    reader.pages = {1: history_page([booking_payload(), future])}

    report = await _deliver(session_maker, request, reader, RefusingSender())

    assert report.outcome == OUTCOME_REFUSED
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_PAID


@pytest.mark.asyncio
async def test_a_paid_order_whose_code_changed_is_never_delivered(session_maker, enabled) -> None:
    request, reader, _ = await _paid(session_maker)
    reader.order = voucher_order(marker=request.marker, status="paid")
    reader.order["vouchers"][0]["code"] = "SOMEBODY-ELSES-CODE"

    report = await _deliver(session_maker, request, reader, RefusingSender())

    assert report.outcome == OUTCOME_REFUSED


# ---------------------------------------------------------------------------
# Refund
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_refund_before_any_send_is_allowed(session_maker, enabled) -> None:
    request, reader, mutator = await _paid(session_maker)

    report = await _refund(session_maker, request, reader, mutator)

    assert report.outcome == OUTCOME_PROVEN
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_REFUNDED


@pytest.mark.asyncio
async def test_a_refund_is_forbidden_once_a_message_may_exist(session_maker, enabled) -> None:
    """Accepted, delivered or unknown — the money stays where it is."""
    request, reader, mutator = await _paid(session_maker)
    await _deliver(session_maker, request, reader, FakeSender())

    plan, _, _ = await _plan(session_maker, request, reader, STAGE_REFUND)
    report = await _refund(session_maker, request, reader, RefusingMutator())

    assert plan.ready is False
    assert REFUND_FORBIDDEN_AFTER_SEND in plan.reasons
    assert report.outcome == OUTCOME_REFUSED


@pytest.mark.asyncio
async def test_a_refund_is_forbidden_after_an_unknown_send(session_maker, enabled) -> None:
    request, reader, mutator = await _paid(session_maker)
    await _deliver(session_maker, request, reader, FakeSender(outcome=UNKNOWN_OUTCOME))

    report = await _refund(session_maker, request, reader, RefusingMutator())

    assert report.outcome == OUTCOME_REFUSED
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_SEND_UNKNOWN


@pytest.mark.asyncio
async def test_a_rejected_pay_can_be_planned_again_after_a_fix(session_maker, enabled) -> None:
    """A validation refusal provably did not act, so a fresh approval may retry."""
    request, reader = await _ready(session_maker)
    mutator = FakeMutator(reader, marker=request.marker)
    await _create(session_maker, request, reader, mutator)
    reader.order = voucher_order(marker=request.marker)
    mutator.pay_error = EasyWeekPermanentError("rejected", status_code=422)

    first = await _pay(session_maker, request, reader, mutator)
    mutator.pay_error = None
    second = await _pay(session_maker, request, reader, mutator)

    assert first.outcome == OUTCOME_CONTRACT_MISMATCH
    assert second.outcome == OUTCOME_PROVEN
    assert mutator.calls == ["create", "pay", "pay"]


# ---------------------------------------------------------------------------
# The code never rests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_voucher_code_reaches_the_request_and_nothing_else(session_maker, enabled, caplog) -> None:
    """One sentinel, one legitimate destination, and every other surface clean."""
    caplog.set_level(logging.DEBUG)
    request, reader, _ = await _paid(session_maker)
    sender = FakeSender()

    report = await _deliver(session_maker, request, reader, sender)

    # The one place it belongs.
    assert sender.calls[0]["params"][1] == VOUCHER_CODE_SENTINEL

    snapshot = await ledger_module.load(session_maker)
    surfaces = [
        json.dumps(report.as_safe_dict(), ensure_ascii=False),
        json.dumps(snapshot.as_safe_dict(), ensure_ascii=False),
        repr(snapshot),
        repr(report),
        "\n".join(record.getMessage() for record in caplog.records),
    ]
    for surface in surfaces:
        assert VOUCHER_CODE_SENTINEL not in surface


@pytest.mark.asyncio
async def test_no_campaign_row_learns_the_code(session_maker, enabled) -> None:
    """Not the recipient, not a job, not an outbox row — none of them exist."""
    from sqlalchemy import func, select

    from altegio_bot.models.models import (
        CampaignRecipient,
        EasyWeekCampaignVoucherDeliveryAttempt,
        MessageJob,
        OutboxMessage,
    )

    request, reader, _ = await _paid(session_maker)
    await _deliver(session_maker, request, reader, FakeSender())

    async with session_maker() as session:
        jobs = await session.scalar(select(func.count()).select_from(MessageJob))
        outbox = await session.scalar(select(func.count()).select_from(OutboxMessage))
        recipient = await session.get(CampaignRecipient, request.campaign_recipient_id)
        attempts = list((await session.execute(select(EasyWeekCampaignVoucherDeliveryAttempt))).scalars().all())

    assert jobs == 0
    assert outbox == 0
    assert VOUCHER_CODE_SENTINEL not in json.dumps(recipient.__dict__, default=str)
    # The audit row records which template was used and nothing it said.
    assert len(attempts) == 1
    assert attempts[0].outcome == "provider_accepted"
    assert VOUCHER_CODE_SENTINEL not in json.dumps(attempts[0].__dict__, default=str)


@pytest.mark.asyncio
async def test_an_exception_carrying_a_report_still_leaks_nothing(session_maker, enabled) -> None:
    request, reader, _ = await _paid(session_maker)
    report = await _deliver(session_maker, request, reader, FakeSender())

    error = RuntimeError(f"canary failed: {report.as_safe_dict()}")

    assert VOUCHER_CODE_SENTINEL not in str(error)


# ---------------------------------------------------------------------------
# Structure
# ---------------------------------------------------------------------------


def test_no_function_runs_the_whole_canary_end_to_end() -> None:
    """The stop between stages is the control, not an inconvenience."""
    names = [name for name in dir(runner_module) if name.startswith("run_")]

    assert sorted(names) == [
        "run_create",
        "run_deliver",
        "run_pay",
        "run_reconcile",
        "run_refund",
        "run_status",
    ]


@pytest.mark.asyncio
async def test_every_report_repeats_that_no_campaign_is_authorised(session_maker, enabled) -> None:
    request, reader, _ = await _paid(session_maker)

    report = await _deliver(session_maker, request, reader, FakeSender())
    safe = report.as_safe_dict()

    assert safe["campaign_send_authorized"] is False
    assert safe["bulk_delivery_authorized"] is False
    assert safe["global_ready_for_send"] is False
    assert safe["voucher_code_omitted"] is True


@pytest.mark.asyncio
async def test_status_is_database_only(session_maker, enabled) -> None:
    request, reader, _ = await _paid(session_maker)
    before = list(reader.calls)

    report = await runner_module.run_status(session_maker)

    assert report.outcome == OUTCOME_PROVEN
    assert reader.calls == before


# ---------------------------------------------------------------------------
# Reconciliation actually reconciles
# ---------------------------------------------------------------------------


async def _reconcile(session_maker, request, reader):
    return await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)


async def _force(session_maker, status, **kwargs):
    """Put the ledger into an unresolved state the way a real run would."""
    snapshot = await ledger_module.load(session_maker)
    return await ledger_module.record_outcome(
        session_maker,
        status=status,
        expected_statuses=frozenset({snapshot.status or ""}),
        **kwargs,
    )


@pytest.mark.asyncio
async def test_an_unknown_pay_that_really_landed_becomes_durably_paid(session_maker, enabled) -> None:
    """The blocker: reconcile said "proven" and left the ledger unresolved.

    An unknown pay blocks every stage after it forever, so a proven reading has
    to reach the durable state — not just the operator's terminal.
    """
    request, reader = await _ready(session_maker)
    mutator = FakeMutator(reader, marker=request.marker)
    await _create(session_maker, request, reader, mutator)
    identity = runner_module._identity_from(
        request,
        (await _plan(session_maker, request, reader, STAGE_PAY))[1],
    )
    await ledger_module.claim_pay(session_maker, identity=identity, plan_digest="a" * 64)
    await _force(session_maker, VOUCHER_DELIVERY_PAY_UNKNOWN, reason_code="x")
    # EasyWeek did apply the payment; we simply never saw the answer.
    reader.order = voucher_order(marker=request.marker, status="paid")

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == OUTCOME_PROVEN
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_DELIVERY_PAID
    assert snapshot.stage_timestamps["pay_verified_at"] is not None
    assert snapshot.reconciliation_required is False
    # And the next stage is genuinely open again.
    plan, _, _ = await _plan(session_maker, request, reader, STAGE_DELIVER)
    assert plan.ready is True


@pytest.mark.asyncio
async def test_an_unknown_pay_over_a_still_open_order_stays_unknown(session_maker, enabled) -> None:
    """An open order is not proof the payment failed; it may be in flight."""
    request, reader = await _ready(session_maker)
    mutator = FakeMutator(reader, marker=request.marker)
    await _create(session_maker, request, reader, mutator)
    identity = runner_module._identity_from(request, (await _plan(session_maker, request, reader, STAGE_PAY))[1])
    await ledger_module.claim_pay(session_maker, identity=identity, plan_digest="a" * 64)
    await _force(session_maker, VOUCHER_DELIVERY_PAY_UNKNOWN, reason_code="x")
    reader.order = voucher_order(marker=request.marker)

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == OUTCOME_UNKNOWN
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_DELIVERY_PAY_UNKNOWN
    assert snapshot.reconciliation_required is True


@pytest.mark.asyncio
async def test_an_unknown_refund_that_really_landed_becomes_durably_refunded(session_maker, enabled) -> None:
    request, reader, mutator = await _paid(session_maker)
    identity = runner_module._identity_from(request, (await _plan(session_maker, request, reader, STAGE_REFUND))[1])
    await ledger_module.claim_refund(session_maker, identity=identity, plan_digest="b" * 64)
    await _force(session_maker, VOUCHER_DELIVERY_REFUND_UNKNOWN, reason_code="x")
    reader.order = voucher_order(marker=request.marker, status="refunded")

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == OUTCOME_PROVEN
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_DELIVERY_REFUNDED
    assert snapshot.stage_timestamps["refund_verified_at"] is not None
    assert snapshot.manual_cleanup_required is False


@pytest.mark.asyncio
async def test_reconcile_never_reaches_a_mutation_client() -> None:
    """Structural: reconcile has no mutator parameter to pass one through."""
    import inspect

    signature = inspect.signature(runner_module.run_reconcile)

    assert "mutator" not in signature.parameters
    assert "sender" not in signature.parameters


@pytest.mark.asyncio
async def test_a_known_state_with_the_wrong_marker_is_never_proven(session_maker, enabled) -> None:
    """`classify_order` returning a familiar word proves nothing by itself."""
    request, reader = await _ready(session_maker)
    mutator = FakeMutator(reader, marker=request.marker)
    await _create(session_maker, request, reader, mutator)
    identity = runner_module._identity_from(request, (await _plan(session_maker, request, reader, STAGE_PAY))[1])
    await ledger_module.claim_pay(session_maker, identity=identity, plan_digest="a" * 64)
    await _force(session_maker, VOUCHER_DELIVERY_PAY_UNKNOWN, reason_code="x")
    reader.order = voucher_order(marker="somebody-elses-marker", status="paid")

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome != OUTCOME_PROVEN
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_PAY_UNKNOWN


@pytest.mark.asyncio
async def test_two_marker_orders_durably_stop_the_canary(session_maker, enabled) -> None:
    request, reader = await _ready(session_maker)
    mutator = FakeMutator(
        reader,
        marker=request.marker,
        create_error=EasyWeekVoucherMutationUnknown("lost"),
    )
    await _create(session_maker, request, reader, mutator)
    reader.order_pages = [
        orders_page(
            [
                await marker_order(session_maker, marker=request.marker),
                await marker_order(session_maker, marker=request.marker, uuid=str(OTHER_UUID)),
            ]
        )
    ]

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == runner_module.OUTCOME_AMBIGUOUS
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == "ambiguous"
    assert snapshot.manual_cleanup_required is True
    # And no further mutation stage is reachable.
    plan, _, _ = await _plan(session_maker, request, reader, STAGE_PAY)
    assert plan.ready is False


@pytest.mark.asyncio
async def test_a_manually_closed_draft_becomes_durably_manually_cleaned(session_maker, enabled) -> None:
    """Observed, never claimed: the application did not close it."""
    request, reader = await _ready(session_maker)
    # The create left as an unknown: the request went out, the answer did not
    # come back, and the ledger has no order UUID to read.
    mutator = FakeMutator(reader, marker=request.marker, create_error=EasyWeekVoucherMutationUnknown("lost"))
    await _create(session_maker, request, reader, mutator)
    closed = await marker_order(session_maker, marker=request.marker, status="canceled", is_canceled=True)
    reader.order = closed
    reader.order_pages = [orders_page([closed])]

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == OUTCOME_PROVEN
    assert _OUTCOME_EXIT_CODES[report.outcome] == EXIT_OK
    assert report.manual_cleanup_required is False
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == "manually_cleaned"
    assert snapshot.manual_cleanup_required is False
    assert snapshot.stage_timestamps["manual_cleanup_observed_at"] is not None
    # No refund attempt was invented on our behalf.
    assert snapshot.stage_timestamps["refund_attempted_at"] is None


@pytest.mark.asyncio
async def test_a_paid_order_with_no_payment_provenance_stops_the_canary(session_maker, enabled) -> None:
    """Paid, with no payment we can account for. Not ours to call proven."""
    request, reader = await _ready(session_maker)
    mutator = FakeMutator(reader, marker=request.marker, create_error=EasyWeekVoucherMutationUnknown("lost"))
    await _create(session_maker, request, reader, mutator)
    paid = await marker_order(session_maker, marker=request.marker, status="paid")
    reader.order = paid
    reader.order_pages = [orders_page([paid])]

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == runner_module.OUTCOME_AMBIGUOUS
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == "ambiguous"
    assert snapshot.stage_timestamps["pay_attempted_at"] is None
    plan, _, _ = await _plan(session_maker, request, reader, STAGE_DELIVER)
    assert plan.ready is False


@pytest.mark.asyncio
async def test_reconcile_under_another_recipient_reads_nothing(session_maker, enabled) -> None:
    request, reader, _ = await _paid(session_maker)
    before = list(reader.calls)
    foreign = canary_request(run_id=request.preview_run_id, recipient_id=request.campaign_recipient_id + 7)

    report = await _reconcile(session_maker, foreign, reader)

    assert report.outcome == OUTCOME_REFUSED
    assert reader.calls == before


# ---------------------------------------------------------------------------
# An already-refunded order is never refunded again
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_already_refunded_order_gives_no_ready_refund_plan(session_maker, enabled) -> None:
    request, reader, mutator = await _paid(session_maker)
    reader.order = voucher_order(marker=request.marker, status="refunded")

    plan, _, _ = await _plan(session_maker, request, reader, STAGE_REFUND)

    assert plan.ready is False
    assert VOUCHER_ORDER_ALREADY_REFUNDED in plan.reasons
    assert mutator.calls == ["create", "pay"]


@pytest.mark.asyncio
async def test_a_refund_that_becomes_redundant_between_plan_and_apply_sends_nothing(session_maker, enabled) -> None:
    """The race the plan alone cannot close.

    The plan was built while the order was paid; somebody refunded it in the
    dashboard a second later. The last read before the claim is what stops a
    second real refund against a provider with no idempotency key.
    """
    request, reader, mutator = await _paid(session_maker)
    plan, _, _ = await _plan(session_maker, request, reader, STAGE_REFUND)
    assert plan.ready is True
    calls_before = list(mutator.calls)
    reader.order = voucher_order(marker=request.marker, status="refunded")

    async with session_maker() as session:
        report = await runner_module.run_refund(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            plan_digest=plan.digest,
            plan_issued_at=plan.issued_at,
            confirmation_phrase=plan.confirmation_phrase,
            apply=True,
        )

    assert mutator.calls == calls_before
    assert report.external_mutation_attempted is False
    assert VOUCHER_ORDER_ALREADY_REFUNDED in report.reasons


# ---------------------------------------------------------------------------
# The cleanup path does not depend on the delivery machinery
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "break_it",
    [
        "hmac_key_missing",
        "hmac_key_id_missing",
        "hmac_key_rotated",
        "template_missing",
        "template_inactive",
        "sender_missing",
        "sender_inactive",
        "voucher_code_missing",
        "vouchers_missing",
        "vouchers_malformed",
        "artifact_unprovable",
    ],
)
async def test_a_refund_stays_available_after_an_unreadable_artifact(
    session_maker, enabled, monkeypatch, break_it
) -> None:
    """Every one of these is a reason to get the money BACK.

    A template Meta paused, a sender switched off, a key rotated, a voucher body
    that stopped making sense — none of them is a reason to leave a real €15
    sitting in a paid order nobody will ever deliver.
    """
    from pydantic import SecretStr
    from sqlalchemy import delete, update

    from altegio_bot.models.models import MessageTemplate, WhatsAppSender

    request, reader, mutator = await _paid(session_maker)

    if break_it == "hmac_key_missing":
        monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key", SecretStr(""), raising=False)
    elif break_it == "hmac_key_id_missing":
        monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key_id", "", raising=False)
    elif break_it == "hmac_key_rotated":
        monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key", SecretStr("j" * 48), raising=False)
        monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key_id", "rotated", raising=False)
    elif break_it in {"template_missing", "template_inactive", "sender_missing", "sender_inactive"}:
        async with session_maker() as session:
            async with session.begin():
                if break_it == "template_missing":
                    await session.execute(delete(MessageTemplate))
                elif break_it == "template_inactive":
                    await session.execute(update(MessageTemplate).values(is_active=False))
                elif break_it == "sender_missing":
                    await session.execute(delete(WhatsAppSender))
                else:
                    await session.execute(update(WhatsAppSender).values(is_active=False))
    else:
        order = voucher_order(marker=request.marker, status="paid")
        if break_it == "voucher_code_missing":
            del order["vouchers"][0]["code"]
        elif break_it == "vouchers_missing":
            del order["vouchers"]
        elif break_it == "vouchers_malformed":
            order["vouchers"] = "not a list"
        else:
            order["vouchers"] = [{"voucher_template_uuid": str(OTHER_UUID), "price": 9999}]
        reader.order = order

    plan, _, prerequisites = await _plan(session_maker, request, reader, STAGE_REFUND)
    assert plan.ready is True, plan.reasons
    # The report is honest about what a refund did and did not check.
    safe = prerequisites.as_safe_dict()
    assert safe["delivery_checks_applied"] is False
    for key in ("hmac_key_usable", "template_proven", "sender_proven"):
        assert safe[key] == "not_required_for_refund"

    report = await _refund(session_maker, request, reader, mutator)

    assert report.outcome == OUTCOME_PROVEN
    assert mutator.calls == ["create", "pay", "refund"]
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_REFUNDED


@pytest.mark.asyncio
async def test_a_refund_still_needs_its_own_authorisation_and_identity(session_maker, enabled) -> None:
    """Loosened prerequisites, not loosened authorisation."""
    request, reader, mutator = await _paid(session_maker)

    stale = await _refund(session_maker, request, reader, mutator, plan_digest="0" * 64)
    unapplied = await _refund(session_maker, request, reader, mutator, apply=False)

    assert stale.outcome == OUTCOME_REFUSED
    assert unapplied.outcome == OUTCOME_REFUSED
    assert mutator.calls == ["create", "pay"]


@pytest.mark.asyncio
async def test_a_refund_after_any_send_attempt_remains_forbidden(session_maker, enabled) -> None:
    request, reader, mutator = await _paid(session_maker)
    await _deliver(session_maker, request, reader, FakeSender(outcome=UNKNOWN_OUTCOME))

    plan, _, _ = await _plan(session_maker, request, reader, STAGE_REFUND)
    report = await _refund(session_maker, request, reader, RefusingMutator())

    assert plan.ready is False
    assert report.outcome == OUTCOME_REFUSED


# ---------------------------------------------------------------------------
# A delivered voucher leaves nothing to clean up
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_proven_delivery_clears_the_cleanup_flag(session_maker, enabled) -> None:
    """The blocker: status asked for manual cleanup forever after success."""
    request, reader, _ = await _paid(session_maker)
    assert (await ledger_module.load(session_maker)).manual_cleanup_required is True

    report = await _deliver(session_maker, request, reader, FakeSender())

    assert report.manual_cleanup_required is False
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.manual_cleanup_required is False
    status = await runner_module.run_status(session_maker)
    assert status.manual_cleanup_required is False
    assert status.ledger["manual_cleanup_required"] is False


@pytest.mark.asyncio
async def test_the_webhook_ladder_keeps_the_cleanup_flag_down(session_maker, enabled) -> None:
    request, reader, _ = await _paid(session_maker)
    await _deliver(session_maker, request, reader, FakeSender())

    for status in ("delivered", "read", "delivered"):
        await ledger_module.record_webhook_transition(
            session_maker, provider_message_id=PROVIDER_MESSAGE_ID, status=status
        )
        snapshot = await ledger_module.load(session_maker)
        assert snapshot.manual_cleanup_required is False

    # The duplicate, out-of-order callback did not walk the state back either.
    assert (await ledger_module.load(session_maker)).status == "read"


@pytest.mark.asyncio
async def test_an_unknown_send_still_asks_for_a_human(session_maker, enabled) -> None:
    request, reader, _ = await _paid(session_maker)

    report = await _deliver(session_maker, request, reader, FakeSender(outcome=UNKNOWN_OUTCOME))

    assert report.manual_cleanup_required is True
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.manual_cleanup_required is True
    assert snapshot.reconciliation_required is True
    status = await runner_module.run_status(session_maker)
    assert status.reconciliation_required is True


@pytest.mark.asyncio
async def test_a_refunded_canary_asks_for_no_cleanup(session_maker, enabled) -> None:
    request, reader, mutator = await _paid(session_maker)

    await _refund(session_maker, request, reader, mutator)

    status = await runner_module.run_status(session_maker)
    assert status.manual_cleanup_required is False


# ---------------------------------------------------------------------------
# A refund somebody else performed still has to close this ledger
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_manual_dashboard_refund_settles_the_ledger_without_a_post(session_maker, enabled) -> None:
    """The blocker: no POST, but the ledger stayed `paid` forever.

    The rebuilt plan refuses — correctly — because the order is already
    refunded. Refusing and stopping there left `paid` against a refunded order,
    which every later reconcile answered with `contract_mismatch` and no way out.
    """
    request, reader, mutator = await _paid(session_maker)
    calls_before = list(mutator.calls)
    reader.order = voucher_order(marker=request.marker, status="refunded")

    report = await _refund(session_maker, request, reader, mutator)

    assert mutator.calls == calls_before
    assert report.external_mutation_attempted is False
    # The operation finished. The informational reason explains why no POST went
    # out; it does not make a completed transition look like a failure.
    assert report.outcome == OUTCOME_PROVEN
    assert VOUCHER_ORDER_ALREADY_REFUNDED in report.reasons
    assert report.manual_cleanup_required is False
    assert report.reconciliation_required is False
    assert _OUTCOME_EXIT_CODES[report.outcome] == EXIT_OK
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_DELIVERY_MANUALLY_CLEANED
    assert snapshot.reconciliation_required is False
    assert snapshot.manual_cleanup_required is False
    assert snapshot.stage_timestamps["manual_cleanup_observed_at"] is not None
    # No refund provenance is claimed anywhere on the row.
    assert snapshot.stage_timestamps["refund_claimed_at"] is None
    assert snapshot.stage_timestamps["refund_attempted_at"] is None
    assert snapshot.stage_timestamps["refund_verified_at"] is None
    assert snapshot.stage_plan_digests["refund"] is None


@pytest.mark.asyncio
async def test_an_external_refund_is_never_recorded_as_our_refund(session_maker, enabled) -> None:
    """`manually_cleaned`, not `refunded`: this application refunded nothing.

    Nothing is back-filled either. A claim timestamp, an attempt, a verification
    or a plan digest would each be a record of a request that never went out.
    """
    request, reader, mutator = await _paid(session_maker)
    reader.order = voucher_order(marker=request.marker, status="refunded")

    await _refund(session_maker, request, reader, mutator)

    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status != VOUCHER_DELIVERY_REFUNDED
    assert snapshot.stage_timestamps["refund_claimed_at"] is None
    assert snapshot.stage_timestamps["refund_attempted_at"] is None
    assert snapshot.stage_timestamps["refund_verified_at"] is None
    assert snapshot.stage_timestamps["manual_cleanup_observed_at"] is not None
    assert snapshot.stage_plan_digests["refund"] is None


@pytest.mark.asyncio
async def test_a_stale_refund_plan_over_an_externally_refunded_order_still_settles(session_maker, enabled) -> None:
    """The plan→apply race, followed through to the durable state.

    The approval was issued while the order was paid, so it cannot match the
    world any more. That is precisely the case that must still end somewhere.
    """
    request, reader, mutator = await _paid(session_maker)
    plan, _, _ = await _plan(session_maker, request, reader, STAGE_REFUND)
    assert plan.ready is True
    calls_before = list(mutator.calls)
    reader.order = voucher_order(marker=request.marker, status="refunded")

    async with session_maker() as session:
        report = await runner_module.run_refund(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            plan_digest=plan.digest,
            plan_issued_at=plan.issued_at,
            confirmation_phrase=plan.confirmation_phrase,
            apply=True,
        )

    assert mutator.calls == calls_before
    assert report.external_mutation_attempted is False
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_MANUALLY_CLEANED


@pytest.mark.asyncio
async def test_a_settlement_refuses_a_refunded_order_that_is_not_ours(session_maker, enabled) -> None:
    """A refunded order with a stranger's marker closes nothing."""
    request, reader, mutator = await _paid(session_maker)
    calls_before = list(mutator.calls)
    reader.order = voucher_order(marker="somebody-elses-marker", status="refunded")

    report = await _refund(session_maker, request, reader, mutator)

    assert mutator.calls == calls_before
    assert report.outcome == OUTCOME_REFUSED
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_PAID


@pytest.mark.asyncio
async def test_a_refund_without_apply_settles_nothing(session_maker, enabled) -> None:
    """A preview reads and reports. It does not move the durable state."""
    request, reader, mutator = await _paid(session_maker)
    reader.order = voucher_order(marker=request.marker, status="refunded")

    report = await _refund(session_maker, request, reader, mutator, apply=False)

    assert report.outcome == OUTCOME_REFUSED
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_PAID


@pytest.mark.asyncio
async def test_reconcile_closes_a_paid_ledger_whose_order_was_refunded_by_hand(session_maker, enabled) -> None:
    """The same ending must be reachable without running the refund command."""
    request, reader, mutator = await _paid(session_maker)
    calls_before = list(mutator.calls)
    reader.order = voucher_order(marker=request.marker, status="refunded")

    report = await _reconcile(session_maker, request, reader)

    assert mutator.calls == calls_before
    assert report.outcome == OUTCOME_PROVEN
    assert _OUTCOME_EXIT_CODES[report.outcome] == EXIT_OK
    assert report.manual_cleanup_required is False
    assert report.reconciliation_required is False
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_DELIVERY_MANUALLY_CLEANED
    assert snapshot.reconciliation_required is False
    assert snapshot.stage_timestamps["refund_attempted_at"] is None


@pytest.mark.asyncio
async def test_a_delivered_canary_is_never_settled_by_an_external_refund(session_maker, enabled) -> None:
    """A message reached a customer. Nothing about that is "cleaned up"."""
    request, reader, _ = await _paid(session_maker)
    await _deliver(session_maker, request, reader, FakeSender())
    reader.order = voucher_order(marker=request.marker, status="refunded")

    report = await _reconcile(session_maker, request, reader)

    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_DELIVERY_PROVIDER_ACCEPTED
    assert report.outcome == OUTCOME_CONTRACT_MISMATCH


@pytest.mark.asyncio
async def test_the_settlement_helper_cannot_reach_a_mutation_client() -> None:
    """Structural: no settlement path has a mutator or a sender to call."""
    import inspect

    for function in (runner_module.settle_external_cleanup, runner_module._settle_externally_refunded):
        parameters = inspect.signature(function).parameters
        assert "mutator" not in parameters
        assert "sender" not in parameters


# ---------------------------------------------------------------------------
# The create reconciliation proves identity before it closes anything
# ---------------------------------------------------------------------------


async def _lost_create(session_maker):
    """A create whose answer never came back: no order UUID in the ledger."""
    request, reader = await _ready(session_maker)
    mutator = FakeMutator(reader, marker=request.marker, create_error=EasyWeekVoucherMutationUnknown("lost"))
    await _create(session_maker, request, reader, mutator)
    return request, reader


@pytest.mark.asyncio
async def test_a_candidate_whose_exact_read_carries_another_marker_stays_unresolved(session_maker, enabled) -> None:
    """The blocker: a listing hit was treated as identity.

    The listing was scoped by branch and customer, which is not the same
    question as "is this our order?". The exact read is where the marker is
    actually compared — and a foreign order must not close this canary.
    """
    request, reader = await _lost_create(session_maker)
    reader.order_pages = [
        orders_page([await marker_order(session_maker, marker=request.marker, status="canceled", is_canceled=True)])
    ]
    # What the exact read returns is a different order entirely.
    reader.order = voucher_order(marker="somebody-elses-marker", status="canceled", is_canceled=True)

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == OUTCOME_UNKNOWN
    assert VOUCHER_ORDER_UNPROVEN in report.reasons
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_DELIVERY_CREATE_UNKNOWN
    assert snapshot.reconciliation_required is True


@pytest.mark.asyncio
async def test_a_candidate_bound_to_another_customer_stays_unresolved(session_maker, enabled) -> None:
    request, reader = await _lost_create(session_maker)
    closed = await marker_order(session_maker, marker=request.marker, status="canceled", is_canceled=True)
    reader.order_pages = [orders_page([closed])]
    reader.order = voucher_order(
        marker=request.marker,
        status="canceled",
        is_canceled=True,
        customer={"uuid": str(OTHER_UUID)},
    )

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == OUTCOME_UNKNOWN
    assert VOUCHER_ORDER_UNPROVEN in report.reasons
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_CREATE_UNKNOWN


@pytest.mark.asyncio
async def test_a_candidate_whose_exact_read_answers_with_another_uuid_stays_unresolved(session_maker, enabled) -> None:
    """The read must be of the order we asked about, not merely a valid one."""
    request, reader = await _lost_create(session_maker)
    reader.order_pages = [
        orders_page([await marker_order(session_maker, marker=request.marker, status="canceled", is_canceled=True)])
    ]
    reader.order = voucher_order(
        marker=request.marker,
        status="canceled",
        is_canceled=True,
        uuid=str(OTHER_UUID),
    )

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == OUTCOME_UNKNOWN
    assert VOUCHER_ORDER_UNPROVEN in report.reasons
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_CREATE_UNKNOWN


@pytest.mark.asyncio
async def test_a_foreign_paid_order_does_not_make_this_canary_ambiguous(session_maker, enabled) -> None:
    """`ambiguous` is a claim about OUR order being unattributable."""
    request, reader = await _lost_create(session_maker)
    reader.order_pages = [orders_page([await marker_order(session_maker, marker=request.marker, status="paid")])]
    reader.order = voucher_order(marker="somebody-elses-marker", status="paid")

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome != runner_module.OUTCOME_AMBIGUOUS
    assert VOUCHER_ORDER_UNPROVEN in report.reasons
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_CREATE_UNKNOWN


@pytest.mark.asyncio
async def test_the_create_reconciliation_still_closes_its_own_cancelled_order(session_maker, enabled) -> None:
    """Proving identity must not break the case identity actually holds."""
    request, reader = await _lost_create(session_maker)
    closed = await marker_order(session_maker, marker=request.marker, status="canceled", is_canceled=True)
    reader.order_pages = [orders_page([closed])]
    reader.order = closed

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == OUTCOME_PROVEN
    assert "get_order" in reader.calls
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_DELIVERY_MANUALLY_CLEANED


@pytest.mark.asyncio
async def test_the_create_reconciliation_still_flags_its_own_unexplained_paid_order(session_maker, enabled) -> None:
    request, reader = await _lost_create(session_maker)
    paid = await marker_order(session_maker, marker=request.marker, status="paid")
    reader.order_pages = [orders_page([paid])]
    reader.order = paid

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == runner_module.OUTCOME_AMBIGUOUS
    assert (await ledger_module.load(session_maker)).status == "ambiguous"


@pytest.mark.asyncio
async def test_a_lost_compare_and_set_is_never_reported_as_ambiguous(session_maker, enabled, monkeypatch) -> None:
    """Another writer moved first, so THIS attempt proved nothing.

    Reporting `ambiguous` after a refused compare-and-set would describe a
    transition that did not happen.
    """
    request, reader = await _lost_create(session_maker)
    paid = await marker_order(session_maker, marker=request.marker, status="paid")
    reader.order_pages = [orders_page([paid])]
    reader.order = paid

    real = ledger_module.record_outcome

    async def refused(*args, **kwargs):
        result = await real(*args, **kwargs)
        return ledger_module.RecordOutcome(applied=False, reason="lost_race", snapshot=result.snapshot)

    monkeypatch.setattr(ledger_module, "record_outcome", refused)

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == OUTCOME_UNKNOWN


@pytest.mark.asyncio
async def test_a_cleanup_that_is_still_pending_never_reports_success(session_maker, enabled) -> None:
    """The mirror image of the blocker, and just as wrong.

    A rejected pay leaves a real draft in EasyWeek that a human has to close by
    hand. The ledger and the order agree about that, but "agreeing" is not the
    same as "finished", and a zero exit code would tell a wrapper there is
    nothing left to do.
    """
    request, reader = await _ready(session_maker)
    mutator = FakeMutator(reader, marker=request.marker)
    await _create(session_maker, request, reader, mutator)
    reader.order = voucher_order(marker=request.marker)
    mutator.pay_error = EasyWeekPermanentError("rejected", status_code=422)
    await _pay(session_maker, request, reader, mutator)

    report = await _reconcile(session_maker, request, reader)

    assert report.manual_cleanup_required is True
    assert report.outcome == runner_module.OUTCOME_MANUAL_CLEANUP
    assert _OUTCOME_EXIT_CODES[report.outcome] == EXIT_MANUAL_CLEANUP
    assert MANUAL_CLEANUP_REQUIRED in report.reasons


@pytest.mark.asyncio
async def test_an_unknown_send_never_reports_success(session_maker, enabled) -> None:
    """The customer may be holding the code. Nothing about that is `proven`."""
    request, reader, _ = await _paid(session_maker)
    await _deliver(session_maker, request, reader, FakeSender(outcome=UNKNOWN_OUTCOME))

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == OUTCOME_UNKNOWN
    assert _OUTCOME_EXIT_CODES[report.outcome] != EXIT_OK
    assert report.manual_cleanup_required is True
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_SEND_UNKNOWN


@pytest.mark.asyncio
async def test_a_settlement_that_loses_the_race_reports_no_success(session_maker, enabled, monkeypatch) -> None:
    """Another writer moved first, so this call settled nothing."""
    request, reader, mutator = await _paid(session_maker)
    reader.order = voucher_order(marker=request.marker, status="refunded")

    real = ledger_module.record_outcome

    async def refused(*args, **kwargs):
        result = await real(*args, **kwargs)
        return ledger_module.RecordOutcome(applied=False, reason="lost_race", snapshot=result.snapshot)

    monkeypatch.setattr(ledger_module, "record_outcome", refused)

    report = await _refund(session_maker, request, reader, mutator)

    assert report.outcome != OUTCOME_PROVEN
    assert _OUTCOME_EXIT_CODES[report.outcome] != EXIT_OK
    assert mutator.calls == ["create", "pay"]
    # The snapshot that comes back is the live row, not an invented ending.
    assert report.ledger["status"] == (await ledger_module.load(session_maker)).status


# ---------------------------------------------------------------------------
# The create window is bounded, and the tests say so without consulting a clock
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_order_inside_the_proven_window_is_recognised(session_maker, enabled) -> None:
    """Whatever the calendar says today, the middle of the window is inside it."""
    request, reader = await _lost_create(session_maker)
    start, end = await create_window(session_maker)
    assert start < end
    closed = await marker_order(session_maker, marker=request.marker, status="canceled", is_canceled=True)
    reader.order_pages = [orders_page([closed])]
    reader.order = closed

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == OUTCOME_PROVEN
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_MANUALLY_CLEANED


@pytest.mark.asyncio
async def test_an_order_created_before_the_window_is_not_ours(session_maker, enabled) -> None:
    """An older order carrying the same comment is not this canary's order."""
    request, reader = await _lost_create(session_maker)
    start, _ = await create_window(session_maker)
    early = await marker_order(
        session_maker,
        marker=request.marker,
        status="canceled",
        is_canceled=True,
        at=start - timedelta(seconds=1),
    )
    reader.order_pages = [orders_page([early])]
    reader.order = early

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == OUTCOME_UNKNOWN
    assert VOUCHER_ORDER_UNPROVEN in report.reasons
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_CREATE_UNKNOWN


@pytest.mark.asyncio
async def test_an_order_created_after_the_window_is_not_ours(session_maker, enabled) -> None:
    request, reader = await _lost_create(session_maker)
    _, end = await create_window(session_maker)
    late = await marker_order(
        session_maker,
        marker=request.marker,
        status="canceled",
        is_canceled=True,
        at=end + timedelta(seconds=1),
    )
    reader.order_pages = [orders_page([late])]
    reader.order = late

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == OUTCOME_UNKNOWN
    assert VOUCHER_ORDER_UNPROVEN in report.reasons
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_CREATE_UNKNOWN


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "created_at",
    [
        pytest.param(None, id="absent"),
        pytest.param("", id="empty"),
        pytest.param("2026-09-12 12:00:00", id="no_timezone"),
        pytest.param("the twelfth", id="unparseable"),
    ],
)
async def test_a_created_at_that_cannot_be_trusted_is_fail_closed(session_maker, enabled, created_at) -> None:
    """No usable timestamp is not "inside the window"; it is not a match."""
    request, reader = await _lost_create(session_maker)
    row = voucher_order(marker=request.marker, status="canceled", is_canceled=True)
    if created_at is None:
        row.pop("created_at")
    else:
        row["created_at"] = created_at
    reader.order_pages = [orders_page([row])]
    reader.order = row

    report = await _reconcile(session_maker, request, reader)

    assert report.outcome == OUTCOME_UNKNOWN
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_CREATE_UNKNOWN


@pytest.mark.asyncio
async def test_the_window_tests_never_consult_the_wall_clock(session_maker, enabled) -> None:
    """Structural: the window comes from the ledger, not from today's date.

    The bug this replaces was a fixture constant that sat inside the window on
    the afternoon it was written and outside it by teatime.
    """
    request, reader = await _ready(session_maker)
    mutator = FakeMutator(reader, marker=request.marker, create_error=EasyWeekVoucherMutationUnknown("lost"))
    await _create(session_maker, request, reader, mutator)
    start, end = await create_window(session_maker)
    row = await marker_order(session_maker, marker=request.marker)

    moment = datetime.fromisoformat(row["created_at"])
    assert start < moment < end
    # Comfortably inside, not clinging to an edge.
    assert moment - start > timedelta(minutes=1)
    assert end - moment > timedelta(minutes=1)
    assert moment != NOW
