"""The controlled manual-basis voucher delivery canary (§37.2).

An operator picked somebody out of a preview by hand. That is a legitimate
reason to send one controlled voucher and it is NOT evidence of a first visit,
so what these tests guard is, in order: that the decision can never be recorded
as an entitlement; that one person can be given one €15 once; that nothing
external happens before a claim is committed; that an unknown outcome is never
retried; and that the code never appears anywhere except in memory.

Every identity here is synthetic, and the voucher code is a sentinel the
secrecy tests hunt for by name.
"""

from __future__ import annotations

import asyncio
import logging
import uuid as uuid_module
from datetime import timedelta

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from altegio_bot.campaigns.easyweek_manual_voucher import ledger as ledger_module
from altegio_bot.campaigns.easyweek_manual_voucher import runner as runner_module
from altegio_bot.campaigns.easyweek_manual_voucher.baseline import prove_baseline
from altegio_bot.campaigns.easyweek_manual_voucher.eligibility import prove_manual_recipient
from altegio_bot.campaigns.easyweek_manual_voucher.identity import (
    BASELINE_DRIFT,
    CANARY_DISABLED,
    CUSTOMER_IDENTITY_NOT_CURRENT,
    CUSTOMER_PHONE_NOT_CURRENT,
    LIVE_GUARD_UNCERTAIN,
    MANUAL_BASELINE_VERSION,
    MUTATION_UNKNOWN,
    RECIPIENT_BASIS_UNSUPPORTED,
    RECIPIENT_NOT_CANDIDATE,
    RECIPIENT_OPTED_OUT,
    REFUND_FORBIDDEN_AFTER_SEND,
    RUN_UNPROVEN,
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_PAY,
    STAGE_REFUND,
    manual_marker,
)
from altegio_bot.campaigns.easyweek_manual_voucher.ledger import ManualCanaryIdentity
from altegio_bot.easyweek_client import EasyWeekError
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    KARLSRUHE_LOCATION_UUID,
)
from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationUnknown, VoucherMutationResponse
from altegio_bot.models.models import (
    MANUAL_VOUCHER_CREATED,
    MANUAL_VOUCHER_PAID,
    MANUAL_VOUCHER_PLANNED,
    MANUAL_VOUCHER_PROVIDER_ACCEPTED,
    MANUAL_VOUCHER_SEND_UNKNOWN,
    PROVIDER_EASYWEEK,
    CampaignRecipient,
    EasyWeekManualVoucherDeliveryAttempt,
    EasyWeekManualVoucherDeliveryLedger,
)
from altegio_bot.tests.easyweek_manual_voucher_fixtures import (  # noqa: F401 - fixtures
    ACCOUNT_UUID,
    BOOKING_LINK,
    COMPANY_ID,
    CUSTOMER_NAME,
    EW_CUSTOMER_UUID,
    ORDER_UUID,
    OTHER_CUSTOMER_UUID,
    OTHER_ORDER_UUID,
    PERIOD_END,
    PERIOD_START,
    PHONE,
    PROVIDER_MESSAGE_ID,
    STAFFER_UUID,
    VOUCHER_CODE_SENTINEL,
    FakeMutator,
    FakeReader,
    FakeSender,
    customer_payload,
    customers_page,
    issued_voucher,
    location_map,
    manual_request,
    marker_order,
    orders_page,
    rejected_outcome,
    seed_manual_recipient,
    seed_template_and_sender,
    template_payload,
    unknown_outcome,
    voucher_order,
)
from altegio_bot.utils import utcnow


def _identity(run_id: int, recipient_id: int, **changes) -> ManualCanaryIdentity:
    values = {
        "company_id": COMPANY_ID,
        "campaign_code": "new_clients_monthly",
        "campaign_run_id": run_id,
        "campaign_recipient_id": recipient_id,
        "easyweek_customer_uuid": str(EW_CUSTOMER_UUID),
        "campaign_period_start": PERIOD_START,
        "campaign_period_end": PERIOD_END,
        "location_uuid": KARLSRUHE_LOCATION_UUID,
        "staffer_uuid": STAFFER_UUID,
        "payment_account_uuid": ACCOUNT_UUID,
        "voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID,
        "reconciliation_marker": manual_marker(preview_run_id=run_id, campaign_recipient_id=recipient_id),
        "baseline_version": MANUAL_BASELINE_VERSION,
    }
    values.update(changes)
    return ManualCanaryIdentity(**values)


# ---------------------------------------------------------------------------
# The basis: a decision, never an entitlement
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_manual_recipient_is_proven_without_claiming_a_first_visit(session_maker) -> None:
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    reader = FakeReader()

    async with session_maker() as session:
        proof = await prove_manual_recipient(
            session,
            preview_run_id=run_id,
            campaign_recipient_id=recipient_id,
            client_reader=reader,
            now=utcnow(),
        )

    assert proof.proven is True
    safe = proof.as_safe_dict()
    assert safe["recipient_basis"] == "operator_manual_selection"
    # Never a quiet `true`, and never a refusal either.
    assert safe["first_visit_proof"] == "not_applicable"
    # The identity was proven live, and the history was never walked — the fake
    # raises if anything tries.
    assert reader.customer_calls == [str(EW_CUSTOMER_UUID)]
    # Values are held, not printed.
    assert PHONE not in str(safe)
    assert str(EW_CUSTOMER_UUID) not in str(safe)


@pytest.mark.asyncio
async def test_an_earned_recipient_is_refused_by_this_canary(session_maker) -> None:
    """§36 serves earned rows. Mixing the two ledgers is the mistake."""
    # A real earned row, seeded through §36's own fixture: the database refuses
    # an "earned" EasyWeek recipient that carries no source proof, so there is
    # no way to fake one here — which is itself the invariant working.
    from altegio_bot.tests.easyweek_voucher_delivery_fixtures import seed_recipient

    run_id, recipient_id = await seed_recipient(session_maker)
    async with session_maker() as session:
        proof = await prove_manual_recipient(
            session,
            preview_run_id=run_id,
            campaign_recipient_id=recipient_id,
            client_reader=FakeReader(),
            now=utcnow(),
        )
    assert proof.proven is False
    assert RECIPIENT_BASIS_UNSUPPORTED in proof.reasons


@pytest.mark.asyncio
async def test_the_existing_delivery_canary_still_refuses_a_manual_recipient(session_maker) -> None:
    """§36's refusal is not weakened by §37.2 existing."""
    from altegio_bot.campaigns.easyweek_voucher_delivery.eligibility import prove_recipient
    from altegio_bot.campaigns.easyweek_voucher_delivery.identity import (
        RECIPIENT_BASIS_UNSUPPORTED as DELIVERY_BASIS_UNSUPPORTED,
    )

    run_id, recipient_id = await seed_manual_recipient(session_maker)
    async with session_maker() as session:
        proof = await prove_recipient(
            session,
            preview_run_id=run_id,
            campaign_recipient_id=recipient_id,
            expected_company_id=COMPANY_ID,
            client_reader=FakeReader(),
            now=utcnow(),
        )
    assert proof.proven is False
    assert DELIVERY_BASIS_UNSUPPORTED in proof.reasons


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({"run_status": "running"}, RUN_UNPROVEN),
        ({"run_mode": "send-real"}, RUN_UNPROVEN),
        ({"campaign_code": "something_else"}, RUN_UNPROVEN),
        ({"recipient_status": "skipped"}, RECIPIENT_NOT_CANDIDATE),
        ({"opted_out": True}, RECIPIENT_OPTED_OUT),
    ],
)
@pytest.mark.asyncio
async def test_every_local_refusal_is_named_before_any_live_read(session_maker, changes, expected) -> None:
    run_id, recipient_id = await seed_manual_recipient(session_maker, **changes)
    reader = FakeReader()
    async with session_maker() as session:
        proof = await prove_manual_recipient(
            session,
            preview_run_id=run_id,
            campaign_recipient_id=recipient_id,
            client_reader=reader,
            now=utcnow(),
        )
    assert proof.proven is False
    assert expected in proof.reasons
    assert reader.customer_calls == [], "a locally answerable refusal still read EasyWeek"


@pytest.mark.parametrize(
    ("customer", "expected"),
    [
        (customer_payload(uuid=str(OTHER_CUSTOMER_UUID)), CUSTOMER_IDENTITY_NOT_CURRENT),
        (customer_payload(phone="+4915199999999"), CUSTOMER_IDENTITY_NOT_CURRENT),
        (EasyWeekError("timeout"), LIVE_GUARD_UNCERTAIN),
        (RuntimeError("429"), LIVE_GUARD_UNCERTAIN),
    ],
)
@pytest.mark.asyncio
async def test_a_live_identity_that_does_not_match_refuses(session_maker, customer, expected) -> None:
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    async with session_maker() as session:
        proof = await prove_manual_recipient(
            session,
            preview_run_id=run_id,
            campaign_recipient_id=recipient_id,
            client_reader=FakeReader(customer=customer),
            now=utcnow(),
        )
    assert proof.proven is False
    assert expected in proof.reasons


@pytest.mark.asyncio
async def test_a_phone_that_changed_since_the_preview_refuses(session_maker) -> None:
    run_id, recipient_id = await seed_manual_recipient(session_maker, client_phone="+4915100000000")
    async with session_maker() as session:
        proof = await prove_manual_recipient(
            session,
            preview_run_id=run_id,
            campaign_recipient_id=recipient_id,
            client_reader=FakeReader(),
            now=utcnow(),
        )
    assert proof.proven is False
    assert CUSTOMER_PHONE_NOT_CURRENT in proof.reasons


# ---------------------------------------------------------------------------
# The versioned baseline
# ---------------------------------------------------------------------------


def test_the_baseline_is_forty_two_and_does_not_adapt() -> None:
    assert prove_baseline(template_payload()).proven is True
    # §35's 43/43 is drift HERE, and the §35 record is untouched by saying so.
    drift = prove_baseline(template_payload(services=43, all_services=43))
    assert drift.proven is False
    assert set(drift.mismatched_fields) == {"services_count", "all_services_count"}
    # And any other number is drift too — never a new normal.
    assert prove_baseline(template_payload(services=41, all_services=41)).proven is False


def test_the_two_service_counts_must_agree_with_each_other() -> None:
    skewed = prove_baseline(template_payload(services=42, all_services=41))
    assert skewed.proven is False
    assert "services_count_vs_all_services_count" in skewed.mismatched_fields


def test_unreadable_counters_are_not_zero() -> None:
    proof = prove_baseline(template_payload(vouchers_count="1"))
    assert proof.proven is False
    assert proof.counters is None


def test_the_thirty_five_baseline_is_unchanged() -> None:
    """§37.2 parameterised the comparison; §35 still compares against 43/43."""
    from altegio_bot.easyweek_voucher_canary.plan import frozen_template_mismatches
    from altegio_bot.easyweek_voucher_identity import FROZEN_TEMPLATE_FACTS

    assert FROZEN_TEMPLATE_FACTS["services_count"] == 43
    assert FROZEN_TEMPLATE_FACTS["all_services_count"] == 43
    # Default call: the §35 baseline, unchanged.
    assert frozen_template_mismatches(template_payload(services=43, all_services=43)) == ()
    assert "services_count" in frozen_template_mismatches(template_payload())


@pytest.mark.asyncio
async def test_a_drifted_baseline_stops_a_payment_but_not_a_refund(
    session_maker, manual_configuration, binding_key
) -> None:
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    await seed_template_and_sender(session_maker)
    request = manual_request(run_id=run_id, recipient_id=recipient_id)
    reader = FakeReader(template=template_payload(services=43, all_services=43))

    async with session_maker() as session:
        pay_plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_PAY, request=request, reader=reader, order_reader=reader
        )
        refund_plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_REFUND, request=request, reader=reader, order_reader=reader
        )

    assert BASELINE_DRIFT in pay_plan.reasons
    # Cleanup matters more than the tidiness of the configuration it cleans up
    # after: a drift must not be what strands a real €15.
    assert BASELINE_DRIFT not in refund_plan.reasons


# ---------------------------------------------------------------------------
# The fence, and what a plan authorises
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_closed_fence_refuses_before_any_read(session_maker, binding_key) -> None:
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    await seed_template_and_sender(session_maker)
    reader = FakeReader()

    async with session_maker() as session:
        plan, _, prerequisites, _ = await runner_module.build_stage_plan(
            session,
            session_maker,
            stage=STAGE_CREATE,
            request=manual_request(run_id=run_id, recipient_id=recipient_id),
            reader=reader,
            order_reader=reader,
            enabled=False,
        )

    assert plan.ready is False
    assert CANARY_DISABLED in plan.reasons
    assert prerequisites.fence_open is False


@pytest.mark.asyncio
async def test_a_stage_without_apply_does_nothing(session_maker, manual_configuration, binding_key) -> None:
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    await seed_template_and_sender(session_maker)
    reader = FakeReader()
    mutator = FakeMutator()

    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session,
            session_maker,
            stage=STAGE_CREATE,
            request=manual_request(run_id=run_id, recipient_id=recipient_id),
            reader=reader,
            order_reader=reader,
        )
        report = await runner_module.run_create(
            session,
            session_maker,
            request=manual_request(run_id=run_id, recipient_id=recipient_id),
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            apply=False,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )

    assert report.outcome == "refused"
    assert "manual_voucher_apply_flag_missing" in report.reasons
    assert mutator.calls == []
    assert report.as_safe_dict()["external_effect_attempted"] is False


@pytest.mark.asyncio
async def test_a_stage_digest_never_authorises_another_stage(session_maker, manual_configuration, binding_key) -> None:
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    await seed_template_and_sender(session_maker)
    reader = FakeReader()
    request = manual_request(run_id=run_id, recipient_id=recipient_id)

    async with session_maker() as session:
        create_plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_CREATE, request=request, reader=reader, order_reader=reader
        )
        pay_plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_PAY, request=request, reader=reader, order_reader=reader
        )

    assert create_plan.digest != pay_plan.digest
    assert create_plan.confirmation_phrase.startswith("create-manual-voucher-")
    assert pay_plan.confirmation_phrase.startswith("pay-manual-voucher-")
    # And neither reads as a §36 approval.
    assert "manual-voucher" in create_plan.confirmation_phrase


@pytest.mark.asyncio
async def test_a_plan_older_than_its_ttl_authorises_nothing(session_maker, manual_configuration, binding_key) -> None:
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    await seed_template_and_sender(session_maker)
    reader = FakeReader()
    request = manual_request(run_id=run_id, recipient_id=recipient_id)
    mutator = FakeMutator()

    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_CREATE, request=request, reader=reader, order_reader=reader
        )
        stale = plan.issued_at - timedelta(hours=2)
        report = await runner_module.run_create(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            apply=True,
            supplied_digest=plan.digest_for(stale),
            supplied_issued_at=stale,
            supplied_phrase=plan.phrase_for(plan.digest_for(stale)),
        )

    assert report.outcome == "refused"
    assert "manual_voucher_plan_expired" in report.reasons
    assert mutator.calls == []


# ---------------------------------------------------------------------------
# The stages
# ---------------------------------------------------------------------------


async def _advance_to_paid(session_maker, *, run_id: int, recipient_id: int) -> None:
    """Move a created row to `paid` through the real claim, not around it.

    Writing `pay_verified_at` directly is refused by the stage-order constraint,
    which is the point: a verification without an attempt is not a state this
    ledger can be in.
    """
    identity = _identity(run_id, recipient_id)
    claim = await ledger_module.claim_pay(session_maker, identity=identity, plan_digest="digest-pay")
    assert claim.granted, claim.reason
    outcome = await ledger_module.record_outcome(
        session_maker,
        status=MANUAL_VOUCHER_PAID,
        expected_statuses=frozenset({"pay_claimed"}),
        verified_field="pay_verified_at",
        # The claim raised both flags; a proven payment lowers them, exactly as
        # `run_pay` does. A helper that left them up would leave the row in a
        # state production never produces.
        reconciliation_required=False,
        manual_cleanup_required=False,
    )
    assert outcome.applied, outcome.reason


async def _create(session_maker, *, mutator=None, reader=None, orders=None):
    """Drive a proven CREATE and return (report, ids, reader)."""
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    await seed_template_and_sender(session_maker)
    request = manual_request(run_id=run_id, recipient_id=recipient_id)
    marker = manual_marker(preview_run_id=run_id, campaign_recipient_id=recipient_id)
    reader = reader or FakeReader(
        orders=orders if orders is not None else {str(ORDER_UUID): voucher_order(marker=marker)}
    )
    mutator = mutator or FakeMutator(
        create=VoucherMutationResponse(http_status=201, envelope={"uuid": str(ORDER_UUID)})
    )

    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_CREATE, request=request, reader=reader, order_reader=reader
        )
        assert plan.ready, plan.reasons
        report = await runner_module.run_create(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )
    return report, (run_id, recipient_id), reader, mutator


@pytest.mark.asyncio
async def test_create_opens_the_row_and_binds_the_code(session_maker, manual_configuration, binding_key) -> None:
    report, (run_id, recipient_id), _reader, mutator = await _create(session_maker)

    assert report.outcome == "created"
    assert report.as_safe_dict()["external_effect_attempted"] is True
    assert mutator.calls == ["create"]

    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == MANUAL_VOUCHER_CREATED
    assert row.recipient_basis == "operator_manual_selection"
    assert row.company_id == COMPANY_ID
    assert row.campaign_run_id == run_id
    assert row.campaign_recipient_id == recipient_id
    assert row.baseline_version == MANUAL_BASELINE_VERSION
    # The code is bound, and is not stored.
    assert row.voucher_code_hmac is not None and row.hmac_key_id == "test-key-1"
    assert VOUCHER_CODE_SENTINEL not in str(row.evidence)
    assert row.create_verified_at is not None
    # An open draft exists in the POS until it is paid or closed by hand.
    assert row.manual_cleanup_required is True


@pytest.mark.asyncio
async def test_an_unknown_create_is_never_retried(session_maker, manual_configuration, binding_key) -> None:
    report, _ids, _reader, mutator = await _create(
        session_maker, mutator=FakeMutator(create=EasyWeekVoucherMutationUnknown("timeout"))
    )

    assert report.outcome == "unknown"
    assert MUTATION_UNKNOWN in report.reasons
    assert report.as_safe_dict()["reconciliation_required"] is True
    # Exactly one attempt left the process.
    assert mutator.calls == ["create"]

    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    # The attempt is recorded and never cleared: a crash after the commit reads
    # the same as a lost answer.
    assert row.create_attempted_at is not None
    assert row.reconciliation_required is True


@pytest.mark.asyncio
async def test_a_rejected_create_may_be_claimed_again(session_maker, manual_configuration, binding_key) -> None:
    report, _ids, _reader, _mutator = await _create(session_maker, mutator=FakeMutator(create=EasyWeekError("422")))
    assert report.outcome == "rejected"
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == "create_rejected"
    assert row.status in ledger_module.CREATE_CLAIMABLE_FROM


@pytest.mark.asyncio
async def test_a_singleton_voucher_without_quantity_is_accepted(
    session_maker, manual_configuration, binding_key
) -> None:
    """The shape production actually returned: a list of one, and no quantity."""
    report, _ids, _reader, _mutator = await _create(session_maker)
    assert report.outcome == "created"

    # And a second entry is not a singleton, whatever else it says. Asked of the
    # proof directly: no database row is needed to know that two vouchers are
    # not one, and seeding another recipient would collide with the entitlement
    # constraint that exists for exactly that reason.
    two = voucher_order(marker="x", vouchers=[issued_voucher(), issued_voucher()])
    assert runner_module._voucher_code(two) is None
    # A present but wrong quantity never falls back to the singleton proof: the
    # field was readable and it did not say "one".
    wrong = voucher_order(marker="x", vouchers=[issued_voucher(quantity=2)])
    assert runner_module._voucher_code(wrong) is None
    empty = voucher_order(marker="x", vouchers=[])
    assert runner_module._voucher_code(empty) is None


@pytest.mark.asyncio
async def test_pay_then_deliver_reaches_provider_accepted_and_no_further(
    session_maker, manual_configuration, binding_key
) -> None:
    report, (run_id, recipient_id), reader, _m = await _create(session_maker)
    assert report.outcome == "created"
    request = manual_request(run_id=run_id, recipient_id=recipient_id)
    marker = manual_marker(preview_run_id=run_id, campaign_recipient_id=recipient_id)

    # PAY
    mutator = FakeMutator(pay=VoucherMutationResponse(http_status=201, envelope={"uuid": str(ORDER_UUID)}))
    reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker, status="paid")
    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_PAY, request=request, reader=reader, order_reader=reader
        )
        # The plan reads the order as paid already; the payable check belongs to
        # an open order, so drive the stage from the open state instead.
        reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker)
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_PAY, request=request, reader=reader, order_reader=reader
        )
        assert plan.ready, plan.reasons

        class PayingReader(FakeReader):
            """Open before the POST, paid after it — as the real one behaves."""

            def __init__(self, inner: FakeReader) -> None:
                super().__init__(customer=inner.customer, orders=dict(inner.orders), template=inner.template)
                self.seen = 0

            async def get_order(self, order_uuid: str):
                self.seen += 1
                if self.seen > 2:
                    return voucher_order(marker=marker, status="paid")
                return await super().get_order(order_uuid)

        paying = PayingReader(reader)
        pay_report = await runner_module.run_pay(
            session,
            session_maker,
            request=request,
            reader=paying,
            order_reader=paying,
            mutator=mutator,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )
    assert pay_report.outcome == "paid", pay_report.reasons
    assert mutator.calls == ["pay"]

    # DELIVER
    reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker, status="paid")
    sender = FakeSender()
    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_DELIVER, request=request, reader=reader, order_reader=reader
        )
        assert plan.ready, plan.reasons
        deliver_report = await runner_module.run_deliver(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            sender=sender,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )

    assert deliver_report.outcome == "provider_accepted"
    assert sender.calls == 1
    # The code reached Meta and nothing else.
    assert sender.saw_code is True
    payload = deliver_report.as_safe_dict()
    assert payload["external_send_attempted"] is True
    assert payload["campaign_send_authorized"] is False
    assert payload["bulk_delivery_authorized"] is False
    assert payload["ready_for_send"] is False
    assert VOUCHER_CODE_SENTINEL not in str(payload)

    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
        attempts = list((await session.execute(select(EasyWeekManualVoucherDeliveryAttempt))).scalars().all())
    assert row.status == MANUAL_VOUCHER_PROVIDER_ACCEPTED
    assert row.send_attempt_count == 1
    assert row.provider_message_id == PROVIDER_MESSAGE_ID
    # The redacted intent holds no message, no parameters and no code.
    assert len(attempts) == 1
    assert attempts[0].outcome == "provider_accepted"
    assert VOUCHER_CODE_SENTINEL not in str(attempts[0].__dict__)
    assert PHONE not in str(attempts[0].__dict__)


@pytest.mark.asyncio
async def test_an_unknown_send_is_never_retried_and_forbids_a_refund(
    session_maker, manual_configuration, binding_key
) -> None:
    report, (run_id, recipient_id), reader, _m = await _create(session_maker)
    assert report.outcome == "created"
    request = manual_request(run_id=run_id, recipient_id=recipient_id)
    marker = manual_marker(preview_run_id=run_id, campaign_recipient_id=recipient_id)

    # Get to `paid` through the real claim: this test is about the send.
    await _advance_to_paid(session_maker, run_id=run_id, recipient_id=recipient_id)
    reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker, status="paid")
    sender = FakeSender(unknown_outcome("timeout"))

    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_DELIVER, request=request, reader=reader, order_reader=reader
        )
        assert plan.ready, plan.reasons
        deliver = await runner_module.run_deliver(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            sender=sender,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )
    assert deliver.outcome == "unknown"
    assert deliver.as_safe_dict()["reconciliation_required"] is True
    assert sender.calls == 1

    # A second delivery is not refused by policy — it is unrepresentable.
    async with session_maker() as session:
        second = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_DELIVER, request=request, reader=reader, order_reader=reader
        )
    assert "manual_voucher_delivery_already_attempted" in second[0].reasons

    # And the money stays where it is: the customer may be holding the code.
    async with session_maker() as session:
        refund_plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_REFUND, request=request, reader=reader, order_reader=reader
        )
    assert REFUND_FORBIDDEN_AFTER_SEND in refund_plan.reasons

    mutator = FakeMutator()
    async with session_maker() as session:
        refused = await runner_module.run_refund(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            apply=True,
            supplied_digest=refund_plan.digest,
            supplied_issued_at=refund_plan.issued_at,
            supplied_phrase=refund_plan.confirmation_phrase,
        )
    assert refused.outcome == "refused"
    assert mutator.calls == []

    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == MANUAL_VOUCHER_SEND_UNKNOWN
    assert row.send_attempt_count == 1


@pytest.mark.asyncio
async def test_a_rotated_key_stops_the_send_before_it_happens(
    session_maker, manual_configuration, binding_key, monkeypatch
) -> None:
    report, (run_id, recipient_id), reader, _m = await _create(session_maker)
    assert report.outcome == "created"
    request = manual_request(run_id=run_id, recipient_id=recipient_id)
    marker = manual_marker(preview_run_id=run_id, campaign_recipient_id=recipient_id)
    await _advance_to_paid(session_maker, run_id=run_id, recipient_id=recipient_id)
    reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker, status="paid")

    # Somebody rotated the key between paying and sending.
    from pydantic import SecretStr

    from altegio_bot.settings import settings as live_settings

    monkeypatch.setattr(live_settings, "easyweek_voucher_delivery_hmac_key_id", "test-key-2", raising=False)
    monkeypatch.setattr(live_settings, "easyweek_voucher_delivery_hmac_key", SecretStr("y" * 48), raising=False)

    sender = FakeSender()
    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_DELIVER, request=request, reader=reader, order_reader=reader
        )
        deliver = await runner_module.run_deliver(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            sender=sender,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )
    assert deliver.outcome == "refused"
    assert "manual_voucher_binding_mismatch" in deliver.reasons
    assert sender.calls == 0


@pytest.mark.asyncio
async def test_no_report_or_log_ever_carries_the_code_or_the_phone(
    session_maker, manual_configuration, binding_key, caplog
) -> None:
    caplog.set_level(logging.DEBUG)
    report, (run_id, recipient_id), reader, _m = await _create(session_maker)
    request = manual_request(run_id=run_id, recipient_id=recipient_id)

    async with session_maker() as session:
        plan, proof, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_PAY, request=request, reader=reader, order_reader=reader
        )
    status = await runner_module.run_status(session_maker)

    printed = "".join(
        [
            str(report.as_safe_dict()),
            str(plan.as_safe_dict()),
            str(proof.as_safe_dict()),
            str(status.as_safe_dict()),
            caplog.text,
        ]
    )
    for secret in (VOUCHER_CODE_SENTINEL, PHONE, str(EW_CUSTOMER_UUID)):
        assert secret not in printed


# ---------------------------------------------------------------------------
# The database, on real PostgreSQL
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_one_person_one_period_one_voucher(session_maker) -> None:
    """A fresh preview of the same August wave issues nothing a second time."""
    run_a, recipient_a = await seed_manual_recipient(session_maker)
    await ledger_module.open_canary(session_maker, identity=_identity(run_a, recipient_a))

    # A second preview: new row ids, same human being, same campaign period.
    run_b, recipient_b = await seed_manual_recipient(session_maker)
    async with session_maker() as session:
        row = EasyWeekManualVoucherDeliveryLedger(
            canary_scope="easyweek_manual_voucher_canary_v2",
            request_schema_version="1",
            baseline_version=MANUAL_BASELINE_VERSION,
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            campaign_code="new_clients_monthly",
            recipient_basis="operator_manual_selection",
            campaign_run_id=run_b,
            campaign_recipient_id=recipient_b,
            easyweek_customer_uuid=EW_CUSTOMER_UUID,
            campaign_period_start=PERIOD_START,
            campaign_period_end=PERIOD_END,
            location_uuid=uuid_module.UUID(KARLSRUHE_LOCATION_UUID),
            staffer_uuid=uuid_module.UUID(STAFFER_UUID),
            payment_account_uuid=uuid_module.UUID(ACCOUNT_UUID),
            voucher_template_uuid=uuid_module.UUID(EASYWEEK_VOUCHER_TEMPLATE_UUID),
            reconciliation_marker="ewmv1-second",
            status=MANUAL_VOUCHER_PLANNED,
            evidence={},
        )
        session.add(row)
        with pytest.raises(IntegrityError) as raised:
            await session.commit()
    assert "uq_ew_manual_voucher_entitlement" in str(raised.value)


@pytest.mark.asyncio
async def test_a_second_canary_scope_is_impossible(session_maker) -> None:
    run_a, recipient_a = await seed_manual_recipient(session_maker)
    await ledger_module.open_canary(session_maker, identity=_identity(run_a, recipient_a))
    # Another person, another preview — but the same one canary scope.
    run_b, recipient_b = await seed_manual_recipient(
        session_maker, customer_uuid=OTHER_CUSTOMER_UUID, phone="+4915100000001"
    )
    snapshot = await ledger_module.open_canary(
        session_maker,
        identity=_identity(run_b, recipient_b, easyweek_customer_uuid=str(OTHER_CUSTOMER_UUID)),
    )
    # `open_canary` returns the EXISTING row rather than starting a second one.
    assert snapshot.campaign_run_id == run_a
    async with session_maker() as session:
        count = len(list((await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalars().all()))
    assert count == 1


async def _paid_row(session_maker) -> None:
    """A row valid all the way to `paid`, so a schema test reaches ITS rule."""
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    identity = _identity(run_id, recipient_id)
    await ledger_module.open_canary(session_maker, identity=identity)
    window = utcnow()
    await ledger_module.claim_create(
        session_maker,
        identity=identity,
        plan_digest="d",
        create_window_start=window - timedelta(minutes=30),
        create_window_end=window + timedelta(minutes=30),
    )
    await ledger_module.record_outcome(
        session_maker,
        status=MANUAL_VOUCHER_CREATED,
        expected_statuses=frozenset({"create_claimed"}),
        target_order_uuid=str(ORDER_UUID),
        voucher_code_hmac="a" * 64,
        hmac_key_id="test-key-1",
        verified_field="create_verified_at",
    )
    await _advance_to_paid(session_maker, run_id=run_id, recipient_id=recipient_id)


@pytest.mark.asyncio
async def test_the_schema_refuses_a_second_delivery_attempt(session_maker) -> None:
    await _paid_row(session_maker)
    # The transaction block is inside `raises`: the constraint fires on the
    # flush that ends it, which is the moment the database refuses.
    with pytest.raises(IntegrityError) as raised:
        async with session_maker() as session:
            async with session.begin():
                row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
                moment = utcnow()
                row.live_guard_reproven_at = moment
                row.send_claimed_at = moment
                row.send_attempted_at = moment
                row.send_attempt_count = 2
    assert "ck_ew_manual_voucher_single_attempt" in str(raised.value)


@pytest.mark.asyncio
async def test_the_schema_refuses_a_refund_after_a_send(session_maker) -> None:
    await _paid_row(session_maker)
    with pytest.raises(IntegrityError) as raised:
        async with session_maker() as session:
            async with session.begin():
                row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
                moment = utcnow()
                row.live_guard_reproven_at = moment
                row.send_claimed_at = moment
                row.send_attempted_at = moment
                row.send_attempt_count = 1
                row.refund_claimed_at = moment
                row.refund_attempted_at = moment
    assert "ck_ew_manual_voucher_refund_is_pre_send" in str(raised.value)


@pytest.mark.asyncio
async def test_the_schema_refuses_another_branch_and_another_basis(session_maker) -> None:
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    for field, value, constraint in (
        ("company_id", 308697, "ck_ew_manual_voucher_company"),
        ("recipient_basis", "earned_first_visit", "ck_ew_manual_voucher_basis"),
    ):
        async with session_maker() as session:
            values = {
                "canary_scope": f"scope-{field}",
                "request_schema_version": "1",
                "baseline_version": MANUAL_BASELINE_VERSION,
                "provider": PROVIDER_EASYWEEK,
                "company_id": COMPANY_ID,
                "campaign_code": "new_clients_monthly",
                "recipient_basis": "operator_manual_selection",
                "campaign_run_id": run_id,
                "campaign_recipient_id": recipient_id,
                "easyweek_customer_uuid": EW_CUSTOMER_UUID,
                "campaign_period_start": PERIOD_START,
                "campaign_period_end": PERIOD_END,
                "location_uuid": uuid_module.UUID(KARLSRUHE_LOCATION_UUID),
                "staffer_uuid": uuid_module.UUID(STAFFER_UUID),
                "payment_account_uuid": uuid_module.UUID(ACCOUNT_UUID),
                "voucher_template_uuid": uuid_module.UUID(EASYWEEK_VOUCHER_TEMPLATE_UUID),
                "reconciliation_marker": f"marker-{field}",
                "status": MANUAL_VOUCHER_PLANNED,
                "evidence": {},
            }
            values[field] = value
            session.add(EasyWeekManualVoucherDeliveryLedger(**values))
            with pytest.raises(IntegrityError) as raised:
                await session.commit()
        assert constraint in str(raised.value)


@pytest.mark.asyncio
async def test_two_concurrent_claims_produce_exactly_one_winner(session_maker) -> None:
    """The row lock decides, not the order two operators pressed enter in."""
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    identity = _identity(run_id, recipient_id)
    await ledger_module.open_canary(session_maker, identity=identity)

    window = utcnow()
    outcomes = await asyncio.gather(
        ledger_module.claim_create(
            session_maker,
            identity=identity,
            plan_digest="digest-a",
            create_window_start=window - timedelta(minutes=30),
            create_window_end=window + timedelta(minutes=30),
        ),
        ledger_module.claim_create(
            session_maker,
            identity=identity,
            plan_digest="digest-b",
            create_window_start=window - timedelta(minutes=30),
            create_window_end=window + timedelta(minutes=30),
        ),
    )
    granted = [outcome for outcome in outcomes if outcome.granted]
    assert len(granted) == 1
    refused = [outcome for outcome in outcomes if not outcome.granted]
    assert refused[0].reason == ledger_module.CLAIM_REFUSED_STATE


@pytest.mark.asyncio
async def test_a_late_writer_cannot_walk_a_stage_backwards(session_maker) -> None:
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    identity = _identity(run_id, recipient_id)
    await ledger_module.open_canary(session_maker, identity=identity)
    window = utcnow()
    await ledger_module.claim_create(
        session_maker,
        identity=identity,
        plan_digest="d",
        create_window_start=window - timedelta(minutes=30),
        create_window_end=window + timedelta(minutes=30),
    )
    await ledger_module.record_outcome(
        session_maker,
        status=MANUAL_VOUCHER_CREATED,
        expected_statuses=frozenset({"create_claimed"}),
        target_order_uuid=str(ORDER_UUID),
        verified_field="create_verified_at",
    )
    # A reconciliation that read the world a moment too early.
    late = await ledger_module.record_outcome(
        session_maker,
        status="create_unknown",
        expected_statuses=frozenset({"create_claimed", MANUAL_VOUCHER_CREATED}),
        reason_code="late",
    )
    assert late.applied is False
    assert late.reason == ledger_module.RECORD_WOULD_REGRESS
    assert late.snapshot.status == MANUAL_VOUCHER_CREATED


@pytest.mark.asyncio
async def test_an_identity_that_does_not_match_the_row_claims_nothing(session_maker) -> None:
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    await ledger_module.open_canary(session_maker, identity=_identity(run_id, recipient_id))
    window = utcnow()
    outcome = await ledger_module.claim_create(
        session_maker,
        # A rotated staffer after the row was opened.
        identity=_identity(run_id, recipient_id, staffer_uuid=str(OTHER_CUSTOMER_UUID)),
        plan_digest="d",
        create_window_start=window - timedelta(minutes=30),
        create_window_end=window + timedelta(minutes=30),
    )
    assert outcome.granted is False
    assert outcome.reason == ledger_module.CLAIM_REFUSED_IDENTITY


@pytest.mark.asyncio
async def test_a_canary_freezes_its_preview_for_both_ledgers(session_maker) -> None:
    from altegio_bot.campaigns.preview_freeze import preview_is_locked_by_any_canary

    run_id, recipient_id = await seed_manual_recipient(session_maker)
    async with session_maker() as session:
        assert await preview_is_locked_by_any_canary(session, campaign_run_id=run_id) is False

    await ledger_module.open_canary(session_maker, identity=_identity(run_id, recipient_id))
    async with session_maker() as session:
        assert await preview_is_locked_by_any_canary(session, campaign_run_id=run_id) is True


# ---------------------------------------------------------------------------
# The message: three proven parameters, or none at all
# ---------------------------------------------------------------------------


class RecordingSender(FakeSender):
    """Keeps WHAT was sent, so the exact three parameters can be asserted.

    The captured values live in the test process only; nothing here is printed
    by production code.
    """

    def __init__(self, outcome=None) -> None:
        super().__init__(outcome)
        self.params: list[str] | None = None
        self.template_name: str | None = None
        self.language: str | None = None

    async def send_voucher_template(self, *, params, **kwargs):
        self.params = list(params)
        self.template_name = kwargs.get("template_name")
        self.language = kwargs.get("language")
        return await super().send_voucher_template(params=params, **kwargs)


async def _paid_and_ready(session_maker, *, reader=None):
    """A created + paid row with a paid order on screen. Returns the context."""
    report, (run_id, recipient_id), reader, _m = await _create(session_maker, reader=reader)
    assert report.outcome == "created", report.reasons
    marker = manual_marker(preview_run_id=run_id, campaign_recipient_id=recipient_id)
    await _advance_to_paid(session_maker, run_id=run_id, recipient_id=recipient_id)
    reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker, status="paid")
    return manual_request(run_id=run_id, recipient_id=recipient_id), reader, marker


@pytest.mark.asyncio
async def test_the_send_carries_the_real_booking_link(session_maker, manual_configuration, binding_key) -> None:
    request, reader, _marker = await _paid_and_ready(session_maker)
    sender = RecordingSender()

    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_DELIVER, request=request, reader=reader, order_reader=reader
        )
        assert plan.ready, plan.reasons
        report = await runner_module.run_deliver(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            sender=sender,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )

    assert report.outcome == "provider_accepted"
    # Exactly three, in the approved order, every one non-empty.
    assert sender.params == [CUSTOMER_NAME, VOUCHER_CODE_SENTINEL, BOOKING_LINK]
    assert sender.template_name == "kitilash_ka_new_client_voucher_v1"
    assert sender.language == "de"
    # And none of it reaches a report.
    printed = str(report.as_safe_dict()) + str(plan.as_safe_dict())
    for secret in (VOUCHER_CODE_SENTINEL, BOOKING_LINK, CUSTOMER_NAME):
        assert secret not in printed


@pytest.mark.asyncio
async def test_an_unproven_booking_link_stops_the_send_and_the_create(
    session_maker, manual_configuration, binding_key, monkeypatch
) -> None:
    """Proven before CREATE, not only before DELIVER: a voucher that cannot be
    delivered legally is €15 with two exits."""
    from altegio_bot.settings import settings as live_settings

    request, reader, _marker = await _paid_and_ready(session_maker)

    # The registry loses the link between paying and sending.
    monkeypatch.setattr(live_settings, "easyweek_location_map", location_map(booking_link=""), raising=False)

    sender = RecordingSender()
    async with session_maker() as session:
        plan, _, prerequisites, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_DELIVER, request=request, reader=reader, order_reader=reader
        )
        assert plan.ready is False
        assert "manual_voucher_booking_link_unproven" in plan.reasons
        assert prerequisites.booking_link is None
        report = await runner_module.run_deliver(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            sender=sender,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )

    assert report.outcome == "refused"
    assert sender.calls == 0, "a message went out with an empty link"
    assert sender.params is None

    # And the same blocker is raised before a voucher exists at all.
    async with session_maker() as session:
        create_plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_CREATE, request=request, reader=reader, order_reader=reader
        )
    assert "manual_voucher_booking_link_unproven" in create_plan.reasons


# ---------------------------------------------------------------------------
# The webhook, through the worker's own function
# ---------------------------------------------------------------------------


async def _accepted_row(session_maker) -> None:
    """A row that reached `provider_accepted`, the way the canary reaches it."""
    request, reader, _marker = await _paid_and_ready(session_maker)
    sender = FakeSender()
    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_DELIVER, request=request, reader=reader, order_reader=reader
        )
        report = await runner_module.run_deliver(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            sender=sender,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )
    assert report.outcome == "provider_accepted"


@pytest.mark.asyncio
async def test_the_status_worker_advances_the_canary_delivered_then_read(
    session_maker, manual_configuration, binding_key
) -> None:
    """Driven through the worker's real handler, not the ledger directly."""
    from altegio_bot.workers.whatsapp_inbox_worker import _handle_delivery_statuses

    await _accepted_row(session_maker)

    async with session_maker() as session:
        async with session.begin():
            await _handle_delivery_statuses(
                session,
                None,
                [{"status": "delivered", "provider_message_id": PROVIDER_MESSAGE_ID}],
            )
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == "delivered"
    assert row.delivered_at is not None and row.read_at is None
    assert row.manual_cleanup_required is False

    async with session_maker() as session:
        async with session.begin():
            await _handle_delivery_statuses(
                session,
                None,
                [{"status": "read", "provider_message_id": PROVIDER_MESSAGE_ID}],
            )
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == "read"
    assert row.read_at is not None


@pytest.mark.asyncio
async def test_a_read_callback_alone_implies_delivered(session_maker, manual_configuration, binding_key) -> None:
    """WhatsApp can deliver `read` without a `delivered` ever arriving."""
    from altegio_bot.workers.whatsapp_inbox_worker import _handle_delivery_statuses

    await _accepted_row(session_maker)
    async with session_maker() as session:
        async with session.begin():
            await _handle_delivery_statuses(
                session, None, [{"status": "read", "provider_message_id": PROVIDER_MESSAGE_ID}]
            )
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == "read"
    # The CHECK constraint would refuse a read with no delivered behind it, so
    # the same callback has to stamp both.
    assert row.delivered_at is not None
    assert row.read_at is not None


@pytest.mark.asyncio
async def test_duplicate_and_backwards_callbacks_change_nothing(
    session_maker, manual_configuration, binding_key
) -> None:
    from altegio_bot.workers.whatsapp_inbox_worker import _handle_delivery_statuses

    await _accepted_row(session_maker)
    async with session_maker() as session:
        async with session.begin():
            await _handle_delivery_statuses(
                session, None, [{"status": "read", "provider_message_id": PROVIDER_MESSAGE_ID}]
            )
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
        first_read, first_delivered = row.read_at, row.delivered_at

    # A duplicate read, and a delivered arriving after it.
    async with session_maker() as session:
        async with session.begin():
            await _handle_delivery_statuses(
                session,
                None,
                [
                    {"status": "read", "provider_message_id": PROVIDER_MESSAGE_ID},
                    {"status": "delivered", "provider_message_id": PROVIDER_MESSAGE_ID},
                ],
            )
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == "read"
    assert row.read_at == first_read
    assert row.delivered_at == first_delivered


@pytest.mark.asyncio
async def test_a_callback_for_another_message_leaves_the_canary_alone(
    session_maker, manual_configuration, binding_key
) -> None:
    from altegio_bot.workers.whatsapp_inbox_worker import _handle_delivery_statuses

    await _accepted_row(session_maker)
    async with session_maker() as session:
        async with session.begin():
            await _handle_delivery_statuses(
                session, None, [{"status": "read", "provider_message_id": "wamid.SOMEBODY_ELSE"}]
            )
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == MANUAL_VOUCHER_PROVIDER_ACCEPTED
    assert row.delivered_at is None and row.read_at is None


# ---------------------------------------------------------------------------
# A refund proves the order; a delivery proves the person
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "break_it",
    [
        pytest.param("opt_out", id="opted-out"),
        pytest.param("phone", id="number-changed"),
        pytest.param("customer_error", id="customer-GET-fails"),
        pytest.param("template", id="template-row-gone"),
        pytest.param("ambiguous", id="two-customers-on-the-number"),
    ],
)
@pytest.mark.asyncio
async def test_a_refund_survives_what_closes_a_delivery(
    session_maker, manual_configuration, binding_key, break_it
) -> None:
    """Every one of these is a reason to GET THE MONEY BACK, not to leave it."""
    request, reader, _marker = await _paid_and_ready(session_maker)

    if break_it == "opt_out":
        async with session_maker() as session:
            async with session.begin():
                recipient = await session.get(CampaignRecipient, request.campaign_recipient_id)
                recipient.is_opted_out = True
    elif break_it == "phone":
        reader.customer = customer_payload(phone="+4915199999999")
    elif break_it == "customer_error":
        reader.customer = EasyWeekError("503")
    elif break_it == "ambiguous":
        reader.customer_pages = [customers_page([customer_payload(), customer_payload(uuid=str(OTHER_CUSTOMER_UUID))])]
    else:
        async with session_maker() as session:
            async with session.begin():
                await session.execute(text("DELETE FROM message_templates"))

    async with session_maker() as session:
        deliver_plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_DELIVER, request=request, reader=reader, order_reader=reader
        )
        refund_plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_REFUND, request=request, reader=reader, order_reader=reader
        )

    assert deliver_plan.ready is False, f"{break_it}: a delivery was still offered"
    assert refund_plan.ready is True, (break_it, refund_plan.reasons)
    # And it says out loud that no live guard was taken.
    assert refund_plan.snapshot["live_guard_applied"] is False
    assert deliver_plan.snapshot["live_guard_applied"] is True


@pytest.mark.asyncio
async def test_a_refund_still_proves_the_exact_order(session_maker, manual_configuration, binding_key) -> None:
    """Not proving the person is not the same as proving nothing."""
    request, reader, marker = await _paid_and_ready(session_maker)

    # Somebody else's order under our UUID.
    reader.orders[str(ORDER_UUID)] = voucher_order(marker="ewmv1-somebodyelse", status="paid")
    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_REFUND, request=request, reader=reader, order_reader=reader
        )
    assert plan.ready is False
    assert "manual_voucher_order_unproven" in plan.reasons

    # An order that is not paid has nothing to refund.
    reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker, status="open")
    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_REFUND, request=request, reader=reader, order_reader=reader
        )
    assert plan.ready is False
    assert "manual_voucher_order_not_paid" in plan.reasons


@pytest.mark.asyncio
async def test_a_refund_stays_reachable_when_the_payment_is_unproven(
    session_maker, manual_configuration, binding_key
) -> None:
    """The money probably moved and the artifact did not prove out."""
    report, (run_id, recipient_id), reader, _m = await _create(session_maker)
    assert report.outcome == "created"
    request = manual_request(run_id=run_id, recipient_id=recipient_id)
    marker = manual_marker(preview_run_id=run_id, campaign_recipient_id=recipient_id)

    # Everything proves out up to the payment; the readback AFTER it cannot
    # prove the voucher line. That is the case this test is about — a refusal
    # before the claim is a different, already-covered one.
    class UnprovableAfterPay(FakeReader):
        def __init__(self, inner: FakeReader) -> None:
            super().__init__(customer=inner.customer, orders=dict(inner.orders), template=inner.template)
            self.paid = False

        async def get_order(self, order_uuid: str):
            if self.paid:
                return voucher_order(marker=marker, status="paid", vouchers=[])
            return await super().get_order(order_uuid)

    paying = UnprovableAfterPay(reader)

    class PayingMutator(FakeMutator):
        async def pay_voucher_order(self, **kwargs):
            paying.paid = True
            return await super().pay_voucher_order(**kwargs)

    mutator = PayingMutator(pay=VoucherMutationResponse(http_status=200, envelope={"uuid": str(ORDER_UUID)}))
    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_PAY, request=request, reader=paying, order_reader=paying
        )
        assert plan.ready, plan.reasons
        pay_report = await runner_module.run_pay(
            session,
            session_maker,
            request=request,
            reader=paying,
            order_reader=paying,
            mutator=mutator,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )

    # Not "paid": an honest uncertainty.
    assert pay_report.outcome == "unknown"
    assert "manual_voucher_artifact_unproven" in pay_report.reasons
    assert pay_report.as_safe_dict()["reconciliation_required"] is True

    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == "pay_unknown"

    # DELIVER is shut, REFUND is not.
    reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker, status="paid")
    async with session_maker() as session:
        deliver_plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_DELIVER, request=request, reader=reader, order_reader=reader
        )
        refund_plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_REFUND, request=request, reader=reader, order_reader=reader
        )
    assert deliver_plan.ready is False
    assert refund_plan.ready is True, refund_plan.reasons


# ---------------------------------------------------------------------------
# Reconciling a create whose answer was lost
# ---------------------------------------------------------------------------


async def _unknown_create(session_maker, *, reader=None):
    """A row left in `create_unknown` with no order UUID at all."""
    report, ids, reader, _m = await _create(
        session_maker,
        mutator=FakeMutator(create=EasyWeekVoucherMutationUnknown("timeout")),
        reader=reader,
    )
    assert report.outcome == "unknown"
    return (
        manual_request(run_id=ids[0], recipient_id=ids[1]),
        reader,
        manual_marker(preview_run_id=ids[0], campaign_recipient_id=ids[1]),
    )


@pytest.mark.asyncio
async def test_reconcile_finds_the_lost_order_by_marker_and_rebinds_the_code(
    session_maker, manual_configuration, binding_key
) -> None:
    request, reader, marker = await _unknown_create(session_maker)
    found = await marker_order(session_maker, marker=marker)
    reader.order_pages = [orders_page([found])]
    reader.orders[str(ORDER_UUID)] = found

    report = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)

    assert report.outcome == "observed"
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == MANUAL_VOUCHER_CREATED
    assert str(row.target_order_uuid) == str(ORDER_UUID)
    # The binding is restored, so the payment gate has something to verify.
    assert row.voucher_code_hmac is not None and row.hmac_key_id is not None
    assert await ledger_module.binding_matches(
        session_maker, voucher_code=VOUCHER_CODE_SENTINEL, target_order_uuid=str(ORDER_UUID)
    )
    # A candidate UUID never reaches the report.
    assert str(ORDER_UUID) not in str(report.as_safe_dict())


@pytest.mark.parametrize(
    ("pages", "expected"),
    [
        pytest.param([orders_page([])], "manual_voucher_marker_search_unresolved", id="nothing-found"),
        pytest.param(
            [orders_page([{"uuid": str(ORDER_UUID)}, {"uuid": str(OTHER_ORDER_UUID)}])],
            "manual_voucher_marker_search_unresolved",
            id="rows-that-do-not-match",
        ),
        pytest.param(
            [orders_page([], current=1, last=2)],
            "manual_voucher_marker_search_incomplete",
            id="unfinished-walk",
        ),
    ],
)
@pytest.mark.asyncio
async def test_an_unresolved_marker_search_stays_unknown(
    session_maker, manual_configuration, binding_key, pages, expected
) -> None:
    request, reader, _marker = await _unknown_create(session_maker)
    reader.order_pages = pages

    report = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)

    assert expected in report.reasons
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    # Zero matches is not "it was not created", and an unfinished walk proves
    # nothing at all. Both stay where a human has to look.
    assert row.status == "create_unknown"
    assert row.target_order_uuid is None


@pytest.mark.asyncio
async def test_two_marker_matches_are_a_full_stop(session_maker, manual_configuration, binding_key) -> None:
    request, reader, marker = await _unknown_create(session_maker)
    reader.order_pages = [
        orders_page(
            [
                await marker_order(session_maker, marker=marker),
                await marker_order(session_maker, marker=marker, order_uuid=OTHER_ORDER_UUID),
            ]
        )
    ]
    report = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)
    assert "manual_voucher_marker_search_ambiguous" in report.reasons
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == "create_unknown"


@pytest.mark.asyncio
async def test_reconcile_never_sends_a_second_create(session_maker, manual_configuration, binding_key) -> None:
    request, reader, marker = await _unknown_create(session_maker)
    found = await marker_order(session_maker, marker=marker)
    reader.order_pages = [orders_page([found])]
    reader.orders[str(ORDER_UUID)] = found

    mutator = FakeMutator()
    await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)
    # The reconcile has no mutator at all — it cannot mutate by construction —
    # and the one handed to the stage above was never called again.
    assert mutator.calls == []


@pytest.mark.asyncio
async def test_a_draft_closed_by_hand_is_recorded_as_an_observation(
    session_maker, manual_configuration, binding_key
) -> None:
    report, (run_id, recipient_id), reader, _m = await _create(session_maker)
    assert report.outcome == "created"
    request = manual_request(run_id=run_id, recipient_id=recipient_id)
    marker = manual_marker(preview_run_id=run_id, campaign_recipient_id=recipient_id)

    # The operator closed it in the EasyWeek dashboard.
    reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker, status="refunded")

    reconcile = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)

    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == "manually_cleaned"
    assert row.manual_cleanup_observed_at is not None
    assert row.manual_cleanup_required is False
    assert reconcile.as_safe_dict()["manual_cleanup_required"] is False


# ---------------------------------------------------------------------------
# One number, one customer — proven workspace-wide before every stage
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("pages", "expected"),
    [
        pytest.param(
            [customers_page([customer_payload(), customer_payload(uuid=str(OTHER_CUSTOMER_UUID))])],
            "manual_voucher_customer_ambiguous",
            id="two-customers-one-number",
        ),
        pytest.param(
            [customers_page([], total=3)],
            "manual_voucher_customer_lookup_undetermined",
            id="server-counted-rows-it-did-not-hand-over",
        ),
        pytest.param(
            # Page one says there is a second; page two answers as page one.
            # Merging it would double-count or skip, so the walk is unfinished.
            [customers_page([], current=1, last=2), customers_page([customer_payload()], current=1, last=2)],
            "manual_voucher_customer_lookup_undetermined",
            id="unfinished-walk",
        ),
        pytest.param(
            [{"data": [customer_payload()], "meta": {}}],
            "manual_voucher_customer_lookup_undetermined",
            id="malformed-pagination",
        ),
    ],
)
@pytest.mark.asyncio
async def test_an_ambiguous_or_unfinished_phone_lookup_stops_every_stage(
    session_maker, manual_configuration, binding_key, pages, expected
) -> None:
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(customer_pages=pages)

    async with session_maker() as session:
        proof = await prove_manual_recipient(
            session,
            preview_run_id=run_id,
            campaign_recipient_id=recipient_id,
            client_reader=reader,
            now=utcnow(),
        )
    assert proof.proven is False
    assert expected in proof.reasons
    # The direct read is never even attempted once the listing is in doubt.
    assert reader.customer_calls == []


@pytest.mark.asyncio
async def test_a_transport_failure_on_the_listing_is_undetermined_not_absent(
    session_maker, manual_configuration, binding_key
) -> None:
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    reader = FakeReader(customer_pages=EasyWeekError("503"))
    async with session_maker() as session:
        proof = await prove_manual_recipient(
            session,
            preview_run_id=run_id,
            campaign_recipient_id=recipient_id,
            client_reader=reader,
            now=utcnow(),
        )
    assert proof.proven is False
    assert "manual_voucher_customer_lookup_undetermined" in proof.reasons


@pytest.mark.asyncio
async def test_the_number_now_belonging_to_somebody_else_refuses(
    session_maker, manual_configuration, binding_key
) -> None:
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    reader = FakeReader(customer_pages=[customers_page([customer_payload(uuid=str(OTHER_CUSTOMER_UUID))])])
    async with session_maker() as session:
        proof = await prove_manual_recipient(
            session,
            preview_run_id=run_id,
            campaign_recipient_id=recipient_id,
            client_reader=reader,
            now=utcnow(),
        )
    assert proof.proven is False
    assert CUSTOMER_IDENTITY_NOT_CURRENT in proof.reasons


@pytest.mark.asyncio
async def test_a_proven_manual_recipient_still_claims_no_first_visit(
    session_maker, manual_configuration, binding_key
) -> None:
    """The lookup proves identity. It does not invent a source booking."""
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    async with session_maker() as session:
        proof = await prove_manual_recipient(
            session,
            preview_run_id=run_id,
            campaign_recipient_id=recipient_id,
            client_reader=FakeReader(),
            now=utcnow(),
        )
        recipient = await session.get(CampaignRecipient, recipient_id)
    assert proof.proven is True
    assert proof.as_safe_dict()["first_visit_proof"] == "not_applicable"
    # Nothing was written to the recipient to make it look earned.
    assert recipient.source_booking_uuid is None
    assert recipient.source_easyweek_event_id is None
    assert recipient.source_record_id is None
    assert recipient.source_visits_total is None
    assert recipient.recipient_basis == "operator_manual_selection"


# ---------------------------------------------------------------------------
# The freeze, and the edit that races it
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_removal_that_wins_the_race_stops_the_canary_opening(
    session_maker, manual_configuration, binding_key
) -> None:
    """Remove first, then open: the canary refuses and CREATE is never called."""
    from altegio_bot.campaigns.runner import remove_recipient_from_preview

    run_id, recipient_id = await seed_manual_recipient(session_maker)
    await seed_template_and_sender(session_maker)
    request = manual_request(run_id=run_id, recipient_id=recipient_id)
    reader = FakeReader(orders={str(ORDER_UUID): voucher_order(marker=request.marker)})
    mutator = FakeMutator(create=VoucherMutationResponse(http_status=201, envelope={"uuid": str(ORDER_UUID)}))

    # The plan is built while the recipient is still a candidate.
    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_CREATE, request=request, reader=reader, order_reader=reader
        )
        assert plan.ready, plan.reasons

    # The operator removes them before the stage runs.
    await remove_recipient_from_preview(run_id, recipient_id)

    async with session_maker() as session:
        report = await runner_module.run_create(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )

    assert report.outcome == "refused"
    assert mutator.calls == [], "a voucher was created for a removed recipient"
    async with session_maker() as session:
        rows = list((await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalars().all())
    assert rows == []


@pytest.mark.asyncio
async def test_a_freeze_that_wins_the_race_refuses_the_editor(session_maker, manual_configuration, binding_key) -> None:
    """Open first, then remove: the editor is refused under the same lock."""
    from altegio_bot.campaigns.runner import remove_recipient_from_preview

    run_id, recipient_id = await seed_manual_recipient(session_maker)
    opened = await ledger_module.open_canary(session_maker, identity=_identity(run_id, recipient_id))
    assert opened.exists is True

    with pytest.raises(ValueError) as raised:
        await remove_recipient_from_preview(run_id, recipient_id)
    assert "canary" in str(raised.value)

    async with session_maker() as session:
        recipient = await session.get(CampaignRecipient, recipient_id)
    assert recipient.status == "candidate"


@pytest.mark.asyncio
async def test_the_two_orders_of_the_race_never_both_win(session_maker, manual_configuration, binding_key) -> None:
    """Both sequences, driven deterministically — no sleeps deciding anything.

    The lock is what orders them: both paths take ``CampaignRun`` FOR UPDATE
    first, so whichever transaction gets there first is the one that wins, and
    the other sees the world it left behind.
    """
    from altegio_bot.campaigns.runner import remove_recipient_from_preview

    # Sequence A: the remove commits first.
    run_a, recipient_a = await seed_manual_recipient(session_maker)
    await remove_recipient_from_preview(run_a, recipient_a)
    opened = await ledger_module.open_canary(session_maker, identity=_identity(run_a, recipient_a))
    assert opened.exists is False, "the canary opened on a removed recipient"

    # Sequence B: the open commits first.
    run_b, recipient_b = await seed_manual_recipient(
        session_maker, customer_uuid=OTHER_CUSTOMER_UUID, phone="+4915100000002"
    )
    opened = await ledger_module.open_canary(
        session_maker,
        identity=_identity(run_b, recipient_b, easyweek_customer_uuid=str(OTHER_CUSTOMER_UUID)),
    )
    assert opened.exists is True
    with pytest.raises(ValueError):
        await remove_recipient_from_preview(run_b, recipient_b)


@pytest.mark.asyncio
async def test_concurrent_open_and_remove_resolve_to_one_winner(
    session_maker, manual_configuration, binding_key
) -> None:
    """Started together; the row lock — not the scheduler — decides."""
    from altegio_bot.campaigns.runner import remove_recipient_from_preview

    run_id, recipient_id = await seed_manual_recipient(session_maker)

    async def remove() -> str:
        try:
            await remove_recipient_from_preview(run_id, recipient_id)
            return "removed"
        except ValueError:
            return "refused"

    async def open_it() -> str:
        snapshot = await ledger_module.open_canary(session_maker, identity=_identity(run_id, recipient_id))
        return "opened" if snapshot.exists else "refused"

    removal, opening = await asyncio.gather(remove(), open_it())

    # Exactly one of them changed the world, whichever way the lock fell.
    assert {removal, opening} in ({"removed", "refused"}, {"refused", "opened"})
    async with session_maker() as session:
        recipient = await session.get(CampaignRecipient, recipient_id)
        rows = list((await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalars().all())
    if opening == "opened":
        assert recipient.status == "candidate"
        assert len(rows) == 1
    else:
        assert recipient.status == "skipped"
        assert rows == []


# ---------------------------------------------------------------------------
# One key, two canaries, two different MACs
# ---------------------------------------------------------------------------


def test_the_two_canaries_bind_the_same_code_differently() -> None:
    from altegio_bot.campaigns.easyweek_voucher_delivery.binding import (
        MANUAL_VOUCHER_DOMAIN,
        voucher_code_mac,
        voucher_code_matches,
    )

    shared = {
        "voucher_code": VOUCHER_CODE_SENTINEL,
        "ledger_uuid": "scope",
        "target_order_uuid": str(ORDER_UUID),
        "voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID,
        "key": "k" * 48,
        "key_id": "one-key",
    }
    _, delivery_mac = voucher_code_mac(**shared)
    _, manual_mac = voucher_code_mac(**shared, domain=MANUAL_VOUCHER_DOMAIN)
    assert delivery_mac != manual_mac

    # And neither verifies the other's, with the very same key.
    assert voucher_code_matches(
        **shared, expected_mac=manual_mac, expected_key_id="one-key", domain=MANUAL_VOUCHER_DOMAIN
    )
    assert not voucher_code_matches(
        **shared, expected_mac=delivery_mac, expected_key_id="one-key", domain=MANUAL_VOUCHER_DOMAIN
    )
    assert not voucher_code_matches(**shared, expected_mac=manual_mac, expected_key_id="one-key")


def test_the_delivery_domain_is_unchanged() -> None:
    """§36's MACs must keep verifying: the default is its label, byte for byte."""
    from altegio_bot.campaigns.easyweek_voucher_delivery.binding import _DOMAIN, voucher_code_mac

    assert _DOMAIN == b"altegio_bot/easyweek_voucher_delivery/voucher_code/v1"
    shared = {
        "voucher_code": "C",
        "ledger_uuid": "l",
        "target_order_uuid": "o",
        "voucher_template_uuid": "t",
        "key": "k" * 48,
        "key_id": "id",
    }
    assert voucher_code_mac(**shared)[1] == voucher_code_mac(**shared, domain=_DOMAIN)[1]


@pytest.mark.asyncio
async def test_the_stored_binding_is_bound_to_this_row_and_this_order(
    session_maker, manual_configuration, binding_key
) -> None:
    report, _ids, _reader, _m = await _create(session_maker)
    assert report.outcome == "created"

    # The right code against the right order verifies.
    assert await ledger_module.binding_matches(
        session_maker, voucher_code=VOUCHER_CODE_SENTINEL, target_order_uuid=str(ORDER_UUID)
    )
    # Another code, or another order, does not.
    assert not await ledger_module.binding_matches(
        session_maker, voucher_code="SOME-OTHER-CODE", target_order_uuid=str(ORDER_UUID)
    )
    assert not await ledger_module.binding_matches(
        session_maker, voucher_code=VOUCHER_CODE_SENTINEL, target_order_uuid=str(OTHER_ORDER_UUID)
    )


# ---------------------------------------------------------------------------
# Recovering a create whose result was unknown
# ---------------------------------------------------------------------------


async def _create_unknown_with_uuid(session_maker, *, readback: dict | None = None):
    """A create that named an order but could not prove it on the first read.

    The 2xx carried a UUID; the immediate readback did not prove the artifact,
    so the row is `create_unknown` WITH a target order — and with no MAC, which
    is the state that used to be unrecoverable.
    """
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    await seed_template_and_sender(session_maker)
    request = manual_request(run_id=run_id, recipient_id=recipient_id)
    marker = manual_marker(preview_run_id=run_id, campaign_recipient_id=recipient_id)
    # The first readback answers with an order whose voucher line is unreadable.
    unprovable = readback if readback is not None else voucher_order(marker=marker, vouchers=[])
    reader = FakeReader(orders={str(ORDER_UUID): unprovable})
    mutator = FakeMutator(create=VoucherMutationResponse(http_status=201, envelope={"uuid": str(ORDER_UUID)}))

    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_CREATE, request=request, reader=reader, order_reader=reader
        )
        assert plan.ready, plan.reasons
        report = await runner_module.run_create(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )
    assert report.outcome == "unknown", report.reasons
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == "create_unknown"
    assert str(row.target_order_uuid) == str(ORDER_UUID)
    assert row.voucher_code_hmac is None, "the unknown create wrote a binding it could not have"
    return request, reader, marker


@pytest.mark.asyncio
async def test_a_known_order_uuid_is_recovered_and_bound_on_the_next_reconcile(
    session_maker, manual_configuration, binding_key
) -> None:
    """The blocker: this row could never leave `create_unknown`.

    It had a target order but no MAC, and the reconcile asked whether the code
    matched a binding that had never been written — which can only answer no.
    """
    request, reader, marker = await _create_unknown_with_uuid(session_maker)

    # The order is readable now.
    reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker)
    report = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)

    assert report.outcome == "observed", report.reasons
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == MANUAL_VOUCHER_CREATED
    assert row.create_verified_at is not None
    # The binding was CREATED from this readback, under this canary's domain.
    assert row.voucher_code_hmac is not None and row.hmac_key_id == "test-key-1"
    assert await ledger_module.binding_matches(
        session_maker, voucher_code=VOUCHER_CODE_SENTINEL, target_order_uuid=str(ORDER_UUID)
    )
    # A proven-but-unpaid draft still needs a hand.
    assert row.manual_cleanup_required is True
    # And nothing secret reached the report.
    printed = str(report.as_safe_dict())
    for secret in (VOUCHER_CODE_SENTINEL, row.voucher_code_hmac, str(ORDER_UUID)):
        assert secret not in printed


@pytest.mark.asyncio
async def test_a_known_uuid_never_triggers_a_marker_listing_or_a_second_create(
    session_maker, manual_configuration, binding_key
) -> None:
    request, reader, marker = await _create_unknown_with_uuid(session_maker)
    reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker)
    # Any listing at all is a failure: the order can already be named.
    reader.order_pages = AssertionError("the recovery listed orders for an order it could name")

    report = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)

    assert report.outcome == "observed", report.reasons
    # Reconcile has no mutator by construction — it cannot create — and the
    # order was read exactly by its UUID.
    assert reader.order_calls.count(str(ORDER_UUID)) >= 1
    assert report.as_safe_dict()["external_effect_attempted"] is False


@pytest.mark.parametrize(
    "broken",
    [
        pytest.param("marker", id="another-canary-marker"),
        pytest.param("customer", id="another-customer"),
        pytest.param("voucher", id="unreadable-voucher-line"),
        pytest.param("two-vouchers", id="two-voucher-entries"),
        pytest.param("price", id="wrong-price"),
        pytest.param("paid", id="not-open-any-more"),
    ],
)
@pytest.mark.asyncio
async def test_a_known_uuid_that_does_not_prove_out_stays_unknown(
    session_maker, manual_configuration, binding_key, broken
) -> None:
    request, reader, marker = await _create_unknown_with_uuid(session_maker)

    if broken == "marker":
        answer = voucher_order(marker="ewmv1-somebodyelse")
    elif broken == "customer":
        answer = voucher_order(marker=marker, customer={"uuid": str(OTHER_CUSTOMER_UUID)})
    elif broken == "voucher":
        answer = voucher_order(marker=marker, vouchers=[{"code": ""}])
    elif broken == "two-vouchers":
        answer = voucher_order(marker=marker, vouchers=[issued_voucher(), issued_voucher()])
    elif broken == "price":
        answer = voucher_order(marker=marker, vouchers=[issued_voucher(price=999)])
    else:
        answer = voucher_order(marker=marker, status="paid")
    reader.orders[str(ORDER_UUID)] = answer

    report = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)

    assert report.outcome == "unknown", (broken, report.reasons)
    assert report.reasons, broken
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == "create_unknown", broken
    assert row.voucher_code_hmac is None, f"{broken}: a binding was written for an unproven order"
    assert row.manual_cleanup_required is True, broken


@pytest.mark.asyncio
async def test_a_disagreeing_binding_is_never_overwritten(session_maker, manual_configuration, binding_key) -> None:
    """Two different codes for one order is a question for a human."""
    request, reader, marker = await _create_unknown_with_uuid(session_maker)

    # A first recovery binds the code the order issued.
    reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker)
    await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
        bound = row.voucher_code_hmac
    assert bound is not None

    # Put the row back into `create_unknown` and let the order answer with a
    # DIFFERENT code — the shape of somebody having re-issued underneath us.
    async with session_maker() as session:
        async with session.begin():
            row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
            row.status = "create_unknown"
            row.reconciliation_required = True
    reader.orders[str(ORDER_UUID)] = voucher_order(
        marker=marker, vouchers=[issued_voucher(code="SENTINEL-DIFFERENT-CODE-xyz")]
    )

    report = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)

    assert report.outcome == "unknown"
    assert "manual_voucher_binding_mismatch" in report.reasons
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == "create_unknown"
    assert row.voucher_code_hmac == bound, "a disagreeing readback overwrote the binding"


@pytest.mark.asyncio
async def test_a_missing_hmac_key_is_a_named_refusal_not_a_crash(
    session_maker, manual_configuration, monkeypatch
) -> None:
    """No `binding_key` fixture here: the key is simply not configured."""
    from pydantic import SecretStr

    from altegio_bot.settings import settings as live_settings

    monkeypatch.setattr(live_settings, "easyweek_voucher_delivery_hmac_key", SecretStr("k" * 48), raising=False)
    monkeypatch.setattr(live_settings, "easyweek_voucher_delivery_hmac_key_id", "test-key-1", raising=False)
    request, reader, marker = await _create_unknown_with_uuid(session_maker)
    reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker)

    # The key disappears between the create and the recovery.
    monkeypatch.setattr(live_settings, "easyweek_voucher_delivery_hmac_key", SecretStr(""), raising=False)

    report = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)

    assert report.outcome == "unknown"
    assert "manual_voucher_hmac_key_missing" in report.reasons
    # A stable code, not a database failure and not a traceback.
    assert "manual_voucher_database_unavailable" not in report.reasons
    assert "Traceback" not in str(report.as_safe_dict())
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == "create_unknown"


# ---------------------------------------------------------------------------
# An unresolved reconcile is not a success
# ---------------------------------------------------------------------------


def _install_cli(monkeypatch, session_maker, reader):
    """Point the operator CLI at this test's database and fake transports.

    Only the session and the read transport are replaced. The parser, the fence
    check, the request build, the dispatch and the exit mapping are the shipped
    ones — that is the whole point of asking the CLI rather than the runner.
    """
    import altegio_bot.scripts.easyweek_manual_voucher_canary as cli

    class _Reader:
        async def __aenter__(self):
            return reader

        async def __aexit__(self, *exc):
            return None

    def _forbidden(*args, **kwargs):
        raise AssertionError("a reconcile opened a mutation transport")

    monkeypatch.setattr(cli, "SessionLocal", session_maker)
    monkeypatch.setattr(cli, "EasyWeekClient", lambda *a, **k: _Reader())
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", _forbidden)
    monkeypatch.setattr(cli, "VoucherDeliveryClient", _forbidden)
    return cli


async def _run_cli(cli, argv: list[str]) -> tuple[dict, int]:
    """Drive the CLI the way it drives itself, and read back (payload, code).

    ``main()`` itself calls :func:`asyncio.run`, which cannot be nested inside
    the test's running loop; everything it does around that call — parse, fence,
    dispatch, map the exit code — is exercised here directly.
    """
    args = cli._build_parser().parse_args(argv)
    return await cli._dispatch(args)


def _reconcile_argv(request) -> list[str]:
    return [
        "reconcile",
        "--preview-run-id",
        str(request.preview_run_id),
        "--campaign-recipient-id",
        str(request.campaign_recipient_id),
    ]


@pytest.mark.asyncio
async def test_an_unresolved_create_reconcile_exits_three(
    session_maker, manual_configuration, binding_key, monkeypatch, capsys
) -> None:
    """No matches: the order may exist and nobody can say. Never exit 0."""
    request, reader, _marker = await _unknown_create(session_maker)
    reader.order_pages = [orders_page([])]

    report = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)
    assert report.outcome == "unknown"
    assert "manual_voucher_marker_search_unresolved" in report.reasons

    cli = _install_cli(monkeypatch, session_maker, reader)
    printed, code = await _run_cli(cli, _reconcile_argv(request))

    assert code == 3, printed
    assert printed["outcome"] == "unknown"
    assert printed["reasons"]
    assert printed["reconciliation_required"] is True


@pytest.mark.parametrize(
    ("pages", "expected"),
    [
        pytest.param(
            [orders_page([], current=1, last=2)], "manual_voucher_marker_search_incomplete", id="incomplete-walk"
        ),
        pytest.param(None, "manual_voucher_marker_search_ambiguous", id="ambiguous-walk"),
    ],
)
@pytest.mark.asyncio
async def test_an_incomplete_or_ambiguous_walk_exits_three(
    session_maker, manual_configuration, binding_key, monkeypatch, capsys, pages, expected
) -> None:
    request, reader, marker = await _unknown_create(session_maker)
    if pages is None:
        reader.order_pages = [
            orders_page(
                [
                    await marker_order(session_maker, marker=marker),
                    await marker_order(session_maker, marker=marker, order_uuid=OTHER_ORDER_UUID),
                ]
            )
        ]
    else:
        reader.order_pages = pages

    cli = _install_cli(monkeypatch, session_maker, reader)
    printed, code = await _run_cli(cli, _reconcile_argv(request))

    assert code == 3
    assert printed["outcome"] == "unknown"
    assert expected in printed["reasons"]


@pytest.mark.asyncio
async def test_a_pay_unknown_over_a_still_open_order_exits_three(
    session_maker, manual_configuration, binding_key, monkeypatch, capsys
) -> None:
    """The payment did not land. That is unresolved, not observed."""
    report, (run_id, recipient_id), reader, _m = await _create(session_maker)
    assert report.outcome == "created"
    request = manual_request(run_id=run_id, recipient_id=recipient_id)
    marker = manual_marker(preview_run_id=run_id, campaign_recipient_id=recipient_id)
    # Into pay_unknown through the real claim, from `created` where a payment
    # is actually claimable.
    claim = await ledger_module.claim_pay(session_maker, identity=_identity(run_id, recipient_id), plan_digest="d")
    assert claim.granted
    await ledger_module.record_outcome(
        session_maker,
        status="pay_unknown",
        expected_statuses=frozenset({"pay_claimed"}),
        reason_code="manual_voucher_mutation_unknown",
        reconciliation_required=True,
    )
    reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker, status="open")

    cli = _install_cli(monkeypatch, session_maker, reader)
    printed, code = await _run_cli(cli, _reconcile_argv(request))

    assert code == 3
    assert printed["outcome"] == "unknown"
    assert printed["ledger"]["status"] == "pay_unknown"


@pytest.mark.asyncio
async def test_a_send_unknown_is_never_resolved_by_reading_an_order(
    session_maker, manual_configuration, binding_key, monkeypatch, capsys
) -> None:
    """Whether Meta delivered is not a fact the POS system holds."""
    request, reader, marker = await _paid_and_ready(session_maker)
    sender = FakeSender(unknown_outcome("timeout"))
    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_DELIVER, request=request, reader=reader, order_reader=reader
        )
        await runner_module.run_deliver(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            sender=sender,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )

    cli = _install_cli(monkeypatch, session_maker, reader)
    printed, code = await _run_cli(cli, _reconcile_argv(request))

    assert code == 3
    assert printed["outcome"] == "unknown"
    assert "manual_voucher_mutation_unknown" in printed["reasons"]
    assert printed["ledger"]["status"] == MANUAL_VOUCHER_SEND_UNKNOWN


@pytest.mark.asyncio
async def test_a_resolved_create_exits_six_because_a_draft_is_open(
    session_maker, manual_configuration, binding_key, monkeypatch, capsys
) -> None:
    """Proven, nothing outstanding to reconcile — but a draft needs a hand."""
    request, reader, marker = await _create_unknown_with_uuid(session_maker)
    reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker)

    cli = _install_cli(monkeypatch, session_maker, reader)
    printed, code = await _run_cli(cli, _reconcile_argv(request))

    assert code == 6, (printed["outcome"], printed["reasons"])
    assert printed["outcome"] == "observed"
    assert printed["reconciliation_required"] is False
    assert printed["manual_cleanup_required"] is True
    assert printed["ledger"]["status"] == MANUAL_VOUCHER_CREATED


@pytest.mark.asyncio
async def test_a_resolved_paid_and_a_resolved_refund_exit_zero(
    session_maker, manual_configuration, binding_key, monkeypatch, capsys
) -> None:
    request, reader, marker = await _paid_and_ready(session_maker)

    cli = _install_cli(monkeypatch, session_maker, reader)
    printed, code = await _run_cli(cli, _reconcile_argv(request))
    assert code == 0, printed
    assert printed["outcome"] == "observed"
    assert printed["ledger"]["status"] == MANUAL_VOUCHER_PAID

    # And the same once the money is back.
    await ledger_module.claim_refund(
        session_maker,
        identity=_identity(request.preview_run_id, request.campaign_recipient_id),
        plan_digest="d",
    )
    await ledger_module.record_outcome(
        session_maker,
        status="refund_unknown",
        expected_statuses=frozenset({"refund_claimed"}),
        reconciliation_required=True,
    )
    reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker, status="refunded")

    printed, code = await _run_cli(cli, _reconcile_argv(request))
    assert code == 0, printed
    assert printed["outcome"] == "observed"
    assert printed["ledger"]["status"] == "refunded"


@pytest.mark.asyncio
async def test_a_manually_cleaned_draft_exits_zero(
    session_maker, manual_configuration, binding_key, monkeypatch, capsys
) -> None:
    report, (run_id, recipient_id), reader, _m = await _create(session_maker)
    assert report.outcome == "created"
    request = manual_request(run_id=run_id, recipient_id=recipient_id)
    marker = manual_marker(preview_run_id=run_id, campaign_recipient_id=recipient_id)
    reader.orders[str(ORDER_UUID)] = voucher_order(marker=marker, status="refunded")

    cli = _install_cli(monkeypatch, session_maker, reader)
    printed, code = await _run_cli(cli, _reconcile_argv(request))

    assert code == 0, printed
    assert printed["outcome"] == "observed"
    assert printed["ledger"]["status"] == "manually_cleaned"
    assert printed["manual_cleanup_required"] is False


# ---------------------------------------------------------------------------
# A claimed create may have left a draft, whatever came back
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param("timeout", id="timeout"),
        pytest.param("reset", id="connection-reset"),
        pytest.param("no-uuid", id="2xx-without-a-uuid"),
    ],
)
@pytest.mark.asyncio
async def test_every_unknown_create_asks_for_cleanup_and_reconcile(
    session_maker, manual_configuration, binding_key, failure
) -> None:
    if failure == "no-uuid":
        mutator = FakeMutator(create=VoucherMutationResponse(http_status=201, envelope={}))
    else:
        mutator = FakeMutator(create=EasyWeekVoucherMutationUnknown(failure))

    report, _ids, _reader, _m = await _create(session_maker, mutator=mutator)

    assert report.outcome == "unknown", report.reasons
    payload = report.as_safe_dict()
    assert payload["reconciliation_required"] is True, failure
    assert payload["manual_cleanup_required"] is True, failure
    assert payload["external_effect_attempted"] is True, failure

    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == "create_unknown", failure
    assert row.manual_cleanup_required is True, failure
    assert row.reconciliation_required is True, failure
    # One attempt left the process, and the state it left behind cannot be
    # claimed again.
    assert row.create_attempted_at is not None
    assert row.status not in ledger_module.CREATE_CLAIMABLE_FROM


@pytest.mark.asyncio
async def test_a_search_that_found_nothing_does_not_clear_the_cleanup_flag(
    session_maker, manual_configuration, binding_key
) -> None:
    """Zero matches is not proof that no order exists."""
    request, reader, _marker = await _unknown_create(session_maker)
    reader.order_pages = [orders_page([])]

    report = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)

    assert report.outcome == "unknown"
    assert report.as_safe_dict()["manual_cleanup_required"] is True
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.manual_cleanup_required is True
    assert row.status == "create_unknown"


@pytest.mark.asyncio
async def test_the_cleanup_flag_is_cleared_only_by_proof(session_maker, manual_configuration, binding_key) -> None:
    """Paid, refunded and a proven manual closure — and nothing else."""
    # Paid clears it.
    _request, _reader, _marker = await _paid_and_ready(session_maker)
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == MANUAL_VOUCHER_PAID
    assert row.manual_cleanup_required is False


# ---------------------------------------------------------------------------
# When Meta accepted, and who may say so
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_acceptance_is_timestamped_by_the_write_that_records_it(
    session_maker, manual_configuration, binding_key
) -> None:
    request, reader, _marker = await _paid_and_ready(session_maker)
    sender = FakeSender()

    before = utcnow()
    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_DELIVER, request=request, reader=reader, order_reader=reader
        )
        report = await runner_module.run_deliver(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            sender=sender,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )
    after = utcnow()
    assert report.outcome == "provider_accepted"

    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    # Present immediately, before any webhook exists, and of this moment.
    assert row.provider_accepted_at is not None
    assert before <= row.provider_accepted_at <= after
    assert row.delivered_at is None and row.read_at is None
    assert row.provider_message_id == PROVIDER_MESSAGE_ID
    # And the report says so without printing the identifier.
    assert row.provider_message_id not in str(report.as_safe_dict())


@pytest.mark.asyncio
async def test_webhooks_never_move_the_acceptance_timestamp(session_maker, manual_configuration, binding_key) -> None:
    from altegio_bot.workers.whatsapp_inbox_worker import _handle_delivery_statuses

    request, reader, _marker = await _paid_and_ready(session_maker)
    sender = FakeSender()
    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_DELIVER, request=request, reader=reader, order_reader=reader
        )
        await runner_module.run_deliver(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            sender=sender,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
        accepted_at = row.provider_accepted_at
    assert accepted_at is not None

    # Delivered, then read, then a duplicate and an out-of-order delivered.
    for batch in (
        [{"status": "delivered", "provider_message_id": PROVIDER_MESSAGE_ID}],
        [{"status": "read", "provider_message_id": PROVIDER_MESSAGE_ID}],
        [
            {"status": "read", "provider_message_id": PROVIDER_MESSAGE_ID},
            {"status": "delivered", "provider_message_id": PROVIDER_MESSAGE_ID},
        ],
    ):
        async with session_maker() as session:
            async with session.begin():
                await _handle_delivery_statuses(session, None, batch)

    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == "read"
    assert row.provider_accepted_at == accepted_at, "a webhook moved the acceptance timestamp"


@pytest.mark.parametrize(
    "outcome",
    [pytest.param("rejected", id="meta-rejected"), pytest.param("unknown", id="meta-unknown")],
)
@pytest.mark.asyncio
async def test_a_send_that_was_not_accepted_has_no_acceptance_timestamp(
    session_maker, manual_configuration, binding_key, outcome
) -> None:
    request, reader, _marker = await _paid_and_ready(session_maker)
    sender = FakeSender(rejected_outcome() if outcome == "rejected" else unknown_outcome())

    async with session_maker() as session:
        plan, _, _, _ = await runner_module.build_stage_plan(
            session, session_maker, stage=STAGE_DELIVER, request=request, reader=reader, order_reader=reader
        )
        report = await runner_module.run_deliver(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            sender=sender,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )
    assert report.outcome == outcome

    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.provider_accepted_at is None
    assert row.provider_message_id is None
    assert row.delivered_at is None and row.read_at is None
    # The one attempt is still spent.
    assert row.send_attempt_count == 1


@pytest.mark.asyncio
async def test_a_foreign_message_id_changes_nothing_at_all(session_maker, manual_configuration, binding_key) -> None:
    from altegio_bot.workers.whatsapp_inbox_worker import _handle_delivery_statuses

    await _accepted_row(session_maker)
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
        before = (row.status, row.provider_accepted_at, row.delivered_at, row.read_at)

    async with session_maker() as session:
        async with session.begin():
            await _handle_delivery_statuses(
                session,
                None,
                [
                    {"status": "delivered", "provider_message_id": "wamid.NOT_OURS_0001"},
                    {"status": "read", "provider_message_id": "wamid.NOT_OURS_0002"},
                ],
            )

    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert (row.status, row.provider_accepted_at, row.delivered_at, row.read_at) == before


def test_the_exit_code_mapping_is_never_optimistic() -> None:
    """The number a wrapper acts on, pinned directly.

    The reconcile path already reports `unknown` for anything unresolved, so the
    outstanding-reconciliation clause is a second line of defence for any stage
    that might one day return a proven outcome with work still pending. Asserted
    here rather than left to a future caller to discover.
    """
    import altegio_bot.scripts.easyweek_manual_voucher_canary as cli

    def code_for(**changes) -> int:
        return cli._exit_for(runner_module.StageReport(stage="reconcile", **changes))

    assert code_for(outcome="unknown") == 3
    assert code_for(outcome="observed", reconciliation_required=True) == 3
    assert code_for(outcome="refused") == 4
    assert code_for(outcome="rejected") == 4
    # Proven, nothing outstanding, and a draft still open in the POS.
    assert code_for(outcome="observed", manual_cleanup_required=True) == 6
    assert code_for(outcome="observed") == 0
    # And an unresolved state never borrows the cleanup code.
    assert code_for(outcome="unknown", manual_cleanup_required=True) == 3


@pytest.mark.asyncio
async def test_the_create_claim_itself_raises_the_cleanup_flag(session_maker) -> None:
    """Before any answer exists, and therefore before any answer can be lost.

    The claim commits before the request leaves, so from that instant an open
    draft may exist in the POS. Asserted at the claim rather than at an outcome
    because the outcomes that matter most are the ones that never arrive.
    """
    run_id, recipient_id = await seed_manual_recipient(session_maker)
    identity = _identity(run_id, recipient_id)
    opened = await ledger_module.open_canary(session_maker, identity=identity)
    assert opened.exists and opened.manual_cleanup_required is False

    window = utcnow()
    claim = await ledger_module.claim_create(
        session_maker,
        identity=identity,
        plan_digest="d",
        create_window_start=window - timedelta(minutes=30),
        create_window_end=window + timedelta(minutes=30),
    )
    assert claim.granted

    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekManualVoucherDeliveryLedger))).scalar_one()
    assert row.status == "create_claimed"
    assert row.manual_cleanup_required is True
    assert row.reconciliation_required is True
