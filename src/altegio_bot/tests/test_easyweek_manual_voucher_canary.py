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
from sqlalchemy import select
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
    EasyWeekManualVoucherDeliveryAttempt,
    EasyWeekManualVoucherDeliveryLedger,
)
from altegio_bot.tests.easyweek_manual_voucher_fixtures import (  # noqa: F401 - fixtures
    ACCOUNT_UUID,
    COMPANY_ID,
    EW_CUSTOMER_UUID,
    ORDER_UUID,
    OTHER_CUSTOMER_UUID,
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
    issued_voucher,
    manual_request,
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
