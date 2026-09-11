"""Durable claim ledger of the voucher canary, against real PostgreSQL (§35).

These tests run on the project's PostgreSQL instance, because the guarantees
being asserted are database guarantees: a unique constraint deciding which of
two operators owns the canary, CHECK constraints making an impossible row
impossible, and ``SELECT ... FOR UPDATE`` serialising a stage transition.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from altegio_bot.easyweek_voucher_canary import ledger as ledger_module
from altegio_bot.easyweek_voucher_canary.ledger import (
    CLAIM_GRANTED,
    CLAIM_REFUSED_PLAN_DRIFT,
    CLAIM_REFUSED_STATE,
    STATUS_AMBIGUOUS,
    STATUS_CREATE_CLAIMED,
    STATUS_CREATE_REJECTED,
    STATUS_CREATE_UNKNOWN,
    STATUS_CREATED,
    STATUS_MANUALLY_CLEANED,
    STATUS_PAID,
    STATUS_PAY_CLAIMED,
    STATUS_PAY_REJECTED,
    STATUS_PAY_UNKNOWN,
    STATUS_REFUND_CLAIMED,
    STATUS_REFUND_REJECTED,
    STATUS_REFUND_UNKNOWN,
    STATUS_REFUNDED,
    claim_create,
    claim_pay,
    claim_refund,
    load,
    record_outcome,
)
from altegio_bot.easyweek_voucher_identity import VOUCHER_CANARY_SCOPE
from altegio_bot.models.models import EasyWeekVoucherCanaryLedger
from altegio_bot.tests.easyweek_voucher_canary_fixtures import ORDER_UUID, OTHER_UUID
from altegio_bot.utils import utcnow

CREATE_PLAN_DIGEST = "a" * 64
PAY_PLAN_DIGEST = "f" * 64
REFUND_PLAN_DIGEST = "9" * 64
TEMPLATE_DIGEST = "b" * 64
CUSTOMER_FP = "c" * 64
STAFFER_FP = "d" * 64
ACCOUNT_FP = "e" * 64
MARKER = "ewvc1-000000000000"


async def _claim_create(session_maker, **changes: Any):
    now = utcnow()
    kwargs: dict[str, Any] = {
        "create_plan_digest": CREATE_PLAN_DIGEST,
        "template_config_digest": TEMPLATE_DIGEST,
        "customer_fingerprint": CUSTOMER_FP,
        "staffer_fingerprint": STAFFER_FP,
        "account_fingerprint": ACCOUNT_FP,
        "reconciliation_marker": MARKER,
        "create_window_start": now - timedelta(minutes=10),
        "create_window_end": now + timedelta(hours=6),
    }
    kwargs.update(changes)
    return await claim_create(session_maker, **kwargs)


# How far along the stage sequence a status sits. The database refuses a row
# where a later stage was claimed before an earlier one, and a fixture fighting
# that constraint would be asserting against a row the application can never
# produce — so the helper backfills the claims a real run would have made.
_STAGE_PLAN_DIGESTS = {"pay": PAY_PLAN_DIGEST, "refund": REFUND_PLAN_DIGEST}

_STAGES_UP_TO = {
    "create_claimed": ("create",),
    "create_unknown": ("create",),
    "create_rejected": ("create",),
    "created": ("create",),
    "ambiguous": ("create",),
    "manually_cleaned": ("create",),
    "pay_claimed": ("create", "pay"),
    "pay_unknown": ("create", "pay"),
    "pay_rejected": ("create", "pay"),
    "paid": ("create", "pay"),
    "refund_claimed": ("create", "pay", "refund"),
    "refund_unknown": ("create", "pay", "refund"),
    "refund_rejected": ("create", "pay", "refund"),
    "refunded": ("create", "pay", "refund"),
}


async def _force_status(session_maker, status: str, **values: Any) -> None:
    """Put the row into *status* directly, bypassing the state machine."""
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(EasyWeekVoucherCanaryLedger).where(
                        EasyWeekVoucherCanaryLedger.canary_scope == VOUCHER_CANARY_SCOPE
                    )
                )
            ).scalar_one()
            row.status = status
            for stage in _STAGES_UP_TO.get(status, ()):
                if getattr(row, f"{stage}_claimed_at") is None:
                    setattr(row, f"{stage}_claimed_at", now)
                    setattr(row, f"{stage}_attempted_at", now)
                    if stage != "create":
                        setattr(row, f"{stage}_plan_digest", _STAGE_PLAN_DIGESTS[stage])
            for name, value in values.items():
                setattr(row, name, value)


# ---------------------------------------------------------------------------
# Claim before request
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_create_claim_is_committed_and_visible_before_any_request(session_maker) -> None:
    """A separate session sees the claim, so a crash cannot hide it."""
    assert (await load(session_maker)).exists is False

    outcome = await _claim_create(session_maker)

    assert outcome.granted is True
    assert outcome.reason == CLAIM_GRANTED

    # Read through a brand-new session: the claim really is committed.
    snapshot = await load(session_maker)
    assert snapshot.status == STATUS_CREATE_CLAIMED
    # Claimed AND attempted, together. After this commit, "the request may have
    # gone out" is the only safe reading.
    assert snapshot.stage_timestamps["create_claimed_at"] is not None
    assert snapshot.stage_timestamps["create_attempted_at"] is not None
    assert snapshot.stage_timestamps["create_verified_at"] is None


@pytest.mark.asyncio
async def test_two_simultaneous_operators_yield_exactly_one_claim(session_maker) -> None:
    first, second = await asyncio.gather(
        _claim_create(session_maker),
        _claim_create(session_maker),
    )

    assert sorted([first.granted, second.granted]) == [False, True]
    loser = first if not first.granted else second
    assert loser.reason in {CLAIM_REFUSED_STATE, CLAIM_REFUSED_PLAN_DRIFT}

    async with session_maker() as session:
        count = await session.scalar(text("SELECT count(*) FROM easyweek_voucher_canary_ledger"))
    assert count == 1


@pytest.mark.asyncio
async def test_a_second_canary_for_the_same_scope_is_impossible(session_maker) -> None:
    await _claim_create(session_maker)

    async with session_maker() as session:
        session.add(
            EasyWeekVoucherCanaryLedger(
                canary_scope=VOUCHER_CANARY_SCOPE,
                request_schema_version="1",
                create_plan_digest=CREATE_PLAN_DIGEST,
                template_config_digest=TEMPLATE_DIGEST,
                customer_fingerprint=CUSTOMER_FP,
                staffer_fingerprint=STAFFER_FP,
                account_fingerprint=ACCOUNT_FP,
                reconciliation_marker=MARKER,
                status=STATUS_CREATE_CLAIMED,
                create_window_start=utcnow(),
                create_window_end=utcnow() + timedelta(hours=1),
                evidence={},
            )
        )
        with pytest.raises(IntegrityError):
            await session.commit()


# ---------------------------------------------------------------------------
# A mutation is never repeated
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [
        STATUS_CREATE_CLAIMED,
        STATUS_CREATE_UNKNOWN,
        STATUS_CREATED,
        STATUS_PAY_CLAIMED,
        STATUS_PAY_UNKNOWN,
        STATUS_PAID,
        STATUS_REFUND_CLAIMED,
        STATUS_REFUND_UNKNOWN,
        STATUS_REFUNDED,
        STATUS_AMBIGUOUS,
        STATUS_MANUALLY_CLEANED,
    ],
)
async def test_a_create_is_never_re_claimed_from_an_unresolved_or_done_state(session_maker, status) -> None:
    await _claim_create(session_maker)
    needs_target = status not in {STATUS_CREATE_CLAIMED, STATUS_CREATE_UNKNOWN, STATUS_AMBIGUOUS}
    await _force_status(session_maker, status, **({"target_order_uuid": ORDER_UUID} if needs_target else {}))

    outcome = await _claim_create(session_maker)

    assert outcome.granted is False
    assert outcome.reason == CLAIM_REFUSED_STATE
    assert outcome.status == status


@pytest.mark.asyncio
async def test_only_a_provably_rejected_create_may_be_re_claimed(session_maker) -> None:
    """A permanent 4xx means the server declined before acting."""
    await _claim_create(session_maker)
    await _force_status(session_maker, STATUS_CREATE_REJECTED)

    outcome = await _claim_create(session_maker)

    assert outcome.granted is True
    assert (await load(session_maker)).status == STATUS_CREATE_CLAIMED


@pytest.mark.asyncio
async def test_a_re_claim_under_a_drifted_plan_is_refused(session_maker) -> None:
    await _claim_create(session_maker)
    await _force_status(session_maker, STATUS_CREATE_REJECTED)

    outcome = await _claim_create(session_maker, template_config_digest="7" * 64)

    assert outcome.granted is False
    assert outcome.reason == CLAIM_REFUSED_PLAN_DRIFT


@pytest.mark.asyncio
async def test_a_pay_is_only_claimable_from_a_proven_created_order(session_maker) -> None:
    await _claim_create(session_maker)

    # An unresolved create cannot be paid for.
    refused = await claim_pay(session_maker, pay_plan_digest=PAY_PLAN_DIGEST, template_config_digest=TEMPLATE_DIGEST)
    assert refused.granted is False

    await _force_status(session_maker, STATUS_CREATED, target_order_uuid=ORDER_UUID)
    granted = await claim_pay(session_maker, pay_plan_digest=PAY_PLAN_DIGEST, template_config_digest=TEMPLATE_DIGEST)
    assert granted.granted is True

    snapshot = await load(session_maker)
    assert snapshot.status == STATUS_PAY_CLAIMED
    assert snapshot.stage_timestamps["pay_attempted_at"] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [STATUS_PAY_CLAIMED, STATUS_PAY_UNKNOWN, STATUS_PAID, STATUS_REFUNDED])
async def test_a_pay_is_never_repeated_once_claimed(session_maker, status) -> None:
    await _claim_create(session_maker)
    await _force_status(session_maker, status, target_order_uuid=ORDER_UUID)

    outcome = await claim_pay(session_maker, pay_plan_digest=PAY_PLAN_DIGEST, template_config_digest=TEMPLATE_DIGEST)

    assert outcome.granted is False
    assert outcome.reason == CLAIM_REFUSED_STATE


@pytest.mark.asyncio
async def test_a_refund_is_only_claimable_from_a_proven_paid_order(session_maker) -> None:
    await _claim_create(session_maker)
    await _force_status(session_maker, STATUS_CREATED, target_order_uuid=ORDER_UUID)

    refused = await claim_refund(
        session_maker, refund_plan_digest=REFUND_PLAN_DIGEST, template_config_digest=TEMPLATE_DIGEST
    )
    assert refused.granted is False

    await _force_status(session_maker, STATUS_PAID, target_order_uuid=ORDER_UUID)
    granted = await claim_refund(
        session_maker, refund_plan_digest=REFUND_PLAN_DIGEST, template_config_digest=TEMPLATE_DIGEST
    )
    assert granted.granted is True
    assert (await load(session_maker)).status == STATUS_REFUND_CLAIMED


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [STATUS_REFUND_CLAIMED, STATUS_REFUND_UNKNOWN, STATUS_REFUNDED])
async def test_a_refund_is_never_repeated_once_claimed(session_maker, status) -> None:
    await _claim_create(session_maker)
    await _force_status(session_maker, status, target_order_uuid=ORDER_UUID)

    outcome = await claim_refund(
        session_maker, refund_plan_digest=REFUND_PLAN_DIGEST, template_config_digest=TEMPLATE_DIGEST
    )

    assert outcome.granted is False


CLAIM_KWARGS = {
    claim_pay: {"pay_plan_digest": PAY_PLAN_DIGEST},
    claim_refund: {"refund_plan_digest": REFUND_PLAN_DIGEST},
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,claimer",
    [(STATUS_PAY_REJECTED, claim_pay), (STATUS_REFUND_REJECTED, claim_refund)],
)
async def test_a_provably_rejected_stage_may_be_re_claimed(session_maker, status, claimer) -> None:
    """Cleanup matters more than purity: a 422 refund must not strand a payment."""
    await _claim_create(session_maker)
    await _force_status(session_maker, status, target_order_uuid=ORDER_UUID)

    outcome = await claimer(session_maker, **CLAIM_KWARGS[claimer], template_config_digest=TEMPLATE_DIGEST)

    assert outcome.granted is True


@pytest.mark.asyncio
async def test_two_simultaneous_pay_claims_yield_exactly_one(session_maker) -> None:
    await _claim_create(session_maker)
    await _force_status(session_maker, STATUS_CREATED, target_order_uuid=ORDER_UUID)

    first, second = await asyncio.gather(
        claim_pay(session_maker, pay_plan_digest=PAY_PLAN_DIGEST, template_config_digest=TEMPLATE_DIGEST),
        claim_pay(session_maker, pay_plan_digest=PAY_PLAN_DIGEST, template_config_digest=TEMPLATE_DIGEST),
    )

    assert sorted([first.granted, second.granted]) == [False, True]


@pytest.mark.asyncio
async def test_a_stage_cannot_be_claimed_before_the_row_exists(session_maker) -> None:
    outcome = await claim_pay(session_maker, pay_plan_digest=PAY_PLAN_DIGEST, template_config_digest=TEMPLATE_DIGEST)

    assert outcome.granted is False
    assert outcome.status is None


# ---------------------------------------------------------------------------
# Recording outcomes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_evidence_is_merged_so_a_later_stage_never_erases_an_earlier_one(session_maker) -> None:
    await _claim_create(session_maker)
    await record_outcome(
        session_maker,
        status=STATUS_CREATED,
        expected_statuses=frozenset({STATUS_CREATE_CLAIMED}),
        target_order_uuid=ORDER_UUID,
        verified_field="create_verified_at",
        evidence={"create_response": {"stage": "create_response"}},
    )
    await _force_status(session_maker, STATUS_PAY_CLAIMED)
    result = await record_outcome(
        session_maker,
        status=STATUS_PAID,
        expected_statuses=frozenset({STATUS_PAY_CLAIMED}),
        verified_field="pay_verified_at",
        evidence={"pay_response": {"stage": "pay_response"}},
    )

    assert result.applied is True
    assert set(result.snapshot.evidence) == {"create_response", "pay_response"}


@pytest.mark.asyncio
async def test_an_unknown_status_is_refused_by_the_module_not_only_the_database(session_maker) -> None:
    await _claim_create(session_maker)
    with pytest.raises(ValueError):
        await record_outcome(
            session_maker, status="something_else", expected_statuses=frozenset({STATUS_CREATE_CLAIMED})
        )


@pytest.mark.asyncio
async def test_the_target_order_uuid_is_the_only_identifier_the_snapshot_exposes(session_maker) -> None:
    await _claim_create(session_maker)
    await record_outcome(
        session_maker,
        status=STATUS_CREATED,
        expected_statuses=frozenset({STATUS_CREATE_CLAIMED}),
        target_order_uuid=ORDER_UUID,
        verified_field="create_verified_at",
    )
    safe = (await load(session_maker)).as_safe_dict()

    # The safe projection says only that it is KNOWN.
    assert safe["target_order_uuid_known"] is True
    assert ORDER_UUID not in repr(safe)
    for forbidden in (CUSTOMER_FP[:8], "customer_uuid", "staffer_uuid", "account_uuid"):
        assert forbidden not in repr(safe) or forbidden.endswith("_fp")


# ---------------------------------------------------------------------------
# CHECK constraints
# ---------------------------------------------------------------------------


def _row(**changes: Any) -> EasyWeekVoucherCanaryLedger:
    now = utcnow()
    values: dict[str, Any] = {
        "canary_scope": VOUCHER_CANARY_SCOPE,
        "request_schema_version": "1",
        "create_plan_digest": CREATE_PLAN_DIGEST,
        "template_config_digest": TEMPLATE_DIGEST,
        "customer_fingerprint": CUSTOMER_FP,
        "staffer_fingerprint": STAFFER_FP,
        "account_fingerprint": ACCOUNT_FP,
        "reconciliation_marker": MARKER,
        "status": STATUS_CREATE_CLAIMED,
        "create_window_start": now,
        "create_window_end": now + timedelta(hours=1),
        "evidence": {},
    }
    values.update(changes)
    return EasyWeekVoucherCanaryLedger(**values)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        # Unknown status.
        {"status": "invented_state"},
        # A post-create state with no order to point at.
        {"status": STATUS_CREATED},
        {"status": STATUS_PAID},
        {"status": STATUS_REFUNDED},
        # An attempt with no claim in front of it.
        {"create_attempted_at": utcnow(), "create_claimed_at": None},
        # A verification with no attempt behind it.
        {
            "status": STATUS_CREATED,
            "target_order_uuid": ORDER_UUID,
            "create_claimed_at": utcnow(),
            "create_verified_at": utcnow(),
        },
        # A later stage claimed before an earlier one.
        {"status": STATUS_CREATE_CLAIMED, "pay_claimed_at": utcnow(), "create_claimed_at": None},
        # Digest and fingerprint lengths.
        {"create_plan_digest": "short"},
        {"customer_fingerprint": "short"},
        {"template_config_digest": "short"},
        # An inverted reconciliation window.
        {"create_window_start": utcnow() + timedelta(hours=2)},
    ],
)
async def test_an_impossible_row_is_impossible(session_maker, changes) -> None:
    async with session_maker() as session:
        session.add(_row(**changes))
        with pytest.raises((IntegrityError, DBAPIError)):
            await session.commit()


@pytest.mark.asyncio
async def test_a_complete_row_is_accepted(session_maker) -> None:
    now = utcnow()
    async with session_maker() as session:
        session.add(
            _row(
                status=STATUS_REFUNDED,
                target_order_uuid=ORDER_UUID,
                create_claimed_at=now,
                create_attempted_at=now,
                create_verified_at=now,
                pay_plan_digest=PAY_PLAN_DIGEST,
                pay_claimed_at=now,
                pay_attempted_at=now,
                pay_verified_at=now,
                refund_plan_digest=REFUND_PLAN_DIGEST,
                refund_claimed_at=now,
                refund_attempted_at=now,
                refund_verified_at=now,
            )
        )
        await session.commit()

    assert (await load(session_maker)).status == STATUS_REFUNDED


@pytest.mark.asyncio
async def test_the_state_vocabulary_matches_the_database_constraint(session_maker) -> None:
    """Every status the module can write must be a status the table accepts."""
    for status in ledger_module.ALL_STATUSES:
        needs_target = status not in {
            STATUS_CREATE_CLAIMED,
            STATUS_CREATE_UNKNOWN,
            STATUS_CREATE_REJECTED,
            STATUS_AMBIGUOUS,
        }
        async with session_maker() as session:
            async with session.begin():
                await session.execute(text("DELETE FROM easyweek_voucher_canary_ledger"))
                session.add(
                    _row(
                        status=status,
                        target_order_uuid=ORDER_UUID if needs_target else None,
                        create_claimed_at=utcnow(),
                        create_attempted_at=utcnow(),
                        pay_plan_digest=PAY_PLAN_DIGEST,
                        pay_claimed_at=utcnow(),
                        pay_attempted_at=utcnow(),
                        refund_plan_digest=REFUND_PLAN_DIGEST,
                        refund_claimed_at=utcnow(),
                        refund_attempted_at=utcnow(),
                    )
                )


@pytest.mark.asyncio
async def test_manual_cleanup_is_recorded_as_observed_not_as_our_rollback(session_maker) -> None:
    await _claim_create(session_maker)
    await record_outcome(
        session_maker,
        status=STATUS_CREATED,
        expected_statuses=frozenset({STATUS_CREATE_CLAIMED}),
        target_order_uuid=ORDER_UUID,
        verified_field="create_verified_at",
    )
    result = await record_outcome(
        session_maker,
        status=STATUS_MANUALLY_CLEANED,
        expected_statuses=frozenset({STATUS_CREATED}),
        manual_cleanup_observed=True,
    )
    snapshot = result.snapshot

    assert snapshot.status == STATUS_MANUALLY_CLEANED
    assert snapshot.manual_cleanup_observed_at is not None
    # The refund stage never ran, and the ledger does not pretend it did.
    assert snapshot.stage_timestamps["refund_attempted_at"] is None
    assert snapshot.stage_timestamps["refund_verified_at"] is None


@pytest.mark.asyncio
async def test_a_foreign_order_uuid_is_still_just_one_identifier(session_maker) -> None:
    await _claim_create(session_maker)
    result = await record_outcome(
        session_maker,
        status=STATUS_CREATED,
        expected_statuses=frozenset({STATUS_CREATE_CLAIMED}),
        target_order_uuid=OTHER_UUID,
        verified_field="create_verified_at",
    )
    assert result.snapshot.target_order_uuid == OTHER_UUID
    assert OTHER_UUID not in repr(result.snapshot.as_safe_dict())
