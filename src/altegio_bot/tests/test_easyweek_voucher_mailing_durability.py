"""Crash, restart, concurrency and migration, on PostgreSQL 16 (§43.5, group D).

The property every test here defends is the one that cannot be recovered by
apologising afterwards: an external request that may have gone out must never be
sent again. So the question is always the same — after this failure, can anything
retry? — and the answer has to be no, with the state readable and a human in the
loop.

Everything runs against a real PostgreSQL: the row locks, the unique constraints,
the ``FOR UPDATE SKIP LOCKED`` claim and the CHECKs are the subject, not an
implementation detail these tests work around.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import uuid as uuid_module
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import select, text

from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production import operations as operations_module
from altegio_bot.easyweek_voucher_mutation import (
    EasyWeekVoucherMutationUnknown,
    VoucherMutationResponse,
)
from altegio_bot.models.models import (
    EasyWeekVoucherProductionBatchItem,
    EasyWeekVoucherProductionOperation,
)
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    ORDER_UUIDS,
    FakeMutator,
    FakeReader,
    marker_orders,
    seed_production_preview,
    seed_template_and_sender,
)
from altegio_bot.utils import utcnow
from altegio_bot.workers import easyweek_voucher_production_worker as worker_module

PLAN_URL = "/ops/voucher-mailings/api/plan"
CONFIRM_URL = "/ops/voucher-mailings/api/confirm"
STOP_URL = "/ops/voucher-mailings/api/stop"


def _ok(index: int) -> VoucherMutationResponse:
    return VoucherMutationResponse(http_status=200, envelope={"uuid": ORDER_UUIDS[index]})


async def _offer(client, **payload: Any) -> dict[str, Any]:
    answer = (await client.post(PLAN_URL, json=payload)).json()
    assert answer["ready"], answer["reasons"]
    return answer


async def _confirm(client, offer: dict[str, Any]) -> dict[str, Any]:
    response = await client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


async def _frozen(client, session_maker, transports, *, count: int) -> tuple[int, int, FakeReader]:
    run_id, _ = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    transports.use(reader=reader)
    offer = await _offer(
        client,
        stage="freeze",
        preview_run_id=run_id,
        expected_recipient_count=count,
        approved_exposure_minor=count * 1500,
    )
    await _confirm(client, offer)
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None
    snapshot = await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)
    batch_id = int(snapshot.batch_id or 0)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    return run_id, batch_id, reader


async def _items(session_maker, batch_id: int) -> dict[int, EasyWeekVoucherProductionBatchItem]:
    async with session_maker() as session:
        rows = list(
            (
                await session.execute(
                    select(EasyWeekVoucherProductionBatchItem).where(
                        EasyWeekVoucherProductionBatchItem.batch_id == batch_id
                    )
                )
            )
            .scalars()
            .all()
        )
    return {int(row.slot): row for row in rows}


# ===========================================================================
# Concurrent confirmations and concurrent executors
# ===========================================================================


async def test_many_concurrent_confirmations_of_one_offer_make_one_operation(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """Five simultaneous POSTs of the same approval. The unique constraint decides."""
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    body = {
        "approval_id": offer["approval"]["approval_id"],
        "confirmed_count": offer["targets"]["stage_target_count"],
        "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
    }

    responses = await asyncio.gather(
        *[ui_client.post(CONFIRM_URL, json=body) for _ in range(5)], return_exceptions=True
    )
    ok = [r for r in responses if not isinstance(r, BaseException) and r.status_code == 200]
    assert ok, responses
    created_flags = [r.json()["created"] for r in ok]
    # At most one call reports that it was the one that created the operation.
    assert created_flags.count(True) <= 1, created_flags
    operations = await operations_module.list_operations(session_maker, batch_id=batch_id)
    assert len([entry for entry in operations if entry.stage == "create"]) == 1


async def test_two_executors_cannot_claim_the_same_operation(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """``FOR UPDATE SKIP LOCKED`` plus a compare-and-set on the status.

    The supported topology has one executor. A mis-deploy produces two, and the
    database — not a convention — is what stops them both running one stage.
    """
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)

    first, second = await asyncio.gather(
        operations_module.claim_next_operation(session_maker, owner="executor-a"),
        operations_module.claim_next_operation(session_maker, owner="executor-b"),
    )
    claimed = [entry for entry in (first, second) if entry is not None]
    assert len(claimed) == 1, (first, second)
    assert claimed[0].attempts == 1


async def test_two_concurrent_create_claims_on_one_slot_grant_once(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """The per-item claim, raced. One grant, so one possible external request."""
    count = 1
    _run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=count)
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    from altegio_bot.campaigns.easyweek_voucher_production.runner import _identity_from_snapshot

    identity = _identity_from_snapshot(snapshot)
    assert identity is not None
    now = utcnow()

    first, second = await asyncio.gather(
        ledger_module.claim_create(
            session_maker,
            identity=identity,
            batch_id=batch_id,
            slot=1,
            plan_digest="a" * 64,
            create_window_start=now - timedelta(minutes=5),
            create_window_end=now + timedelta(minutes=5),
        ),
        ledger_module.claim_create(
            session_maker,
            identity=identity,
            batch_id=batch_id,
            slot=1,
            plan_digest="b" * 64,
            create_window_start=now - timedelta(minutes=5),
            create_window_end=now + timedelta(minutes=5),
        ),
    )
    granted = [outcome for outcome in (first, second) if outcome.granted]
    assert len(granted) == 1, (first, second)


async def test_a_stop_racing_a_claim_never_lets_one_more_slot_through(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """The stop and the claim both take the header lock, so they serialise.

    Run many times over, because a window this narrow is exactly the kind a single
    lucky pass would hide. Whichever order PostgreSQL picks, the result must be one
    of two consistent answers — never "the stop is active AND a later slot was
    claimed".
    """
    count = 6
    _run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=count)
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    from altegio_bot.campaigns.easyweek_voucher_production.runner import _identity_from_snapshot

    identity = _identity_from_snapshot(snapshot)
    assert identity is not None
    now = utcnow()

    async def claim(slot: int):
        return await ledger_module.claim_create(
            session_maker,
            identity=identity,
            batch_id=batch_id,
            slot=slot,
            plan_digest="c" * 64,
            create_window_start=now - timedelta(minutes=5),
            create_window_end=now + timedelta(minutes=5),
        )

    stop_task = asyncio.create_task(ledger_module.request_stop(session_maker, batch_id=batch_id, requested_by="ops"))
    claims = await asyncio.gather(*[claim(slot) for slot in range(1, count + 1)])
    await stop_task

    # Every refusal after the stop landed is a STOP refusal, not a state one, and
    # no claim was granted once the stop was durable.
    assert (await ledger_module.stop_state(session_maker, batch_id=batch_id)).active
    rows = await _items(session_maker, batch_id)
    for outcome, slot in zip(claims, range(1, count + 1), strict=True):
        row = rows[slot]
        if outcome.granted:
            assert row.create_claimed_at is not None
        else:
            # Not claimed means not attempted: no stamp, and nothing in doubt.
            assert row.create_claimed_at is None
            assert row.reconciliation_required is False
    # And from now on nothing more can be claimed at all. The reason may be either
    # refusal and both are correct: a granted claim leaves its slot unresolved,
    # which halts the header in the same transaction, and a halt outranks a stop.
    # What must never happen is a GRANT.
    for slot in (1, count):
        outcome = await claim(slot)
        assert outcome.granted is False
        assert outcome.reason in (
            ledger_module.CLAIM_REFUSED_STOPPED,
            ledger_module.CLAIM_REFUSED_HALTED,
            ledger_module.CLAIM_REFUSED_STATE,
        ), outcome


# ===========================================================================
# Crash, at each of the four moments that matter
# ===========================================================================


async def test_a_crash_before_any_claim_leaves_nothing_to_recover(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """The executor dies before it reaches the first slot. No claim, no doubt."""
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    body = await _confirm(ui_client, offer)
    claimed = await operations_module.claim_next_operation(session_maker, owner="dying-executor")
    assert claimed is not None

    # The process vanishes here. A new executor starts.
    interrupted = await operations_module.interrupt_abandoned(session_maker, include_all_running=True)
    assert [entry.id for entry in interrupted] == [body["operation"]["operation_id"]]

    rows = await _items(session_maker, batch_id)
    assert all(row.create_claimed_at is None for row in rows.values())
    assert all(row.reconciliation_required is False for row in rows.values())
    # Terminal, and never picked up again.
    assert await operations_module.claim_next_operation(session_maker, owner="new-executor") is None


async def test_a_crash_after_a_claim_reads_as_may_have_happened(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """The claim is committed before the request leaves, so it is the durable trace.

    A crash between the commit and the socket and a crash after the response are
    indistinguishable afterwards — and both must read as "it may have gone out".
    """
    count = 3
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)

    class DyingMutator(FakeMutator):
        """The process dies while slot 2's request is in flight."""

        async def create_voucher_order(self, **kwargs: Any) -> Any:
            if len(self.create_calls) == 1:
                self.create_calls.append(kwargs)
                self.calls.append("create")
                raise EasyWeekVoucherMutationUnknown("the process died mid-request")
            return await super().create_voucher_order(**kwargs)

    mutator = DyingMutator(create_sequence=[_ok(0), _ok(1), _ok(2)])
    transports.use(reader=reader, mutator=mutator)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)
    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None

    rows = await _items(session_maker, batch_id)
    assert rows[1].status == "created"
    # Slot 2's outcome is unknown and stays unknown.
    assert rows[2].status == "create_unknown"
    assert rows[2].reconciliation_required is True
    assert rows[2].manual_cleanup_required is True
    # Slot 3 was never attempted — the suffix stopped.
    assert rows[3].status == "planned"
    assert rows[3].create_claimed_at is None
    # The batch is halted, and the operation is not retryable.
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.halted is True
    assert await operations_module.claim_next_operation(session_maker, owner="again") is None


async def test_a_crash_after_the_external_effect_but_before_the_answer_is_not_retried(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """The worst moment: the voucher exists, and the process never wrote it down.

    What makes this survivable is that the claim went in FIRST. The slot reads
    ``create_claimed`` — not ``planned`` — so nothing may claim it again, and the
    operation is interrupted rather than queued.
    """
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)
    claimed = await operations_module.claim_next_operation(session_maker, owner="dying")
    assert claimed is not None

    # Slot 1's claim is committed, the POST went out, and the process died before
    # recording the answer. Written directly, because the real sequence is exactly
    # "the row is committed and then nothing else happens".
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    from altegio_bot.campaigns.easyweek_voucher_production.runner import _identity_from_snapshot

    identity = _identity_from_snapshot(snapshot)
    assert identity is not None
    now = utcnow()
    granted = await ledger_module.claim_create(
        session_maker,
        identity=identity,
        batch_id=batch_id,
        slot=1,
        plan_digest=offer["approval"].get("plan_digest", "d" * 64),
        create_window_start=now - timedelta(minutes=5),
        create_window_end=now + timedelta(minutes=5),
    )
    assert granted.granted

    interrupted = await operations_module.interrupt_abandoned(session_maker, include_all_running=True)
    assert len(interrupted) == 1
    assert interrupted[0].status == "interrupted"
    assert interrupted[0].outcome_code == "voucher_production_execution_interrupted"

    rows = await _items(session_maker, batch_id)
    # NOT reset to planned. The one reading that cannot create a second voucher.
    assert rows[1].status == "create_claimed"
    assert rows[1].reconciliation_required is True
    # Nothing is queued, so nothing can be retried.
    assert await operations_module.claim_next_operation(session_maker, owner="new") is None
    # And a fresh plan refuses, because the batch is halted by the claim.
    transports.use(reader=reader)
    blocked = (
        await ui_client.post(PLAN_URL, json={"stage": "create", "preview_run_id": run_id, "batch_id": batch_id})
    ).json()
    assert blocked["ready"] is False
    assert "voucher_production_batch_halted" in blocked["reasons"]


async def test_a_restart_never_returns_an_operation_to_the_queue(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """There is no path from ``running`` or ``interrupted`` back to ``queued``.

    Asserted over the whole status vocabulary rather than one case, because a future
    "helpful" requeue is exactly the change this guards against.
    """
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)
    assert await operations_module.claim_next_operation(session_maker, owner="first") is not None

    for _ in range(3):
        await worker_module.run_worker(session_maker, owner="restarted", poll_sec=0.01, max_iterations=1)
        async with session_maker() as session:
            statuses = list((await session.execute(select(EasyWeekVoucherProductionOperation.status))).scalars().all())
        assert "queued" not in statuses, statuses
        assert "interrupted" in statuses, statuses


async def test_an_expired_lease_interrupts_rather_than_retries(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """A worker killed between the claim and the next heartbeat."""
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)
    claimed = await operations_module.claim_next_operation(session_maker, owner="killed", lease=timedelta(seconds=-1))
    assert claimed is not None

    swept = await operations_module.interrupt_abandoned(session_maker)
    assert [entry.status for entry in swept] == ["interrupted"]
    assert await operations_module.claim_next_operation(session_maker, owner="next") is None


async def test_a_live_executor_is_not_swept(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """A slow stage must not be called abandoned while its worker is renewing."""
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)
    claimed = await operations_module.claim_next_operation(session_maker, owner="busy", lease=timedelta(minutes=10))
    assert claimed is not None

    assert await operations_module.interrupt_abandoned(session_maker) == []
    assert await operations_module.renew_lease(session_maker, operation_id=claimed.id, owner="busy") is True
    # A different owner cannot renew it.
    assert await operations_module.renew_lease(session_maker, operation_id=claimed.id, owner="other") is False
    assert await operations_module.interrupt_abandoned(session_maker) == []


async def test_a_finished_operation_is_never_overwritten(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """A late writer must not replace an interrupted row with a tidier-looking one."""
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    body = await _confirm(ui_client, offer)
    operation_id = body["operation"]["operation_id"]
    await operations_module.claim_next_operation(session_maker, owner="dying")
    await operations_module.interrupt_abandoned(session_maker, include_all_running=True)

    late = await operations_module.finish_operation(
        session_maker,
        operation_id=operation_id,
        owner="dying",
        status="completed",
        outcome_code="applied",
    )
    assert late is not None
    assert late.status == "interrupted"
    assert late.outcome_code == "voucher_production_execution_interrupted"


async def test_approvals_and_audit_survive_a_crash(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """The record of who authorised what is not lost with the process that ran it."""
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)
    await operations_module.claim_next_operation(session_maker, owner="dying")
    await operations_module.interrupt_abandoned(session_maker, include_all_running=True)

    approval = await operations_module.load_approval(session_maker, approval_id=offer["approval"]["approval_id"])
    assert approval is not None
    assert approval.status == "consumed"
    assert approval.target_slots == (1,)
    async with session_maker() as session:
        audit = list(
            (
                await session.execute(
                    text("SELECT action, principal FROM easyweek_voucher_production_audit ORDER BY id")
                )
            ).all()
        )
    actions = [row[0] for row in audit]
    assert "plan" in actions and "confirm" in actions
    assert {row[1] for row in audit} == {"ops-operator"}


# ===========================================================================
# Schema guarantees
# ===========================================================================


async def test_the_database_refuses_a_second_operation_for_one_approval(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """Not the button, not the handler: the constraint."""
    from sqlalchemy.exc import IntegrityError

    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    body = await _confirm(ui_client, offer)
    existing = await operations_module.load_operation(session_maker, operation_id=body["operation"]["operation_id"])
    assert existing is not None

    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                session.add(
                    EasyWeekVoucherProductionOperation(
                        approval_id=offer["approval"]["approval_id"],
                        batch_scope="easyweek_voucher_production_mailing_v1",
                        provider="easyweek",
                        company_id=322579,
                        stage="create",
                        campaign_run_id=run_id,
                        batch_id=batch_id,
                        principal="ops-operator",
                        session_fingerprint="f" * 64,
                        identification_limit="shared_ops_account",
                        status="queued",
                        queued_at=utcnow(),
                    )
                )


async def test_a_queued_operation_cannot_hold_a_lease(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """A CHECK, so a sweep can never mistake a waiting row for an abandoned one."""
    from sqlalchemy.exc import IntegrityError

    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    body = await _confirm(ui_client, offer)

    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                row = await session.get(EasyWeekVoucherProductionOperation, body["operation"]["operation_id"])
                assert row is not None
                row.lease_owner = "somebody"
                row.lease_expires_at = utcnow()


async def test_a_stop_row_cannot_exist_twice_for_one_batch(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    from sqlalchemy.exc import IntegrityError

    from altegio_bot.models.models import EasyWeekVoucherProductionStopRequest

    _run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=1)
    await ledger_module.request_stop(session_maker, batch_id=batch_id, requested_by="ops")

    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                session.add(
                    EasyWeekVoucherProductionStopRequest(batch_id=batch_id, requested_by="ops", requested_at=utcnow())
                )


async def test_an_approval_cannot_cover_no_slots(session_maker):
    """An empty slot list is what an executor could read as "no restriction"."""
    from sqlalchemy.exc import IntegrityError

    from altegio_bot.models.models import EasyWeekVoucherProductionApproval

    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                session.add(
                    EasyWeekVoucherProductionApproval(
                        batch_scope="easyweek_voucher_production_mailing_v1",
                        request_schema_version="1",
                        provider="easyweek",
                        company_id=322579,
                        stage="create",
                        principal="ops",
                        session_fingerprint="f" * 64,
                        identification_limit="shared_ops_account",
                        campaign_run_id=1,
                        batch_id=None,
                        target_slots=[],
                        target_slot_count=0,
                        stage_target_count=1,
                        stage_amount_minor=1500,
                        batch_recipient_count=1,
                        batch_exposure_minor=1500,
                        campaign_period_start=utcnow(),
                        campaign_period_end=utcnow() + timedelta(days=1),
                        plan_digest="a" * 64,
                        plan_issued_at=utcnow(),
                        expires_at=utcnow() + timedelta(minutes=30),
                        issuer_pinned=True,
                        issuer_membership_proven=True,
                        runtime_identity_bound=True,
                        baseline_version="x",
                        frozen_digest=None,
                        status="pending",
                    )
                )


async def test_an_approval_expiry_must_be_after_its_issue(session_maker):
    """Queue time is not reading time, and a window has to be a window."""
    from sqlalchemy.exc import IntegrityError

    from altegio_bot.models.models import EasyWeekVoucherProductionApproval

    moment = utcnow()
    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                session.add(
                    EasyWeekVoucherProductionApproval(
                        batch_scope="easyweek_voucher_production_mailing_v1",
                        request_schema_version="1",
                        provider="easyweek",
                        company_id=322579,
                        stage="freeze",
                        principal="ops",
                        session_fingerprint="f" * 64,
                        identification_limit="shared_ops_account",
                        campaign_run_id=1,
                        batch_id=None,
                        target_slots=[1],
                        target_slot_count=1,
                        stage_target_count=1,
                        stage_amount_minor=1500,
                        batch_recipient_count=1,
                        batch_exposure_minor=1500,
                        campaign_period_start=moment,
                        campaign_period_end=moment + timedelta(days=1),
                        plan_digest="a" * 64,
                        plan_issued_at=moment,
                        expires_at=moment - timedelta(minutes=1),
                        issuer_pinned=True,
                        issuer_membership_proven=True,
                        runtime_identity_bound=True,
                        baseline_version="x",
                        frozen_digest=None,
                        status="pending",
                    )
                )


# ===========================================================================
# Migration: upgrade, downgrade, re-upgrade, one head
# ===========================================================================

# Set this to "1" to turn "this environment cannot create a disposable database"
# from a skip into a failure, exactly as ALTEGIO_REQUIRE_MIGTEST does for the §42
# migration suite. The required workflow sets it: this module is one of the dedicated
# gates in docs/ops/test_suite_tiers.md, so a machine without a usable PostgreSQL
# fails the gate rather than reporting a green skip for the migration cycle.
#
# The invariant that must never be able to skip — exactly one Alembic head — does
# not use this fixture at all and runs unconditionally in the rest shard.
REQUIRE_MIGRATION = os.getenv("ALTEGIO_REQUIRE_VOUCHER_MAILING_MIGTEST") == "1"

# The revision these four tables were created ON TOP OF. Named, so "undo the
# migration that created them" keeps meaning that however many revisions follow.
OPERATIONS_PARENT_REVISION = "e2c7b4f16a83"


def _alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    """Run alembic against one database.

    ``heads`` needs no database and is given the repo path as a harmless value, so
    that the single-head check can run with no server at all.
    """
    env = dict(os.environ)
    env["DATABASE_URL"] = database_url
    return subprocess.run(  # noqa: S603 - fixed command, generated args
        ["uv", "run", "alembic", *args],
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
        cwd=_repo_root(),
    )


def _repo_root() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    for _ in range(6):
        if os.path.exists(os.path.join(here, "alembic.ini")):
            return here
        here = os.path.dirname(here)
    raise AssertionError("alembic.ini not found above the test file")


@pytest.fixture
async def disposable_database(session_maker) -> Any:
    """A throwaway database for the migration tests, or a skip.

    Created next to the configured test database, through the server the suite is
    already connected to, and dropped afterwards. Never the suite's own database:
    these tests run DDL from scratch.
    """
    from altegio_bot.settings import Settings

    base = Settings().database_url
    name = f"altegio_mig_{uuid_module.uuid4().hex[:12]}"
    admin_url = base.rsplit("/", 1)[0] + "/postgres"
    target = base.rsplit("/", 1)[0] + "/" + name

    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            await conn.execute(text(f'CREATE DATABASE "{name}"'))
    except Exception as exc:  # noqa: BLE001 - the environment, not the code
        await engine.dispose()
        if REQUIRE_MIGRATION:
            raise
        pytest.skip(f"cannot create a disposable database: {exc}")
    try:
        yield target
    finally:
        async with engine.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        await engine.dispose()


def test_the_chain_has_exactly_one_head():
    """A second head is a deployment that cannot be upgraded.

    Deliberately NOT behind the disposable-database fixture. ``alembic heads``
    reads the revision files and needs no database at all, and this is the one
    invariant that must never be able to skip: a second head is caught here, in
    the required gate, rather than by a deploy that stops half-way.
    """
    result = _alembic(_repo_root(), "heads")
    assert result.returncode == 0, result.stderr
    heads = [line for line in result.stdout.splitlines() if "(head)" in line]
    assert len(heads) == 1, result.stdout


async def test_upgrade_downgrade_and_re_upgrade_on_an_empty_database(disposable_database: str):
    """The new tables appear, disappear and reappear, and the head stays single.

    Addressed by REVISION rather than by "head" and "-1", for the reason §42's own
    migration tests had to learn the hard way: a later revision — including this
    PR's own follow-up — makes "one step back" mean something else, and a test that
    said "-1" would then be checking a different migration than the one it names.
    """
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0

    new_tables = (
        "easyweek_voucher_production_approvals",
        "easyweek_voucher_production_operations",
        "easyweek_voucher_production_stop_requests",
        "easyweek_voucher_production_audit",
    )
    assert await _tables(disposable_database, new_tables) == set(new_tables)

    # Back past the revision that created them, by name.
    assert _alembic(disposable_database, "downgrade", OPERATIONS_PARENT_REVISION).returncode == 0
    assert await _tables(disposable_database, new_tables) == set()
    # The §42 tables are untouched by the downgrade.
    historical = (
        "easyweek_voucher_production_batches",
        "easyweek_voucher_production_batch_items",
        "easyweek_voucher_production_batch_attempts",
        "easyweek_voucher_snapshot_batches",
        "easyweek_voucher_canary_ledger",
    )
    assert await _tables(disposable_database, historical) == set(historical)

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert await _tables(disposable_database, new_tables) == set(new_tables)
    current = _alembic(disposable_database, "current")
    assert current.returncode == 0
    assert "(head)" in current.stdout
    # The stop generation the review's R1 fix added is part of the upgraded schema.
    assert await _columns(disposable_database, "easyweek_voucher_production_approvals") >= {"stop_generation_at_plan"}


async def test_a_downgrade_keeps_historical_ledger_rows(disposable_database: str):
    """An upgrade/downgrade cycle over a populated historical ledger loses nothing.

    The §36 canary ledger is used because it is the oldest voucher ledger in the
    chain and has no foreign keys of its own, so one row can be written as the old
    phase would have written it. Its columns are spelled out here rather than built
    from the ORM: the point is that a REAL historical row survives, and a row built
    from today's model would prove only that today's model round-trips.
    """
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0

    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(disposable_database)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO easyweek_voucher_canary_ledger ("
                    " canary_scope, request_schema_version, template_config_digest,"
                    " create_plan_digest, customer_fingerprint, staffer_fingerprint,"
                    " account_fingerprint, reconciliation_marker, status,"
                    " create_claimed_at, create_attempted_at,"
                    " create_window_start, create_window_end"
                    ") VALUES ("
                    " :scope, '1', :digest, :digest, :fp, :fp, :fp, :marker, 'create_claimed',"
                    " now() - interval '30 minutes', now() - interval '30 minutes',"
                    " now() - interval '1 hour', now() + interval '1 hour')"
                ),
                {
                    "scope": "historical_canary_v1",
                    "digest": "a" * 64,
                    "fp": "b" * 64,
                    "marker": "historical-" + uuid_module.uuid4().hex[:10],
                },
            )
        async with engine.connect() as conn:
            before = (
                await conn.execute(text("SELECT count(*), max(canary_scope) FROM easyweek_voucher_canary_ledger"))
            ).one()
        assert before[0] == 1 and before[1] == "historical_canary_v1"

        # Down and back up again. The historical row is not the new migration's to
        # touch, in either direction.
        assert _alembic(disposable_database, "downgrade", OPERATIONS_PARENT_REVISION).returncode == 0
        async with engine.connect() as conn:
            midway = (await conn.execute(text("SELECT count(*) FROM easyweek_voucher_canary_ledger"))).scalar_one()
        assert midway == 1

        assert _alembic(disposable_database, "upgrade", "head").returncode == 0
        async with engine.connect() as conn:
            after = (
                await conn.execute(text("SELECT count(*), max(canary_scope) FROM easyweek_voucher_canary_ledger"))
            ).one()
        assert after == before
    finally:
        await engine.dispose()


async def test_upgrading_a_database_that_already_holds_a_production_batch(
    disposable_database: str,
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """The §42 tables the new foreign keys point at may already have rows.

    A real deployment upgrades a database with production batches in it, so the
    migration is applied here to a schema that holds one — written through the UI,
    with its campaign run, its recipients and its slots — and the batch has to come
    out of the cycle unchanged.
    """
    # Build a real batch in the suite's own database first, then prove the same
    # migration sequence applies cleanly to a schema shaped like that one.
    _run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=2)
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.exists and snapshot.recipient_count == 2

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert _alembic(disposable_database, "downgrade", OPERATIONS_PARENT_REVISION).returncode == 0
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    current = _alembic(disposable_database, "current")
    assert current.returncode == 0 and "(head)" in current.stdout

    # And the batch in the live database is untouched by any of it.
    after = await ledger_module.load(session_maker, batch_id=batch_id)
    assert after.recipient_count == snapshot.recipient_count
    assert after.frozen_digest == snapshot.frozen_digest
    assert after.staffer_uuid == snapshot.staffer_uuid


async def _columns(database_url: str, table: str) -> set[str]:
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as conn:
            rows = (
                await conn.execute(
                    text("SELECT column_name FROM information_schema.columns WHERE table_name = :t"),
                    {"t": table},
                )
            ).all()
        return {row[0] for row in rows}
    finally:
        await engine.dispose()


async def _tables(database_url: str, names: tuple[str, ...]) -> set[str]:
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as conn:
            rows = (
                await conn.execute(
                    text("SELECT tablename FROM pg_tables WHERE tablename = ANY(:names)"),
                    {"names": list(names)},
                )
            ).all()
        return {row[0] for row in rows}
    finally:
        await engine.dispose()


# ===========================================================================
# The rollback the runbook actually documents (review F3)
# ===========================================================================

RUNBOOK = os.path.join(_repo_root(), "docs", "easyweek", "VOUCHER_PRODUCTION_MAILING_RUNBOOK.md")

# The revision the stop-generation fix was added ON TOP OF: one step back from the
# head, and the target of a rollback of the FIX alone.
STOP_GENERATION_PARENT_REVISION = "a4f1c9d26b70"
STOP_GENERATION_COLUMN = "stop_generation_at_plan"
STOP_GENERATION_CHECK = "ck_ew_voucher_production_approval_stop_gen"

PHASE_TABLES = (
    "easyweek_voucher_production_approvals",
    "easyweek_voucher_production_operations",
    "easyweek_voucher_production_stop_requests",
    "easyweek_voucher_production_audit",
)
HISTORICAL_TABLES = (
    "easyweek_voucher_production_batches",
    "easyweek_voucher_production_batch_items",
    "easyweek_voucher_production_batch_attempts",
    "easyweek_voucher_snapshot_batches",
    "easyweek_voucher_canary_ledger",
)


def _rollback_section() -> str:
    text_body = open(RUNBOOK, encoding="utf-8").read()
    assert "### 12.2 Rollback" in text_body, "the runbook has no rollback section"
    return text_body.split("### 12.2 Rollback", 1)[1].split("\n## ", 1)[0]


def _rollback_commands() -> list[str]:
    """The COMMANDS of the rollback section, without the prose that explains them.

    The prose names ``alembic downgrade -1`` on purpose — to say that it is not what
    the section used to claim it was — so a scan of the whole text would flag the very
    sentence that fixes the defect.
    """
    inside = False
    commands: list[str] = []
    for line in _rollback_section().splitlines():
        if line.startswith("```"):
            inside = line.startswith("```bash")
            continue
        if inside and line.strip():
            commands.append(line.strip())
    return commands


def _documented_downgrade_targets() -> dict[str, str]:
    """The revisions the runbook tells an administrator to downgrade TO.

    Read out of the document rather than written here, because the point of these
    tests is that the documented command does what the document says it does. A test
    with its own hard-coded revision would stay green while the runbook drifted — and
    that drift is exactly the defect under repair.
    """
    import re as re_module

    scope = None
    found: dict[str, str] = {}
    for line in _rollback_section().splitlines():
        if line.startswith("Scope A"):
            scope = "A"
        elif line.startswith("Scope B"):
            scope = "B"
        match = re_module.search(r"alembic downgrade (\S+)", line)
        if match and scope is not None:
            found.setdefault(scope, match.group(1))
    return found


def test_the_runbook_names_explicit_rollback_revisions():
    """Review F3: ``downgrade -1`` was documented as dropping the four tables.

    It never did once a second §43 revision existed. One step back from the head
    removes the stop-generation column and leaves every table in place, so an
    administrator following the old instruction would have reported a rollback that
    had not happened and left the new application over a schema missing a column it
    reads.

    No database is needed for this one, and that is deliberate: a wrong instruction
    is caught by the required gate even on a machine with no PostgreSQL at all.
    """
    section = _rollback_section()
    commands = _rollback_commands()
    assert commands, "the rollback section has no commands at all"
    assert not [line for line in commands if "downgrade -1" in line], (
        f"the runbook still tells an operator to step back one revision: {commands}"
    )
    # And it says so in words, so the next reader does not reintroduce it.
    assert "`alembic downgrade -1` is scope A" in section

    targets = _documented_downgrade_targets()
    assert targets == {"A": STOP_GENERATION_PARENT_REVISION, "B": OPERATIONS_PARENT_REVISION}, targets

    # The two scopes are named and distinguished, with what each one costs.
    assert "Scope A" in section and "Scope B" in section
    assert STOP_GENERATION_COLUMN in section
    assert "pg_dump" in section, "a rollback without a backup is not documented as needing one"
    # The ordering rule the defect would otherwise invite: new code, old schema.
    assert "stop altegio-api altegio-easyweek-voucher-executor" in section
    # And it does not read as routine maintenance.
    assert "not a maintenance step" in section
    # The CLI stays shut either way.
    assert "no supported way to run a mailing" in section


async def test_the_documented_fix_rollback_keeps_the_four_tables(disposable_database: str):
    """Scope A, run exactly as the runbook spells it.

    The column and its CHECK go; the four tables, the historical ledgers and every
    other revision stay. Then forward again, because a rollback nobody can come back
    from is not one.
    """
    target = _documented_downgrade_targets()["A"]
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert await _tables(disposable_database, PHASE_TABLES) == set(PHASE_TABLES)
    assert STOP_GENERATION_COLUMN in await _columns(disposable_database, PHASE_TABLES[0])

    assert _alembic(disposable_database, "downgrade", target).returncode == 0

    current = _alembic(disposable_database, "current")
    assert current.returncode == 0
    assert target in current.stdout, current.stdout
    # The documented outcome: the tables are still there.
    assert await _tables(disposable_database, PHASE_TABLES) == set(PHASE_TABLES)
    assert STOP_GENERATION_COLUMN not in await _columns(disposable_database, PHASE_TABLES[0])
    assert STOP_GENERATION_CHECK not in await _check_constraints(disposable_database, PHASE_TABLES[0])
    assert await _tables(disposable_database, HISTORICAL_TABLES) == set(HISTORICAL_TABLES)

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert STOP_GENERATION_COLUMN in await _columns(disposable_database, PHASE_TABLES[0])
    assert STOP_GENERATION_CHECK in await _check_constraints(disposable_database, PHASE_TABLES[0])
    back = _alembic(disposable_database, "current")
    assert back.returncode == 0 and "(head)" in back.stdout


async def test_the_documented_phase_rollback_drops_the_four_tables(disposable_database: str):
    """Scope B, run exactly as the runbook spells it.

    This is the transition the old instruction CLAIMED to perform, so it is checked
    under the revision the document now names — including the historical ledgers it
    promises not to touch, with a real row in one of them.
    """
    target = _documented_downgrade_targets()["B"]
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0

    from sqlalchemy.ext.asyncio import create_async_engine

    marker = "historical-" + uuid_module.uuid4().hex[:10]
    engine = create_async_engine(disposable_database)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO easyweek_voucher_canary_ledger ("
                    " canary_scope, request_schema_version, template_config_digest,"
                    " create_plan_digest, customer_fingerprint, staffer_fingerprint,"
                    " account_fingerprint, reconciliation_marker, status,"
                    " create_claimed_at, create_attempted_at,"
                    " create_window_start, create_window_end"
                    ") VALUES ("
                    " 'historical_canary_v1', '1', :digest, :digest, :fp, :fp, :fp, :marker,"
                    " 'create_claimed', now(), now(), now() - interval '1 hour', now() + interval '1 hour')"
                ),
                {"digest": "c" * 64, "fp": "d" * 64, "marker": marker},
            )

        assert _alembic(disposable_database, "downgrade", target).returncode == 0

        current = _alembic(disposable_database, "current")
        assert current.returncode == 0 and target in current.stdout, current.stdout
        assert await _tables(disposable_database, PHASE_TABLES) == set()
        assert await _tables(disposable_database, HISTORICAL_TABLES) == set(HISTORICAL_TABLES)
        async with engine.connect() as conn:
            kept = (
                await conn.execute(
                    text("SELECT count(*) FROM easyweek_voucher_canary_ledger WHERE reconciliation_marker = :m"),
                    {"m": marker},
                )
            ).scalar_one()
        assert kept == 1, "the rollback took a historical ledger row with it"

        assert _alembic(disposable_database, "upgrade", "head").returncode == 0
        assert await _tables(disposable_database, PHASE_TABLES) == set(PHASE_TABLES)
        assert STOP_GENERATION_COLUMN in await _columns(disposable_database, PHASE_TABLES[0])
        heads = _alembic(_repo_root(), "heads")
        assert len([line for line in heads.stdout.splitlines() if "(head)" in line]) == 1, heads.stdout
    finally:
        await engine.dispose()


async def _check_constraints(database_url: str, table: str) -> set[str]:
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        "SELECT c.conname FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid"
                        " WHERE t.relname = :t AND c.contype = 'c'"
                    ),
                    {"t": table},
                )
            ).all()
        return {row[0] for row in rows}
    finally:
        await engine.dispose()


async def test_pr21_uuid_migration_refuses_loss_and_preserves_legacy_rows(disposable_database: str):
    """PG16: nullable numeric is UUID-only; populated downgrade refuses atomically."""
    from sqlalchemy.ext.asyncio import create_async_engine

    assert _alembic(disposable_database, "upgrade", "c7e3b8a14f29").returncode == 0
    engine = create_async_engine(disposable_database, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            version = int(await conn.scalar(text("SHOW server_version_num")))
            assert 160000 <= version < 170000
            await conn.execute(
                text(
                    "INSERT INTO clients (provider, company_id, altegio_client_id, raw) "
                    "VALUES ('altegio', 322579, 91000001, '{}'::jsonb)"
                )
            )
        upgraded = _alembic(disposable_database, "upgrade", "f6a8d2c91b47")
        assert upgraded.returncode == 0, upgraded.stderr
        async with engine.connect() as conn:
            await conn.execute(
                text(
                    "INSERT INTO clients (provider, company_id, altegio_client_id, easyweek_customer_uuid, raw) "
                    "VALUES ('easyweek', 322579, NULL, '91919191-1212-4343-8787-565656565656', '{}'::jsonb)"
                )
            )
            with pytest.raises(Exception):
                await conn.execute(
                    text(
                        "INSERT INTO clients (provider, company_id, altegio_client_id, raw) "
                        "VALUES ('altegio', 322579, NULL, '{}'::jsonb)"
                    )
                )
            with pytest.raises(Exception):
                await conn.execute(
                    text(
                        "INSERT INTO clients (provider, company_id, altegio_client_id, easyweek_customer_uuid, raw) "
                        "VALUES ('easyweek', 315607, NULL, '91919191-1212-4343-8787-565656565656', '{}'::jsonb)"
                    )
                )
        refused = _alembic(disposable_database, "downgrade", "c7e3b8a14f29")
        assert refused.returncode != 0 and "PR-21 downgrade refused" in refused.stderr
        async with engine.connect() as conn:
            assert await conn.scalar(text("SELECT count(*) FROM clients")) == 2
            assert await conn.scalar(text("SELECT version_num FROM alembic_version")) == "f6a8d2c91b47"
            assert (
                await conn.scalar(text("SELECT altegio_client_id FROM clients WHERE provider = 'altegio'")) == 91000001
            )
            assert "manual_policy" in await _columns(disposable_database, "campaign_recipients")
    finally:
        await engine.dispose()


async def test_pr21_empty_upgrade_downgrade_reupgrade_preserves_constraints(disposable_database: str):
    from sqlalchemy.ext.asyncio import create_async_engine

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert _alembic(disposable_database, "downgrade", "c7e3b8a14f29").returncode == 0
    assert "easyweek_customer_uuid" not in await _columns(disposable_database, "clients")
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    engine = create_async_engine(disposable_database)
    try:
        async with engine.connect() as conn:
            constraints = set((await conn.execute(text("SELECT conname FROM pg_constraint"))).scalars())
            assert {
                "ck_clients_external_identity",
                "uq_clients_provider_company_easyweek_uuid",
                "ck_campaign_recipients_manual_policy",
                "ck_campaign_recipients_manual_policy_proof",
                "ck_ew_voucher_production_item_source",
                "ck_ew_voucher_production_item_policy",
                "uq_ew_voucher_production_item_entitlement",
                "ck_ew_voucher_production_item_single_attempt",
                "ck_ew_voucher_production_item_refund_is_pre_send",
                "ck_ew_manual_plan_applied",
            } <= constraints
    finally:
        await engine.dispose()


async def test_pr21_branch_uuid_upgrade_preserves_rows_and_refuses_lossy_downgrade(disposable_database: str):
    from sqlalchemy.ext.asyncio import create_async_engine

    assert _alembic(disposable_database, "upgrade", "f6a8d2c91b47").returncode == 0
    engine = create_async_engine(disposable_database, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            assert 160000 <= int(await conn.scalar(text("SHOW server_version_num"))) < 170000
            await conn.execute(
                text(
                    "INSERT INTO clients (provider, company_id, altegio_client_id, raw) "
                    "VALUES ('altegio', 322579, 91000001, '{}'::jsonb)"
                )
            )
            await conn.execute(
                text(
                    "INSERT INTO clients (provider, company_id, altegio_client_id, "
                    "easyweek_customer_uuid, wa_opted_out, raw) VALUES ('easyweek', 322579, 17, "
                    "'91919191-1212-4343-8787-565656565656', true, '{}'::jsonb)"
                )
            )
            before = (await conn.execute(text("SELECT * FROM clients ORDER BY id"))).all()
        upgraded = _alembic(disposable_database, "upgrade", "head")
        assert upgraded.returncode == 0, upgraded.stderr
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT * FROM clients ORDER BY id"))).all() == before
            await conn.execute(
                text(
                    "INSERT INTO clients (provider, company_id, altegio_client_id, "
                    "easyweek_customer_uuid, raw) VALUES ('easyweek', 315607, 17, "
                    "'91919191-1212-4343-8787-565656565656', '{}'::jsonb)"
                )
            )
            with pytest.raises(Exception):
                await conn.execute(
                    text(
                        "INSERT INTO clients (provider, company_id, altegio_client_id, "
                        "easyweek_customer_uuid, raw) VALUES ('easyweek', 315607, NULL, "
                        "'91919191-1212-4343-8787-565656565656', '{}'::jsonb)"
                    )
                )
        refused = _alembic(disposable_database, "downgrade", "f6a8d2c91b47")
        assert refused.returncode != 0 and "PR-21 branch downgrade refused" in refused.stderr
        async with engine.connect() as conn:
            assert await conn.scalar(text("SELECT count(*) FROM clients")) == 3
            assert await conn.scalar(text("SELECT version_num FROM alembic_version")) == "d8b4e6a29c13"
            assert (
                await conn.execute(text("SELECT * FROM clients WHERE company_id=322579 ORDER BY id"))
            ).all() == before
            # Synthetic fixture cleanup permits exercising the supported reverse path.
            await conn.execute(text("DELETE FROM clients WHERE company_id=315607"))
        assert _alembic(disposable_database, "downgrade", "f6a8d2c91b47").returncode == 0
        assert _alembic(disposable_database, "upgrade", "head").returncode == 0
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT * FROM clients ORDER BY id"))).all() == before
    finally:
        await engine.dispose()
