"""Confirmation uncertainty and expiry at the locked consumption boundary."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from asyncpg.exceptions import ConnectionDoesNotExistError
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql.asyncpg import AsyncAdapt_asyncpg_connection
from sqlalchemy.exc import SQLAlchemyError

from altegio_bot.campaigns.easyweek_voucher_followup import send_dispatch, send_ledger
from altegio_bot.models.models import (
    EasyWeekVoucherFollowupApproval,
    EasyWeekVoucherFollowupRun,
    EasyWeekVoucherFollowupSend,
    EasyWeekVoucherProductionBatch,
)
from altegio_bot.tests.easyweek_voucher_followup_fixtures import NOW, TemplateReader
from altegio_bot.tests.easyweek_voucher_followup_send_fixtures import confirm, offer, principal
from altegio_bot.tests.test_easyweek_voucher_mailing_durability import (
    _await_lock_waiter,
    _followup_ready,
    followup_environment,  # noqa: F401
)

pytestmark = pytest.mark.usefixtures("followup_environment")


async def _plan(session_maker):
    batch_id, _ = await _followup_ready(session_maker, slots=[{"state": "delivered"}])
    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.ready and plan.approval is not None
    return batch_id, plan


def _lose_commit_ack(monkeypatch):
    original = AsyncAdapt_asyncpg_connection._commit_and_discard
    lost = []

    async def commit_then_disconnect(connection):
        await original(connection)
        if not lost:
            lost.append(True)
            raise ConnectionDoesNotExistError("synthetic lost commit acknowledgement")

    monkeypatch.setattr(AsyncAdapt_asyncpg_connection, "_commit_and_discard", commit_then_disconnect)
    return lost


async def _counts(session_maker):
    async with session_maker() as session:
        return (
            await session.scalar(select(func.count()).select_from(EasyWeekVoucherFollowupRun)),
            await session.scalar(select(func.count()).select_from(EasyWeekVoucherFollowupSend)),
            await session.scalar(select(func.coalesce(func.sum(EasyWeekVoucherFollowupSend.attempt_count), 0))),
        )


async def test_lost_commit_ack_recovers_the_existing_run_without_reconfirming(session_maker, monkeypatch):
    _, plan = await _plan(session_maker)
    lost = _lose_commit_ack(monkeypatch)

    outcome = await confirm(session_maker, plan=plan)

    assert lost == [True]
    assert outcome.accepted and outcome.run is not None
    assert outcome.created is False
    assert outcome.uncertain is False
    assert await _counts(session_maker) == (1, 1, 0)
    replay = await confirm(session_maker, plan=plan)
    assert replay.run is not None and replay.run.id == outcome.run.id
    assert not replay.created and not replay.uncertain
    assert await _counts(session_maker) == (1, 1, 0)


async def test_lost_commit_ack_and_unavailable_reconciliation_remain_unknown(session_maker, monkeypatch):
    _, plan = await _plan(session_maker)
    lost = _lose_commit_ack(monkeypatch)
    reads = []

    async def unavailable(*args, **kwargs):
        reads.append(kwargs["approval_id"])
        raise SQLAlchemyError("synthetic read outage")

    with monkeypatch.context() as patch:
        patch.setattr(send_ledger, "reconcile_confirmation", unavailable)
        outcome = await confirm(session_maker, plan=plan)

    assert lost == [True]
    assert not outcome.accepted and outcome.run is None
    assert outcome.uncertain is True
    assert reads == [plan.approval.id]
    assert await _counts(session_maker) == (1, 1, 0)
    recovered = await send_ledger.reconcile_confirmation(
        session_maker,
        approval_id=plan.approval.id,
        presented_signature=plan.approval.plan_signature,
        principal=principal(),
        confirmed_count=plan.approval.approved_recipient_count,
    )
    assert recovered.run is not None and recovered.accepted and not recovered.uncertain
    assert await _counts(session_maker) == (1, 1, 0)


async def test_a_proven_count_refusal_creates_nothing(session_maker):
    _, plan = await _plan(session_maker)
    outcome = await confirm(session_maker, plan=plan, count=99)
    assert outcome.run is None and not outcome.uncertain
    assert outcome.reasons == ("followup_approval_count_unconfirmed",)
    assert await _counts(session_maker) == (0, 0, 0)


@pytest.mark.parametrize("wrong", ["signature", "principal", "count"])
async def test_reconciliation_does_not_disclose_a_run_to_a_different_confirmation(session_maker, wrong):
    _, plan = await _plan(session_maker)
    first = await confirm(session_maker, plan=plan)
    assert first.run is not None
    recovered = await send_ledger.reconcile_confirmation(
        session_maker,
        approval_id=plan.approval.id,
        presented_signature="wrong" if wrong == "signature" else plan.approval.plan_signature,
        principal=principal("different-operator") if wrong == "principal" else principal(),
        confirmed_count=99 if wrong == "count" else plan.approval.approved_recipient_count,
    )
    assert recovered.run is None and not recovered.accepted and not recovered.uncertain
    assert recovered.reasons
    assert await _counts(session_maker) == (1, 1, 0)


async def test_reconciliation_without_a_committed_run_is_unknown_and_read_only(session_maker):
    _, plan = await _plan(session_maker)
    outcome = await send_ledger.reconcile_confirmation(
        session_maker,
        approval_id=plan.approval.id,
        presented_signature=plan.approval.plan_signature,
        principal=principal(),
        confirmed_count=plan.approval.approved_recipient_count,
    )
    assert outcome.run is None and outcome.uncertain and not outcome.accepted
    assert await _counts(session_maker) == (0, 0, 0)
    stored = await send_ledger.load_approval(session_maker, approval_id=plan.approval.id)
    assert stored is not None and stored.pending


@pytest.mark.parametrize("offset", [-1, 0, 1], ids=["still-live", "exact-expiry", "expired"])
async def test_expiry_is_rechecked_after_a_real_header_wait(session_maker, monkeypatch, offset):
    batch_id, plan = await _plan(session_maker)
    clock = [plan.approval.expires_at - timedelta(seconds=2)]
    monkeypatch.setattr(send_dispatch, "utcnow", lambda: clock[0])
    monkeypatch.setattr(send_ledger, "utcnow", lambda: clock[0])
    async with session_maker() as holder:
        transaction = await holder.begin()
        await holder.get(EasyWeekVoucherProductionBatch, batch_id, with_for_update=True)
        task = asyncio.create_task(
            send_dispatch.confirm_send(
                session_maker,
                approval_id=plan.approval.id,
                presented_signature=plan.approval.plan_signature,
                principal=principal(),
                confirmed_count=plan.approval.approved_recipient_count,
                template_reader=TemplateReader(),
            )
        )
        try:
            assert await _await_lock_waiter(session_maker), "confirmation must be blocked on the header"
            clock[0] = plan.approval.expires_at + timedelta(seconds=offset)
        finally:
            await transaction.rollback()
            outcome = await asyncio.wait_for(task, timeout=10)

    if offset < 0:
        assert outcome.run is not None and outcome.created
        assert outcome.run.queued_at == clock[0]
        assert await _counts(session_maker) == (1, 1, 0)
    else:
        assert outcome.run is None and not outcome.uncertain
        assert outcome.reasons == ("followup_approval_expired",)
        assert await _counts(session_maker) == (0, 0, 0)
        stored = await send_ledger.load_approval(session_maker, approval_id=plan.approval.id)
        assert stored is not None and stored.pending


async def test_expiry_during_external_template_validation_creates_nothing(session_maker, monkeypatch):
    _, plan = await _plan(session_maker)
    clock = [plan.approval.expires_at - timedelta(seconds=1)]
    monkeypatch.setattr(send_dispatch, "utcnow", lambda: clock[0])
    monkeypatch.setattr(send_ledger, "utcnow", lambda: clock[0])

    class DelayedTemplate(TemplateReader):
        async def list_meta_templates(self):
            clock[0] = plan.approval.expires_at
            return await super().list_meta_templates()

    outcome = await send_dispatch.confirm_send(
        session_maker,
        approval_id=plan.approval.id,
        presented_signature=plan.approval.plan_signature,
        principal=principal(),
        confirmed_count=plan.approval.approved_recipient_count,
        template_reader=DelayedTemplate(),
    )
    assert outcome.run is None and not outcome.uncertain
    assert outcome.reasons == ("followup_approval_expired",)
    assert await _counts(session_maker) == (0, 0, 0)


async def test_expired_consumed_approval_still_returns_its_original_run(session_maker):
    _, plan = await _plan(session_maker)
    original = await confirm(session_maker, plan=plan)
    replay = await confirm(session_maker, plan=plan, now=plan.approval.expires_at + timedelta(days=1))
    assert original.run is not None and replay.run is not None
    assert replay.run.id == original.run.id
    assert not replay.created and not replay.uncertain
    assert await _counts(session_maker) == (1, 1, 0)
    async with session_maker() as session:
        approval = await session.get(EasyWeekVoucherFollowupApproval, plan.approval.id)
        assert approval is not None and approval.consumed_at == NOW
