"""Production proofs and external mutations never hold a read transaction open."""

from __future__ import annotations

from typing import Any
from weakref import WeakSet

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_voucher_production.composition import prove_production_composition
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_PAY,
    STAGE_REFUND,
)
from altegio_bot.campaigns.easyweek_voucher_production.read_sessions import release_read_session
from altegio_bot.models.models import Client
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    FakeMutator,
    FakeSender,
    marker_orders,
    production_request,
)
from altegio_bot.tests.test_easyweek_voucher_mixed_audience import mixed_preview
from altegio_bot.tests.test_easyweek_voucher_production_mailing import _apply, _freeze, _ok_response
from altegio_bot.utils import utcnow


class _ObservedTransport:
    def __init__(self, delegate: Any, sessions: WeakSet, calls: list[str]) -> None:
        self.delegate = delegate
        self.sessions = sessions
        self.calls = calls

    def __getattr__(self, name):
        target = getattr(self.delegate, name)
        if not callable(target):
            return target

        async def call(*args, **kwargs):
            assert all(not session.in_transaction() for session in self.sessions), f"transaction held across {name}"
            self.calls.append(name)
            return await target(*args, **kwargs)

        return call


def _tracked_factory(session_maker):
    sessions = WeakSet()

    class TrackedSession(AsyncSession):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            sessions.add(self)

    return async_sessionmaker(session_maker.kw["bind"], class_=TrackedSession, expire_on_commit=False), sessions


@pytest.mark.parametrize("last_stage", [STAGE_DELIVER, STAGE_REFUND])
async def test_mixed_proofs_and_full_mutations_release_all_read_transactions(
    session_maker, production_configuration, binding_key, monkeypatch, last_stage
):
    tracked, sessions = _tracked_factory(session_maker)
    run_id, _, actual_reader = await mixed_preview(tracked, monkeypatch)
    calls = []
    reader = _ObservedTransport(actual_reader, sessions, calls)
    # The UI's composition-only path must have the same read boundary.
    async with tracked() as session:
        composition = await prove_production_composition(
            session, preview_run_id=run_id, client_reader=reader, now=utcnow()
        )
        assert composition.proven
        assert not session.in_transaction()
    frozen = await _freeze(tracked, reader, production_request(run_id=run_id), count=2)
    request = production_request(run_id=run_id, batch_id=frozen.batch["batch_id"])
    orders = await marker_orders(tracked, batch_id=request.batch_id)
    actual_reader.orders.update(orders)
    create = _ObservedTransport(FakeMutator(create_sequence=[_ok_response(0), _ok_response(1)]), sessions, calls)
    created = await _apply(tracked, reader, stage=STAGE_CREATE, request=request, mutator=create)
    assert created.outcome == "applied"
    paid_orders = await marker_orders(tracked, batch_id=request.batch_id, status="paid")
    pay = _ObservedTransport(
        FakeMutator(pay_sequence=[_ok_response(0), _ok_response(1)], reader=actual_reader, settles=paid_orders),
        sessions,
        calls,
    )
    paid = await _apply(tracked, reader, stage=STAGE_PAY, request=request, mutator=pay)
    assert paid.outcome == "applied"
    if last_stage == STAGE_DELIVER:
        sender = _ObservedTransport(FakeSender(), sessions, calls)
        result = await _apply(tracked, reader, stage=STAGE_DELIVER, request=request, sender=sender)
        assert "send_voucher_template" in calls
    else:
        refund_orders = await marker_orders(tracked, batch_id=request.batch_id, status="refunded")
        refund = _ObservedTransport(
            FakeMutator(refund=_ok_response(1), reader=actual_reader, settles=refund_orders), sessions, calls
        )
        result = await _apply(tracked, reader, stage=STAGE_REFUND, request=request, slot=2, mutator=refund)
        assert "refund_voucher_order" in calls
    assert result.outcome == "applied"
    assert {"get_booking", "get_customer", "list_customer_bookings", "get_order", "create_voucher_order"} <= set(calls)
    assert "pay_voucher_order" in calls


@pytest.mark.parametrize("change", ["new", "dirty", "deleted"])
@pytest.mark.parametrize("boundary", ["release", "proof_entry"])
async def test_release_rejects_pending_writes_instead_of_committing_or_discarding(session_maker, change, boundary):
    async with session_maker() as session:
        client = await session.scalar(select(Client).limit(1))
        assert client is not None
        if change == "new":
            session.add(Client(provider="altegio", company_id=42, altegio_client_id=456, raw={}))
        elif change == "dirty":
            client.display_name = "Pending uncommitted edit"
        else:
            await session.delete(client)
        with pytest.raises(RuntimeError, match="voucher_production_preflight_session_has_writes"):
            if boundary == "release":
                await release_read_session(session)
            else:
                await prove_production_composition(session, preview_run_id=1, client_reader=object(), now=utcnow())
        assert session.in_transaction()
        assert session.new or session.dirty or session.deleted


async def test_release_preserves_detached_loaded_facts_and_allows_fresh_reads(session_maker):
    async with session_maker() as session:
        client = await session.scalar(select(Client).limit(1))
        assert client is not None
        expected = (client.id, client.phone_e164, client.display_name)
        await release_read_session(session)
        assert not session.in_transaction()
        assert (client.id, client.phone_e164, client.display_name) == expected
        fresh = await session.get(Client, expected[0])
        assert fresh is not client and fresh.id == expected[0]
