"""PR-21 UUID-first identity and list confirmation; synthetic provider reads only."""

from __future__ import annotations

import asyncio
import uuid
from datetime import date, timedelta

import pytest
from sqlalchemy import func, select

import altegio_bot.campaigns.easyweek_manual_batch as bulk
import altegio_bot.campaigns.runner as campaign_runner
from altegio_bot.campaigns.easyweek_manual_identity import IDENTITY_CONFLICT
from altegio_bot.campaigns.easyweek_manual_recipient import add_manual_recipient
from altegio_bot.models.models import CampaignRecipient, CampaignRun, Client, EasyWeekManualRecipientPlan
from altegio_bot.utils import utcnow

PHONE = "+4915100000821"
PHONE_TWO = "+4915100000822"
CUSTOMER = "12121212-3434-4567-8901-121212121212"
CUSTOMER_TWO = "12121212-3434-4567-8901-121212121213"
OPERATOR = "synthetic-operator"
SESSION = "synthetic-session"


def page(data, *, total=None, last_page=1):
    return {
        "data": data,
        "meta": {
            "current_page": 1,
            "last_page": last_page,
            "per_page": 100,
            "total": len(data) if total is None else total,
        },
    }


class Reader:
    def __init__(self):
        self.cards = {
            PHONE: {"uuid": CUSTOMER, "phone": PHONE, "first_name": "Synthetic"},
            PHONE_TWO: {"uuid": CUSTOMER_TWO, "phone": PHONE_TWO, "first_name": "Synthetic Two"},
        }
        self.history = {}
        self.calls = []
        self.lookup_error = None
        self.history_error = None
        self.direct_override = None

    async def list_customers(self, *, params):
        self.calls.append("list")
        if self.lookup_error:
            raise self.lookup_error
        card = self.cards.get(params["phone"])
        return page([card] if card else [])

    async def get_customer(self, customer_uuid):
        self.calls.append("get")
        if self.direct_override is not None:
            return self.direct_override
        return next(card for card in self.cards.values() if card["uuid"] == customer_uuid)

    async def list_customer_bookings(self, customer_uuid, page, per_page=100):
        self.calls.append("history")
        if self.history_error:
            raise self.history_error
        return self.history.get(customer_uuid, globals()["page"]([]))


def booking(*, canceled=False, completed=False):
    return {
        "uuid": "cccccccc-aaaa-4111-8222-dddddddddddd",
        "customer": {"uuid": CUSTOMER},
        "is_canceled": canceled,
        "is_completed": completed,
        "start_time": "2099-01-01T12:00:00+00:00",
    }


async def preview(session_maker):
    async with session_maker() as session, session.begin():
        run = CampaignRun(
            provider="easyweek",
            campaign_code="new_clients_monthly",
            mode="preview",
            status="completed",
            company_ids=[322579],
            period_start=date(2026, 9, 1),
            period_end=date(2026, 9, 30),
        )
        session.add(run)
        await session.flush()
        return run.id


async def check(session_maker, run_id, reader=None, phones=PHONE, **kwargs):
    return await bulk.check_manual_recipients(
        session_maker,
        run_id=run_id,
        phones=phones,
        reader=reader or Reader(),
        operator=kwargs.pop("operator", OPERATOR),
        session_fingerprint=SESSION,
        prior_altegio_visit_confirmed=True,
        assign_karlsruhe_confirmed=True,
        **kwargs,
    )


async def confirm(session_maker, run_id, plan, reader=None, **kwargs):
    return await bulk.confirm_manual_recipients(
        session_maker,
        run_id=run_id,
        plan_id=plan["plan_id"],
        confirmed_count=plan["eligible_count"],
        reader=reader or Reader(),
        operator=kwargs.pop("operator", OPERATOR),
        session_fingerprint=kwargs.pop("session_fingerprint", SESSION),
        **kwargs,
    )


async def counts(session_maker):
    async with session_maker() as session:
        return tuple(
            [await session.scalar(select(func.count()).select_from(model)) for model in (Client, CampaignRecipient)]
        )


@pytest.mark.asyncio
async def test_check_is_read_only_then_apply_creates_uuid_only_identity(session_maker, configuration):
    run_id = await preview(session_maker)
    before = await counts(session_maker)
    plan = await check(session_maker, run_id)
    assert plan["ok"] and plan["eligible_count"] == 1
    assert await counts(session_maker) == before
    applied = await confirm(session_maker, run_id, plan)
    assert applied["ok"] and applied["added_count"] == 1
    async with session_maker() as session:
        client = await session.scalar(select(Client).where(Client.phone_e164 == PHONE))
        row = await session.scalar(select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run_id))
        stored_plan = await session.get(EasyWeekManualRecipientPlan, uuid.UUID(plan["plan_id"]))
        assert client.altegio_client_id is None
        assert client.easyweek_customer_uuid == uuid.UUID(CUSTOMER)
        assert client.easyweek_visits_total is None and client.easyweek_visits_total_updated_at is None
        assert client.easyweek_identity_assigned_at is not None
        assert row.client_id == client.id and row.recipient_basis == "operator_manual_selection"
        assert row.manual_policy == bulk.MANUAL_POLICY
        assert row.manual_policy_checked_at and row.manual_operator_attested_at
        assert row.source_booking_uuid is None and row.source_visits_total is None
        assert stored_plan.payload == {}
    replay_reader = Reader()
    assert await confirm(session_maker, run_id, plan, replay_reader) == applied
    assert replay_reader.calls == []


@pytest.mark.asyncio
async def test_single_add_uses_same_explicit_assignment_resolver(session_maker, configuration):
    run_id = await preview(session_maker)
    refused = await add_manual_recipient(session_maker, run_id=run_id, phone=PHONE, reader=Reader())
    assert refused.reason == "manual_recipient_branch_assignment_required"
    added = await add_manual_recipient(
        session_maker, run_id=run_id, phone=PHONE, reader=Reader(), assign_karlsruhe=True
    )
    assert added.ok
    async with session_maker() as session:
        row = await session.get(CampaignRecipient, added.recipient_id)
        assert row.manual_policy is None


@pytest.mark.asyncio
@pytest.mark.parametrize("flags", [{"completed": True}, {"canceled": True}, {}])
async def test_any_booking_including_future_or_canceled_is_excluded(session_maker, configuration, flags):
    run_id = await preview(session_maker)
    reader = Reader()
    reader.history[CUSTOMER] = page([booking(**flags)])
    result = await check(session_maker, run_id, reader)
    assert result["eligible_count"] == 0 and result["rows"][0]["reason"] == bulk.HISTORY_NONEMPTY


@pytest.mark.asyncio
@pytest.mark.parametrize("history", [{}, page([], total=1), page([], last_page=2), {"data": [], "meta": None}])
async def test_incomplete_history_is_not_zero(session_maker, configuration, history):
    reader = Reader()
    reader.history[CUSTOMER] = history
    assert await bulk.prove_zero_booking_history(reader, customer_uuid=CUSTOMER) == bulk.HISTORY_UNPROVEN


@pytest.mark.asyncio
async def test_absent_customer_and_timeout_are_not_zero(session_maker, configuration):
    run_id = await preview(session_maker)
    reader = Reader()
    reader.cards = {}
    absent = await check(session_maker, run_id, reader)
    assert absent["rows"][0]["reason"] == "manual_recipient_customer_absent"
    assert "history" not in reader.calls
    reader = Reader()
    reader.lookup_error = TimeoutError("synthetic")
    failed = await check(session_maker, run_id, reader)
    assert failed["rows"][0]["reason"] == "manual_recipient_customer_unproven"


@pytest.mark.asyncio
async def test_duplicates_normalize_once_and_eligible_subset_is_explicit(session_maker, configuration):
    run_id = await preview(session_maker)
    reader = Reader()
    result = await check(session_maker, run_id, reader, phones=PHONE + "\n+49 151-00000821\ninvalid\n" + PHONE_TWO)
    assert result["eligible_count"] == 2 and result["duplicate_count"] == 1 and result["rejected_count"] == 1
    assert reader.calls.count("list") == 2
    applied = await confirm(session_maker, run_id, result)
    assert applied["added_count"] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("other_branch,opted_out", [(False, True), (True, False)])
async def test_conflicts_and_legacy_optout_cannot_be_bypassed(session_maker, configuration, other_branch, opted_out):
    run_id = await preview(session_maker)
    async with session_maker() as session, session.begin():
        original = Client(
            provider="easyweek" if other_branch else "altegio",
            company_id=315607,
            altegio_client_id=919191,
            phone_e164=PHONE,
            display_name="Original",
            wa_opted_out=opted_out,
        )
        session.add(original)
        await session.flush()
        original_id = original.id
    result = await check(session_maker, run_id)
    assert result["eligible_count"] == 0
    assert result["rows"][0]["reason"] == (IDENTITY_CONFLICT if other_branch else "manual_recipient_opted_out")
    async with session_maker() as session:
        original = await session.get(Client, original_id)
        assert original.wa_opted_out is opted_out
        assert original.altegio_client_id == 919191 and original.company_id == 315607


@pytest.mark.asyncio
async def test_legacy_card_survives_successful_assignment(session_maker, configuration):
    run_id = await preview(session_maker)
    async with session_maker() as session, session.begin():
        legacy = Client(provider="altegio", company_id=315607, altegio_client_id=919191, phone_e164=PHONE)
        session.add(legacy)
        await session.flush()
        legacy_id = legacy.id
    plan = await check(session_maker, run_id)
    assert (await confirm(session_maker, run_id, plan))["ok"]
    async with session_maker() as session:
        legacy = await session.get(Client, legacy_id)
        assert legacy.provider == "altegio" and legacy.altegio_client_id == 919191
        assert legacy.easyweek_customer_uuid is None


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["history", "optout", "identity", "composition"])
async def test_confirm_refuses_changed_facts_without_partial_addition(session_maker, configuration, drift):
    run_id = await preview(session_maker)
    plan = await check(session_maker, run_id, phones=PHONE + "\n" + PHONE_TWO)
    reader = Reader()
    if drift == "history":
        reader.history[CUSTOMER] = page([booking(canceled=True)])
    elif drift == "identity":
        reader.cards[PHONE]["uuid"] = "12121212-3434-4567-8901-121212121299"
    else:
        async with session_maker() as session, session.begin():
            if drift == "optout":
                session.add(
                    Client(provider="altegio", company_id=1, altegio_client_id=333, phone_e164=PHONE, wa_opted_out=True)
                )
            else:
                run = await session.get(CampaignRun, run_id)
                run.period_start = date(2026, 8, 1)
    before = await counts(session_maker)
    result = await confirm(session_maker, run_id, plan, reader)
    assert not result["ok"]
    assert await counts(session_maker) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["operator", "session", "preview", "expired", "tampered"])
async def test_confirmation_is_bound_and_expires(session_maker, configuration, case):
    run_id = await preview(session_maker)
    plan = await check(session_maker, run_id)
    kwargs = {}
    if case == "operator":
        kwargs["operator"] = "another-operator"
    elif case == "session":
        kwargs["session_fingerprint"] = "another-session"
    elif case == "preview":
        run_id = await preview(session_maker)
    else:
        async with session_maker() as session, session.begin():
            stored = await session.get(EasyWeekManualRecipientPlan, uuid.UUID(plan["plan_id"]))
            if case == "expired":
                stored.created_at = utcnow() - timedelta(hours=1)
                stored.expires_at = utcnow() - timedelta(minutes=1)
            else:
                stored.payload = {**stored.payload, "input_digest": "tampered"}
    before = await counts(session_maker)
    assert not (await confirm(session_maker, run_id, plan, **kwargs))["ok"]
    assert await counts(session_maker) == before


@pytest.mark.asyncio
async def test_double_confirmation_is_idempotent(session_maker, configuration):
    run_id = await preview(session_maker)
    plan = await check(session_maker, run_id)
    first, second = await asyncio.gather(confirm(session_maker, run_id, plan), confirm(session_maker, run_id, plan))
    assert first == second and first["ok"]
    assert (await counts(session_maker))[1] == 1


@pytest.mark.asyncio
async def test_counter_failure_rolls_back_all_clients_and_recipients(session_maker, configuration, monkeypatch):
    run_id = await preview(session_maker)
    plan = await check(session_maker, run_id, phones=PHONE + "\n" + PHONE_TWO)
    before = await counts(session_maker)

    async def failed(*args, **kwargs):
        raise RuntimeError("synthetic counter failure")

    monkeypatch.setattr(campaign_runner, "recompute_snapshot_counters", failed)
    with pytest.raises(RuntimeError, match="synthetic counter failure"):
        await confirm(session_maker, run_id, plan)
    assert await counts(session_maker) == before
    async with session_maker() as session:
        stored = await session.get(EasyWeekManualRecipientPlan, uuid.UUID(plan["plan_id"]))
        assert stored.applied_at is None


@pytest.mark.asyncio
async def test_concurrent_single_adds_share_one_uuid_identity(session_maker, configuration):
    first_run = await preview(session_maker)
    second_run = await preview(session_maker)
    results = await asyncio.gather(
        *[
            add_manual_recipient(session_maker, run_id=run_id, phone=PHONE, reader=Reader(), assign_karlsruhe=True)
            for run_id in (first_run, second_run)
        ]
    )
    assert all(result.ok for result in results)
    async with session_maker() as session:
        assert await session.scalar(select(func.count()).select_from(Client).where(Client.phone_e164 == PHONE)) == 1


@pytest.mark.asyncio
async def test_existing_manual_recipient_preserves_historical_policy(session_maker, configuration):
    run_id = await preview(session_maker)
    assert (
        await add_manual_recipient(session_maker, run_id=run_id, phone=PHONE, reader=Reader(), assign_karlsruhe=True)
    ).ok
    plan = await check(session_maker, run_id, phones=PHONE + "\n" + PHONE_TWO)
    assert plan["already_present_count"] == 1 and plan["eligible_count"] == 1
    assert (await confirm(session_maker, run_id, plan))["ok"]
    async with session_maker() as session:
        first = await session.scalar(select(CampaignRecipient).where(CampaignRecipient.phone_e164 == PHONE))
        assert first.manual_policy is None


@pytest.mark.asyncio
async def test_rate_limit_reserves_before_provider_reads(session_maker, configuration):
    run_id = await preview(session_maker)
    for _ in range(3):
        assert (await check(session_maker, run_id))["ok"]
    reader = Reader()
    assert (await check(session_maker, run_id, reader))["reason"] == bulk.RATE_LIMIT
    assert reader.calls == []


@pytest.mark.asyncio
async def test_frozen_preview_never_reads_provider(session_maker, configuration, monkeypatch):
    run_id = await preview(session_maker)

    async def frozen(*args, **kwargs):
        return True

    monkeypatch.setattr(bulk, "_frozen", frozen)
    reader = Reader()
    result = await check(session_maker, run_id, reader)
    assert result["reason"] == "manual_recipient_preview_frozen"
    assert reader.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed",
    [
        {"uuid": CUSTOMER_TWO, "phone": PHONE, "first_name": "Synthetic"},
        {"uuid": CUSTOMER, "phone": PHONE_TWO, "first_name": "Synthetic"},
        {"uuid": "not-a-uuid", "phone": PHONE, "first_name": "Synthetic"},
    ],
)
async def test_external_identity_mismatch_never_creates_a_client(session_maker, configuration, changed):
    run_id = await preview(session_maker)
    reader = Reader()
    reader.direct_override = changed
    before = await counts(session_maker)
    result = await check(session_maker, run_id, reader)
    assert result["eligible_count"] == 0
    assert result["rows"][0]["reason"] == "manual_recipient_customer_unproven"
    assert await counts(session_maker) == before


@pytest.mark.asyncio
async def test_local_uuid_conflict_is_not_overwritten(session_maker, configuration):
    run_id = await preview(session_maker)
    async with session_maker() as session, session.begin():
        session.add(
            Client(
                provider="easyweek",
                company_id=322579,
                altegio_client_id=None,
                easyweek_customer_uuid=uuid.UUID(CUSTOMER_TWO),
                phone_e164=PHONE,
            )
        )
    result = await check(session_maker, run_id)
    assert result["rows"][0]["reason"] == IDENTITY_CONFLICT


@pytest.mark.asyncio
async def test_existing_earned_candidate_is_not_downgraded(session_maker, configuration):
    from altegio_bot.tests.easyweek_voucher_delivery_fixtures import seed_recipient

    run_id, recipient_id = await seed_recipient(session_maker, phone=PHONE, client_phone=PHONE)
    plan = await check(session_maker, run_id, phones=PHONE + "\n" + PHONE_TWO)
    assert plan["already_present_count"] == 1 and plan["eligible_count"] == 1
    assert (await confirm(session_maker, run_id, plan))["ok"]
    async with session_maker() as session:
        row = await session.get(CampaignRecipient, recipient_id)
        assert row.recipient_basis == "earned_first_visit" and row.source_booking_uuid is not None
        assert row.manual_policy is None


@pytest.mark.asyncio
async def test_recipient_pointing_to_a_different_local_card_does_not_create_orphan_identity(
    session_maker, configuration
):
    run_id = await preview(session_maker)
    added = await add_manual_recipient(
        session_maker, run_id=run_id, phone=PHONE, reader=Reader(), assign_karlsruhe=True
    )
    async with session_maker() as session, session.begin():
        row = await session.get(CampaignRecipient, added.recipient_id)
        client = await session.get(Client, row.client_id)
        # Model a contradictory historical mapping, not provider evidence.
        client.altegio_client_id = 887766
        client.easyweek_customer_uuid = None
        client.easyweek_identity_assigned_at = None
        client.phone_e164 = PHONE_TWO
    before = await counts(session_maker)
    rejected = await add_manual_recipient(
        session_maker, run_id=run_id, phone=PHONE, reader=Reader(), assign_karlsruhe=True
    )
    assert not rejected.ok and rejected.reason == "manual_recipient_rows_ambiguous"
    assert await counts(session_maker) == before
    checked = await check(session_maker, run_id)
    assert checked["eligible_count"] == 0 and checked["rows"][0]["reason"] == "manual_recipient_rows_ambiguous"


@pytest.mark.asyncio
async def test_all_malformed_input_retains_per_contact_results(session_maker, configuration):
    run_id = await preview(session_maker)
    reader = Reader()
    result = await check(session_maker, run_id, reader, phones="not-a-number\n+49 O12345")
    assert result["ok"] and result["eligible_count"] == 0 and result["plan_id"] is None
    assert result["rejected_count"] == 2
    assert all(row["reason"] == "manual_recipient_phone_unusable" for row in result["rows"])
    assert reader.calls == []
