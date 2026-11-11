"""§46.7–46.8 — the follow-up send: one attempt, one boundary, one honest answer.

The question every test here asks
---------------------------------
Can this original recipient receive a SECOND extra message? The answer has to be
no for every path: a second run, another preview version, another group
selection, another template, a duplicate card, two executors, a crash, a
restart, a timeout, a rejection, a stop, a re-confirmation. So most of these
tests end in a count — how many POSTs were made — rather than in a status,
because a count is the only thing that cannot be argued with.

The second question is where the attempt is spent. The contract is
:data:`...send_identity.ATTEMPT_BOUNDARY`: the committed transaction that writes
``claimed``, before the one external request. Tests on both sides of that line
prove what it buys — zero requests when a refusal is durable before it, and an
honest ``unknown`` that never becomes a retry after it.

What is real here
-----------------
PostgreSQL: the CHECK constraints, the three partial unique indexes, the row
locks, the consent address advisory locks and ``FOR UPDATE SKIP LOCKED``. The
approval signature and its TTL. The real executor loop. The real callback
routing, through the real inbound status path where that is what is under test.

What is faked is EasyWeek's read-only API and Meta's message endpoint, and the
Meta one COUNTS, which is the whole point of :class:`CountingSender`.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest
import respx
from pydantic import SecretStr
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from altegio_bot.campaigns.easyweek_voucher_followup import callbacks as callbacks_module
from altegio_bot.campaigns.easyweek_voucher_followup import followup_template_contract as message
from altegio_bot.campaigns.easyweek_voucher_followup import send_dispatch as dispatch_module
from altegio_bot.campaigns.easyweek_voucher_followup import send_identity as send_ids
from altegio_bot.campaigns.easyweek_voucher_followup import send_ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production import ledger as production_ledger
from altegio_bot.models.models import (
    FOLLOWUP_CALLBACK_CONFIRMED,
    FOLLOWUP_CALLBACK_CONFLICTING,
    FOLLOWUP_CALLBACK_UNMATCHED,
    FOLLOWUP_GROUP_READ,
    FOLLOWUP_RUN_INTERRUPTED,
    FOLLOWUP_RUN_RUNNING,
    FOLLOWUP_RUN_STOPPED,
    EasyWeekVoucherFollowupCallback,
    EasyWeekVoucherFollowupRun,
    EasyWeekVoucherFollowupSend,
    EasyWeekVoucherProductionBatchItem,
    MessageJob,
    OutboxMessage,
)
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_voucher_followup_fixtures import (
    NOW,
    SENDER_DIGEST,
    TemplateReader,
    location_map,
    person,
)
from altegio_bot.tests.easyweek_voucher_followup_send_fixtures import (
    OPERATOR,
    OTHER_OPERATOR,
    CountingSender,
    StubTransports,
    accepted,
    build_preview,
    confirm,
    offer,
    person_phone,
    person_uuid,
    principal,
    rejected,
    rows_of,
    seed_ready_batch,
    status_payload,
    unknown,
)
from altegio_bot.tests.test_easyweek_voucher_followup_preview import FakeHistoryReader
from altegio_bot.whatsapp_consent import recipient_route_digest
from altegio_bot.workers import easyweek_voucher_followup_worker as worker_module

pytestmark = pytest.mark.usefixtures("binding_key", "no_network")


@pytest.fixture
def binding_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """The voucher MAC key the fixtures' artifacts were bound under, and the registry.

    Both are needed by every path here: the MAC because the send re-proves the
    original binding before each attempt, and the branch registry because the
    approved BODY's second parameter is this branch's booking link and a
    deployment without one must refuse rather than send a link to nowhere.
    """
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key", SecretStr("k" * 48), raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key_id", "test-key-1", raising=False)
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(), raising=False)


@pytest.fixture
def no_network() -> Any:
    """Every real HTTP request in this suite is a defect, including a harmless one."""
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as mock:
        yield mock


@pytest.fixture
def ops_signing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A signing key for the plan signature. Synthetic, and never a credential."""
    monkeypatch.setattr(settings, "ops_secret", "synthetic-ops-signing-secret", raising=False)
    monkeypatch.setattr(settings, "ops_pass", "synthetic-ops-password", raising=False)


async def _ready(session_maker, *, slots: list[dict[str, Any]] | None = None) -> tuple[int, Any]:
    """A seeded batch plus the fake EasyWeek reader its artifacts match."""
    batch_id = await seed_ready_batch(session_maker, slots=slots)
    reader = FakeHistoryReader(batch_id=batch_id)
    await build_preview(session_maker, batch_id=batch_id, reader=reader)
    return batch_id, reader


async def _run(
    session_maker,
    *,
    batch_id: int,
    sender: CountingSender,
    reader: Any,
    owner: str = "executor-1",
) -> Any:
    """Offer, confirm and execute one run end to end, with a counted sender."""
    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.ready, plan.reasons
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.created, outcome.reasons
    assert outcome.run is not None
    claimed = await ledger_module.claim_next_run(session_maker, owner=owner)
    assert claimed is not None
    return await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner=owner,
        transports=StubTransports(reader=reader, sender=sender),
    )


# ---------------------------------------------------------------------------
# The fence (§46.8)
# ---------------------------------------------------------------------------


async def test_the_send_fence_is_closed_by_default_and_refuses_the_whole_scenario(session_maker, ops_signing) -> None:
    """No setting patched at all: the deployed default must refuse by name."""
    batch_id, _reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    assert not plan.ready
    assert plan.reasons == (send_ids.SEND_DISABLED,)
    assert plan.approval is None


async def test_permitting_the_primary_mailing_does_not_open_the_followup_fence(
    session_maker, ops_signing, primary_fence_only
) -> None:
    """§46.8's exact bypass, closed: the two fences are separate settings."""
    batch_id, _reader = await _ready(session_maker)
    assert settings.easyweek_voucher_production_mailing_enabled is True
    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.reasons == (send_ids.SEND_DISABLED,)


async def test_a_confirmed_run_refuses_to_execute_if_the_fence_closes_after_the_confirmation(
    session_maker, ops_signing, send_fence, monkeypatch
) -> None:
    """A confirmation is not a standing permission. Zero requests, honestly recorded."""
    batch_id, reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None

    monkeypatch.setattr(settings, "easyweek_voucher_followup_send_enabled", False, raising=False)
    sender = CountingSender()
    report = await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert report.outcome == "refused"
    assert report.reasons == (send_ids.SEND_DISABLED,)
    assert sender.count == 0
    rows = await rows_of(session_maker, run_id=claimed.id)
    assert {row.status for row in rows.values()} == {send_ids.PLANNED}
    assert all(int(row.attempt_count) == 0 for row in rows.values())


# ---------------------------------------------------------------------------
# The offer and the confirmation (§46.8)
# ---------------------------------------------------------------------------


async def test_an_offer_states_the_exact_sendable_count_and_is_signed(session_maker, ops_signing, send_fence) -> None:
    batch_id, _reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.ready
    assert plan.approval is not None
    assert plan.approval.approved_recipient_count == 2
    assert plan.approval.action == "followup_send"
    assert len(plan.approval.plan_signature) == 64
    # The contract the operator is confirming, pinned on the row rather than
    # recomputed at confirmation time.
    assert plan.approval.meta_template_name == message.FOLLOWUP_META_TEMPLATE_NAME
    assert plan.approval.template_body_digest == message.body_digest()
    assert plan.approval.contract_version == send_ids.SEND_CONTRACT_VERSION


async def test_an_offer_carries_no_personal_value_at_all(session_maker, ops_signing, send_fence) -> None:
    """§46.8: no code, no phone, no name, no rendered BODY in an approval."""
    batch_id, _reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.approval is not None
    printed = repr(plan.approval.as_safe_dict())
    for forbidden in (person(1)["phone"], person(2)["phone"], person(1)["customer_uuid"], "FOLLOWUP-TEST-CODE"):
        assert str(forbidden) not in printed
    # And the approved body itself is never stored or echoed: only its digest.
    assert "Gutschein" not in printed


async def test_the_confirmation_requires_the_exact_count_the_offer_stated(
    session_maker, ops_signing, send_fence
) -> None:
    batch_id, _reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    refused = await confirm(session_maker, plan=plan, count=1)
    assert refused.run is None
    assert refused.reasons == (send_ids.APPROVAL_COUNT_UNCONFIRMED,)
    # Nothing started: a refused confirmation has provably no run.
    async with session_maker() as session:
        assert (await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupRun))).scalar_one() == 0


async def test_a_forged_signature_cannot_spend_an_offer(session_maker, ops_signing, send_fence) -> None:
    batch_id, _reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    refused = await confirm(session_maker, plan=plan, signature="f" * 64)
    assert refused.reasons == (send_ids.APPROVAL_SIGNATURE_INVALID,)
    assert refused.run is None


async def test_another_operator_cannot_spend_this_operators_offer(session_maker, ops_signing, send_fence) -> None:
    """An approval id is not a capability (§46.8)."""
    batch_id, _reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    refused = await confirm(session_maker, plan=plan, actor=principal(OTHER_OPERATOR))
    assert refused.reasons == (send_ids.APPROVAL_PRINCIPAL_MISMATCH,)
    assert refused.run is None
    # The positive control: the operator it was issued to still can.
    allowed = await confirm(session_maker, plan=plan)
    assert allowed.created is True


async def test_an_expired_offer_is_refused_and_the_ttl_is_measured_from_issue(
    session_maker, ops_signing, send_fence
) -> None:
    batch_id, _reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.approval is not None
    assert plan.approval.expires_at == plan.approval.issued_at + send_ids.PLAN_MAX_AGE
    refused = await confirm(
        session_maker,
        plan=plan,
        now=plan.approval.expires_at + timedelta(seconds=1),
    )
    assert refused.reasons == (send_ids.APPROVAL_EXPIRED,)


async def test_confirming_twice_produces_one_run_and_returns_it(session_maker, ops_signing, send_fence) -> None:
    """Replay protection: the double click, the retried POST, the second tab."""
    batch_id, _reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    first = await confirm(session_maker, plan=plan)
    second = await confirm(session_maker, plan=plan)
    assert first.created is True
    assert second.created is False
    assert second.reasons == (send_ids.APPROVAL_ALREADY_CONSUMED,)
    assert second.run is not None and second.run.id == first.run.id
    async with session_maker() as session:
        assert (await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupRun))).scalar_one() == 1


async def test_changing_the_approved_text_after_the_offer_requires_a_new_confirmation(
    session_maker, ops_signing, send_fence, monkeypatch
) -> None:
    """§46.6: a changed message contract invalidates the offer rather than sending."""
    batch_id, _reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    monkeypatch.setattr(message, "FOLLOWUP_TEMPLATE_BODY", "Hallo {client_name}, {booking_link}.", raising=False)
    refused = await confirm(session_maker, plan=plan)
    assert refused.reasons == (send_ids.APPROVAL_CONTRACT_MOVED,)
    assert refused.run is None


async def test_stopping_the_original_mailing_after_the_offer_refuses_the_confirmation(
    session_maker, ops_signing, send_fence
) -> None:
    """§46.5: the original batch's terminal stop forbids its follow-up."""
    batch_id, _reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    await production_ledger.request_stop(session_maker, batch_id=batch_id, requested_by="owner")
    refused = await confirm(session_maker, plan=plan)
    assert refused.reasons == (send_ids.SOURCE_TERMINAL_STOP,)
    assert refused.run is None


async def test_a_second_run_of_the_same_batch_is_refused_while_one_is_in_flight(
    session_maker, ops_signing, send_fence
) -> None:
    batch_id, _reader = await _ready(session_maker)
    first = await offer(session_maker, batch_id=batch_id)
    assert (await confirm(session_maker, plan=first)).created is True
    second = await offer(session_maker, batch_id=batch_id)
    assert second.ready
    refused = await confirm(session_maker, plan=second)
    assert refused.reasons == (send_ids.RUN_ALREADY_ACTIVE,)


async def test_an_offer_needs_a_current_preview(session_maker, ops_signing, send_fence) -> None:
    batch_id = await seed_ready_batch(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.reasons == (send_ids.PREVIEW_UNKNOWN,)

    reader = FakeHistoryReader(batch_id=batch_id)
    created = await build_preview(session_maker, batch_id=batch_id, reader=reader)
    stale = await offer(
        session_maker,
        batch_id=batch_id,
        now=created.valid_until + timedelta(seconds=1),
    )
    assert stale.reasons[0] == send_ids.PREVIEW_NOT_CURRENT


# ---------------------------------------------------------------------------
# One attempt, and the scope of the key (§46.7)
# ---------------------------------------------------------------------------


async def test_a_full_run_sends_once_per_recipient_and_records_the_approved_body(
    session_maker, ops_signing, send_fence
) -> None:
    batch_id, reader = await _ready(session_maker)
    sender = CountingSender()
    report = await _run(session_maker, batch_id=batch_id, sender=sender, reader=reader)

    assert report.attempted == 2
    assert report.accepted == 2
    assert sender.count == 2
    # The approved contract, in the approved order, with no voucher code.
    for call in sender.calls:
        assert call["template_name"] == message.FOLLOWUP_META_TEMPLATE_NAME
        assert call["language"] == message.FOLLOWUP_TEMPLATE_LANGUAGE
        assert len(call["params"]) == 2
        assert call["params"][1].startswith("https://")
        assert "FOLLOWUP-TEST-CODE" not in "".join(call["params"])
    rows = await rows_of(session_maker, run_id=report.run_id)
    assert {row.status for row in rows.values()} == {send_ids.ACCEPTED}
    assert all(int(row.attempt_count) == 1 for row in rows.values())
    assert all(row.provider_message_id for row in rows.values())


async def test_a_second_confirmed_run_cannot_send_to_the_same_recipients_again(
    session_maker, ops_signing, send_fence
) -> None:
    """The one-attempt rule, across runs: the second run sends nobody."""
    batch_id, reader = await _ready(session_maker)
    first = CountingSender()
    await _run(session_maker, batch_id=batch_id, sender=first, reader=reader)
    assert first.count == 2

    await build_preview(session_maker, batch_id=batch_id, reader=reader)
    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.ready
    # The offer itself already states the honest number: nobody is sendable.
    assert plan.approval is not None and plan.approval.approved_recipient_count == 0
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.created is True
    assert outcome.planned == 0
    assert outcome.skipped == 2
    second = CountingSender()
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-2")
    assert claimed is not None
    report = await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-2",
        transports=StubTransports(reader=reader, sender=second),
    )
    assert report.attempted == 0
    assert second.count == 0
    rows = await rows_of(session_maker, run_id=claimed.id)
    assert {row.reason_code for row in rows.values()} == {send_ids.ATTEMPT_ALREADY_USED}


async def test_another_group_selection_does_not_buy_a_second_attempt(session_maker, ops_signing, send_fence) -> None:
    """§46.7: a different segment, preview or template does not bypass the key."""
    batch_id = await seed_ready_batch(session_maker, slots=[{"state": "read"}])
    reader = FakeHistoryReader(batch_id=batch_id)
    await build_preview(session_maker, batch_id=batch_id, reader=reader, groups=(FOLLOWUP_GROUP_READ,))
    sender = CountingSender()
    await _run(session_maker, batch_id=batch_id, sender=sender, reader=reader)
    assert sender.count == 1

    # A brand-new preview, the same person, selected through the same group a
    # second time: the key is the SOURCE recipient, not the selection.
    await build_preview(session_maker, batch_id=batch_id, reader=reader, groups=(FOLLOWUP_GROUP_READ,))
    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.approval is not None and plan.approval.approved_recipient_count == 0


async def test_the_database_itself_refuses_a_second_blocking_row_for_one_source_slot(
    session_maker, ops_signing, send_fence
) -> None:
    """The guarantee is the partial unique index, not the writer that checks first.

    The application already refuses this, and that refusal is tested above. What
    this proves is the layer underneath: even a writer that skipped every check
    — a future bug, a hand-written repair, a second process — cannot promote a
    second row into the blocking set for one source slot.
    """
    batch_id, reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    sender = CountingSender()
    await _run(session_maker, batch_id=batch_id, sender=sender, reader=reader)

    # A second run exists in the ordinary way, and its row is ``skipped``, which
    # does not occupy the key. Promoting it by hand is what must fail.
    await build_preview(session_maker, batch_id=batch_id, reader=reader)
    plan = await offer(session_maker, batch_id=batch_id)
    second = await confirm(session_maker, plan=plan)
    assert second.run is not None and second.skipped == 1
    rows = await rows_of(session_maker, run_id=second.run.id)
    skipped = rows[1]
    assert skipped.status == send_ids.SKIPPED

    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                row = await session.get(EasyWeekVoucherFollowupSend, int(skipped.id))
                assert row is not None
                row.status = send_ids.UNKNOWN
                row.attempt_count = 1
                row.intent_uuid = person_uuid(9)
                row.claimed_at = NOW
                row.sender_digest = SENDER_DIGEST
                row.recipient_digest = "r" * 64
                row.reason_code = None
                await session.flush()


async def test_the_key_is_scoped_to_the_original_mailing_and_not_to_the_customer_for_life(
    session_maker, ops_signing, send_fence
) -> None:
    """§46.7: no accidental lifetime ban on future independent primary mailings.

    The three partial unique indexes are scoped to the ORIGINAL mailing and the
    ORIGINAL entitlement. So the very same canonical customer uuid, reached
    through a DIFFERENT original batch, is outside this key space — which is
    what keeps a future independent primary mailing to that person sendable.

    Asserted against the index itself rather than through a second seeded
    mailing, because the claim being made is about the index's scope: the row
    below carries the first person's uuid and another batch's slot, and it
    inserts precisely because the uuid alone is not the key.
    """
    first_batch, reader = await _ready(session_maker, slots=[{"state": "delivered", "index": 1}])
    sender = CountingSender()
    report = await _run(session_maker, batch_id=first_batch, sender=sender, reader=reader)
    assert sender.count == 1
    rows = await rows_of(session_maker, run_id=report.run_id)
    original = rows[1]

    # A second original mailing, with its own slot and its own entitlement, and
    # deliberately the SAME canonical customer uuid.
    second_batch = await seed_ready_batch(session_maker, slots=[{"state": "delivered", "index": 5}])
    async with session_maker() as session:
        second_item = (
            await session.execute(
                select(EasyWeekVoucherProductionBatchItem).where(
                    EasyWeekVoucherProductionBatchItem.batch_id == second_batch
                )
            )
        ).scalar_one()
    async with session_maker() as session:
        async with session.begin():
            session.add(
                EasyWeekVoucherFollowupSend(
                    run_id=int(original.run_id),
                    source_batch_id=int(second_batch),
                    source_item_id=int(second_item.id),
                    source_slot=int(second_item.slot),
                    campaign_recipient_id=int(second_item.campaign_recipient_id),
                    easyweek_customer_uuid=original.easyweek_customer_uuid,
                    source_entitlement_digest="e" * 64,
                    group_code=str(original.group_code),
                    status=send_ids.UNKNOWN,
                    attempt_count=1,
                    intent_uuid=person_uuid(9),
                    claimed_at=NOW,
                    sender_digest=SENDER_DIGEST,
                    recipient_digest="r" * 64,
                    template_code=str(original.template_code),
                    meta_template_name=str(original.meta_template_name),
                    template_language=str(original.template_language),
                    template_body_digest=str(original.template_body_digest),
                )
            )
            await session.flush()


# ---------------------------------------------------------------------------
# The boundary: STOP and consent before it (§46.4.6, §46.7)
# ---------------------------------------------------------------------------


async def test_a_refusal_durable_before_the_boundary_produces_zero_meta_requests(
    session_maker, ops_signing, send_fence
) -> None:
    """§46.4.6's exact guarantee, asserted as a number."""
    batch_id, reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None

    from altegio_bot.whatsapp_consent import ProjectedRefusal, apply_projected_refusal

    async with session_maker() as session:
        async with session.begin():
            await apply_projected_refusal(
                session,
                ProjectedRefusal(
                    wa_address=person_phone(1).lstrip("+"),
                    origin="test_projection",
                    observed_at=NOW,
                    event_at=NOW,
                    authenticity="signature_verified",
                ),
            )

    sender = CountingSender()
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None
    report = await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    rows = await rows_of(session_maker, run_id=claimed.id)
    assert rows[1].status == send_ids.SKIPPED
    assert int(rows[1].attempt_count) == 0
    # The other recipient is unaffected, which is the positive control: a
    # refusal excludes one person, not the run.
    assert rows[2].status == send_ids.ACCEPTED
    assert sender.count == 1
    assert report.skipped == 1


async def test_an_accepted_but_unprocessed_inbound_message_holds_the_send(
    session_maker, ops_signing, send_fence
) -> None:
    """§46.4.6: a queue that is behind must not become a permission."""
    batch_id, reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    await confirm(session_maker, plan=plan)

    from altegio_bot.models.models import INBOUND_AUTHENTICITY_SIGNED, WhatsAppInboundReceipt

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppInboundReceipt(
                    receipt_key="pending-1",
                    message_index=0,
                    wa_address=person_phone(1).lstrip("+"),
                    # An ordinary message, whose first token is not a consent
                    # command — which is exactly the case that matters: nobody
                    # knows yet whether it says stop.
                    command="none",
                    authenticity=INBOUND_AUTHENTICITY_SIGNED,
                    observed_at=NOW,
                    event_at=NOW,
                    # Unresolved is the whole point: the inbox worker has not
                    # accounted for it yet, and §46.4.6 says that holds the send.
                    resolved_at=None,
                )
            )

    sender = CountingSender()
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None
    await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    rows = await rows_of(session_maker, run_id=claimed.id)
    assert rows[1].status == send_ids.SKIPPED
    assert sender.count == 1


async def test_a_terminal_stop_with_zero_attempts_still_leaves_a_durable_tombstone(
    session_maker, ops_signing, send_fence
) -> None:
    """§46.7: the stop survives a new run, a new preview and a new template."""
    batch_id, reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None

    stop = await ledger_module.request_terminal_stop(session_maker, run_id=outcome.run.id, requested_by=OPERATOR)
    assert stop.applied and stop.tombstoned == 2
    rows = await rows_of(session_maker, run_id=outcome.run.id)
    assert {row.status for row in rows.values()} == {send_ids.STOPPED}
    assert all(int(row.attempt_count) == 0 for row in rows.values())
    # The run is finished rather than waiting for an executor that would only
    # find tombstones.
    stopped = await ledger_module.load_run(session_maker, run_id=outcome.run.id)
    assert stopped is not None and stopped.status == FOLLOWUP_RUN_STOPPED

    # A new preview and a new run do not reopen them.
    await build_preview(session_maker, batch_id=batch_id, reader=reader)
    again = await offer(session_maker, batch_id=batch_id)
    assert again.approval is not None and again.approval.approved_recipient_count == 0
    second = await confirm(session_maker, plan=again)
    assert second.skipped == 2
    sender = CountingSender()
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-2")
    assert claimed is not None
    await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-2",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert sender.count == 0
    later = await rows_of(session_maker, run_id=claimed.id)
    assert {row.reason_code for row in later.values()} == {send_ids.TERMINALLY_STOPPED}


async def test_pressing_stop_twice_is_reported_and_changes_nothing(session_maker, ops_signing, send_fence) -> None:
    batch_id, _reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    first = await ledger_module.request_terminal_stop(session_maker, run_id=outcome.run.id, requested_by=OPERATOR)
    second = await ledger_module.request_terminal_stop(session_maker, run_id=outcome.run.id, requested_by=OPERATOR)
    assert first.already_stopped is False and first.tombstoned == 2
    assert second.already_stopped is True and second.tombstoned == 0


async def test_a_stop_mid_run_leaves_the_started_attempt_alone_and_sends_nothing_further(
    session_maker, ops_signing, send_fence
) -> None:
    """A started request cannot be pretended cancelled; the rest get zero."""
    batch_id, reader = await _ready(
        session_maker, slots=[{"state": "delivered"}, {"state": "delivered"}, {"state": "delivered"}]
    )
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    run_id = outcome.run.id
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None

    stop_after_first = asyncio.Event()

    class StopAfterFirst(CountingSender):
        async def send_voucher_template(self, **kwargs: Any) -> Any:
            answer = await super().send_voucher_template(**kwargs)
            if len(self.calls) == 1:
                # The operator presses stop while the first request is in
                # flight. Durable before the SECOND row's boundary.
                await ledger_module.request_terminal_stop(session_maker, run_id=run_id, requested_by=OPERATOR)
                stop_after_first.set()
            return answer

    sender = StopAfterFirst()
    report = await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert stop_after_first.is_set()
    assert sender.count == 1
    assert report.stopped is True
    rows = await rows_of(session_maker, run_id=run_id)
    assert rows[1].status == send_ids.ACCEPTED
    assert {rows[2].status, rows[3].status} == {send_ids.STOPPED}
    finished = await ledger_module.load_run(session_maker, run_id=run_id)
    assert finished is not None and finished.status == FOLLOWUP_RUN_STOPPED


# ---------------------------------------------------------------------------
# Crash, unknown and restart (§46.7)
# ---------------------------------------------------------------------------


async def test_a_crash_past_the_boundary_becomes_an_honest_unknown_and_never_a_retry(
    session_maker, ops_signing, send_fence
) -> None:
    batch_id, reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    run_id = outcome.run.id
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None

    # The claim commits, and then the process dies before the POST.
    rows = await rows_of(session_maker, run_id=run_id)
    verdict_sender = CountingSender(raise_on_call=RuntimeError("process died"))
    report = await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=verdict_sender),
    )
    assert verdict_sender.count == 1
    assert report.unknown == 1
    rows = await rows_of(session_maker, run_id=run_id)
    assert rows[1].status == send_ids.UNKNOWN
    assert int(rows[1].attempt_count) == 1

    # A restart finds nothing to do, and a new run cannot reopen it.
    await ledger_module.interrupt_abandoned_runs(session_maker, include_all_running=True)
    await build_preview(session_maker, batch_id=batch_id, reader=reader)
    again = await offer(session_maker, batch_id=batch_id)
    assert again.approval is not None and again.approval.approved_recipient_count == 0


async def test_an_executor_that_died_holding_the_lease_leaves_unknowns_not_plans(
    session_maker, ops_signing, send_fence
) -> None:
    """Start-up sweep: ``claimed`` becomes ``unknown``, never ``planned``."""
    batch_id, reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    run_id = outcome.run.id
    claimed = await ledger_module.claim_next_run(session_maker, owner="dead-executor")
    assert claimed is not None

    # The claim is committed and the process vanishes with no answer recorded.
    rows = await rows_of(session_maker, run_id=run_id)
    send_id = int(rows[1].id)
    claim = await ledger_module.claim_send(
        session_maker,
        run_id=run_id,
        send_id=send_id,
        owner="dead-executor",
        address=person_phone(1),
        easyweek_customer_uuid=person_uuid(1),
        expected_consent_fingerprint=await _fingerprint(session_maker, index=1),
        sender_digest=SENDER_DIGEST,
        recipient_digest=str(recipient_route_digest(person_phone(1))),
        source_batch_id=batch_id,
    )
    assert claim.granted

    interrupted = await ledger_module.interrupt_abandoned_runs(session_maker, include_all_running=True)
    assert [entry.id for entry in interrupted] == [run_id]
    after = await ledger_module.load_run(session_maker, run_id=run_id)
    assert after is not None and after.status == FOLLOWUP_RUN_INTERRUPTED
    rows = await rows_of(session_maker, run_id=run_id)
    assert rows[1].status == send_ids.UNKNOWN
    assert int(rows[1].attempt_count) == 1
    assert rows[1].evidence.get("unknown_cause") == "executor_interrupted"

    # An interrupted run is never picked up again.
    assert await ledger_module.claim_next_run(session_maker, owner="executor-2") is None


async def _fingerprint(session_maker, *, index: int) -> str:
    from altegio_bot.campaigns.easyweek_voucher_followup.eligibility import consent_snapshot

    async with session_maker() as session:
        snapshot = await consent_snapshot(
            session,
            phone=person_phone(index),
            easyweek_customer_uuid=person_uuid(index),
        )
    return snapshot.fingerprint


async def test_an_unknown_answer_consumes_the_attempt_and_asks_for_reconciliation(
    session_maker, ops_signing, send_fence
) -> None:
    batch_id, reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    sender = CountingSender(answers=[unknown("meta_timeout")])
    report = await _run(session_maker, batch_id=batch_id, sender=sender, reader=reader)
    assert sender.count == 1
    assert report.unknown == 1
    assert report.accepted == 0
    rows = await rows_of(session_maker, run_id=report.run_id)
    assert rows[1].status == send_ids.UNKNOWN
    assert rows[1].reason_code == send_ids.META_UNKNOWN
    assert int(rows[1].attempt_count) == 1


async def test_a_proven_rejection_also_consumes_the_attempt_and_gives_no_third_message(
    session_maker, ops_signing, send_fence
) -> None:
    """§46.7: a failed follow-up does not entitle anybody to another one."""
    batch_id, reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    sender = CountingSender(answers=[rejected()])
    report = await _run(session_maker, batch_id=batch_id, sender=sender, reader=reader)
    assert report.rejected == 1
    rows = await rows_of(session_maker, run_id=report.run_id)
    assert rows[1].status == send_ids.REJECTED
    assert int(rows[1].attempt_count) == 1

    await build_preview(session_maker, batch_id=batch_id, reader=reader)
    again = await offer(session_maker, batch_id=batch_id)
    assert again.approval is not None and again.approval.approved_recipient_count == 0


async def test_there_is_no_automatic_transport_retry(session_maker, ops_signing, send_fence) -> None:
    """One POST per recipient, whatever the answer was. Counted, not reasoned about."""
    batch_id, reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    sender = CountingSender(answers=[unknown("meta_server_error")])
    await _run(session_maker, batch_id=batch_id, sender=sender, reader=reader)
    assert sender.count == 1
    # And the real client has no retry path either: it follows no redirect and
    # declares no transport retries.
    from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import VoucherDeliveryClient

    client = VoucherDeliveryClient(access_token="t", graph_url="https://example.test", api_version="v21.0")
    try:
        assert client._client.follow_redirects is False
        transport = client._client._transport
        assert getattr(transport, "_pool", None) is None or getattr(transport, "_retries", 0) == 0
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# The primary mailing is untouched (§46.7)
# ---------------------------------------------------------------------------


async def test_the_original_mailing_keeps_its_single_attempt_and_its_bindings(
    session_maker, ops_signing, send_fence
) -> None:
    batch_id, reader = await _ready(session_maker)
    async with session_maker() as session:
        before = (
            await session.execute(
                select(
                    EasyWeekVoucherProductionBatchItem.slot,
                    EasyWeekVoucherProductionBatchItem.status,
                    EasyWeekVoucherProductionBatchItem.send_attempt_count,
                    EasyWeekVoucherProductionBatchItem.voucher_code_hmac,
                    EasyWeekVoucherProductionBatchItem.hmac_key_id,
                    EasyWeekVoucherProductionBatchItem.provider_message_id,
                    EasyWeekVoucherProductionBatchItem.delivered_at,
                    EasyWeekVoucherProductionBatchItem.read_at,
                ).order_by(EasyWeekVoucherProductionBatchItem.slot)
            )
        ).all()
    await _run(session_maker, batch_id=batch_id, sender=CountingSender(), reader=reader)
    async with session_maker() as session:
        after = (
            await session.execute(
                select(
                    EasyWeekVoucherProductionBatchItem.slot,
                    EasyWeekVoucherProductionBatchItem.status,
                    EasyWeekVoucherProductionBatchItem.send_attempt_count,
                    EasyWeekVoucherProductionBatchItem.voucher_code_hmac,
                    EasyWeekVoucherProductionBatchItem.hmac_key_id,
                    EasyWeekVoucherProductionBatchItem.provider_message_id,
                    EasyWeekVoucherProductionBatchItem.delivered_at,
                    EasyWeekVoucherProductionBatchItem.read_at,
                ).order_by(EasyWeekVoucherProductionBatchItem.slot)
            )
        ).all()
    assert after == before
    assert all(int(row[2]) <= 1 for row in after)


async def test_a_run_creates_no_job_no_outbox_row_and_no_voucher_mutation(
    session_maker, ops_signing, send_fence
) -> None:
    """§46.6: no CREATE, no PAY, no refund, no new entitlement, no generic queue."""
    batch_id, reader = await _ready(session_maker)
    report = await _run(session_maker, batch_id=batch_id, sender=CountingSender(), reader=reader)
    assert report.as_safe_dict()["voucher_mutations"] == 0
    async with session_maker() as session:
        assert (await session.execute(select(func.count()).select_from(MessageJob))).scalar_one() == 0
        assert (await session.execute(select(func.count()).select_from(OutboxMessage))).scalar_one() == 0
        # The §42 operations table is the generic voucher queue; nothing here
        # may have written to it.
        operations = (
            await session.execute(text("SELECT count(*) FROM easyweek_voucher_production_operations"))
        ).scalar_one()
        assert int(operations) == 0


# ---------------------------------------------------------------------------
# Callbacks about the follow-up's own messages (§46.3, §46.5)
# ---------------------------------------------------------------------------


async def _sent_one(session_maker, *, slots: list[dict[str, Any]] | None = None) -> tuple[int, Any, Any]:
    """One accepted follow-up message, and the row it belongs to."""
    batch_id, reader = await _ready(session_maker, slots=slots or [{"state": "delivered"}])
    sender = CountingSender(answers=[accepted("wamid.followup.A")])
    report = await _run(session_maker, batch_id=batch_id, sender=sender, reader=reader)
    rows = await rows_of(session_maker, run_id=report.run_id)
    return batch_id, rows[1], reader


async def _deliver(session_maker, status: dict[str, Any], *, kind: str, signed: bool = True) -> Any:
    async with session_maker() as session:
        async with session.begin():
            return await callbacks_module.record_followup_callback(
                session,
                status=status,
                kind=kind,
                signature_verified=signed,
            )


async def test_a_proven_delivered_then_read_moves_the_followup_row_only(session_maker, ops_signing, send_fence) -> None:
    batch_id, row, _reader = await _sent_one(session_maker)
    phone = person_phone(1)
    for kind in ("delivered", "read"):
        outcome = await _deliver(
            session_maker,
            status_payload(provider_message_id="wamid.followup.A", kind=kind, recipient_phone=phone),
            kind=kind,
        )
        assert outcome.claimed and outcome.match_state == FOLLOWUP_CALLBACK_CONFIRMED
        assert outcome.advanced is True

    rows = await rows_of(session_maker, run_id=int(row.run_id))
    assert rows[1].status == send_ids.READ
    assert rows[1].delivered_at is not None and rows[1].read_at is not None
    # And the ORIGINAL message's own record is untouched.
    async with session_maker() as session:
        item = await session.get(EasyWeekVoucherProductionBatchItem, int(row.source_item_id))
        assert item is not None
        assert item.read_at is None
        assert str(item.status) == "delivered"


async def test_a_duplicate_callback_is_one_fact_and_changes_nothing_twice(
    session_maker, ops_signing, send_fence
) -> None:
    _batch, row, _reader = await _sent_one(session_maker)
    payload = status_payload(provider_message_id="wamid.followup.A", kind="delivered", recipient_phone=person_phone(1))
    first = await _deliver(session_maker, payload, kind="delivered")
    second = await _deliver(session_maker, payload, kind="delivered")
    assert first.stored is True
    assert second.stored is False
    assert second.advanced is False
    async with session_maker() as session:
        count = (await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupCallback))).scalar_one()
    assert int(count) == 1


async def test_a_read_that_arrives_before_the_delivered_is_not_walked_backwards(
    session_maker, ops_signing, send_fence
) -> None:
    _batch, row, _reader = await _sent_one(session_maker)
    phone = person_phone(1)
    await _deliver(
        session_maker,
        status_payload(provider_message_id="wamid.followup.A", kind="read", recipient_phone=phone),
        kind="read",
    )
    late = await _deliver(
        session_maker,
        status_payload(provider_message_id="wamid.followup.A", kind="delivered", recipient_phone=phone),
        kind="delivered",
    )
    assert late.advanced is False
    rows = await rows_of(session_maker, run_id=int(row.run_id))
    assert rows[1].status == send_ids.READ
    # Both moments are nevertheless recorded: they are real facts about the
    # same message, and the report should be able to show them.
    assert rows[1].delivered_at is not None and rows[1].read_at is not None


async def test_a_late_failed_after_a_read_is_kept_as_a_contradiction_and_rolls_nothing_back(
    session_maker, ops_signing, send_fence
) -> None:
    _batch, row, _reader = await _sent_one(session_maker)
    phone = person_phone(1)
    await _deliver(
        session_maker,
        status_payload(provider_message_id="wamid.followup.A", kind="read", recipient_phone=phone),
        kind="read",
    )
    failed = await _deliver(
        session_maker,
        status_payload(provider_message_id="wamid.followup.A", kind="failed", recipient_phone=phone, error_code=131026),
        kind="failed",
    )
    assert failed.claimed is True
    assert failed.match_state == FOLLOWUP_CALLBACK_CONFLICTING
    assert failed.reason == callbacks_module.CONFLICT_AFTER_SUCCESS
    rows = await rows_of(session_maker, run_id=int(row.run_id))
    assert rows[1].status == send_ids.READ
    assert int(rows[1].contradiction_count) == 1


async def test_a_callback_from_a_foreign_sender_proves_nothing(session_maker, ops_signing, send_fence) -> None:
    _batch, row, _reader = await _sent_one(session_maker)
    outcome = await _deliver(
        session_maker,
        status_payload(
            provider_message_id="wamid.followup.A",
            kind="delivered",
            phone_number_id="999999999999999",
            recipient_phone=person_phone(1),
        ),
        kind="delivered",
    )
    assert outcome.claimed is True
    assert outcome.match_state == FOLLOWUP_CALLBACK_CONFLICTING
    assert outcome.reason == callbacks_module.CONFLICT_FOREIGN_ROUTE
    rows = await rows_of(session_maker, run_id=int(row.run_id))
    assert rows[1].status == send_ids.ACCEPTED


async def test_an_unsigned_callback_proves_nothing_and_moves_nothing(session_maker, ops_signing, send_fence) -> None:
    _batch, row, _reader = await _sent_one(session_maker)
    outcome = await _deliver(
        session_maker,
        status_payload(provider_message_id="wamid.followup.A", kind="read", recipient_phone=person_phone(1)),
        kind="read",
        signed=False,
    )
    assert outcome.match_state == FOLLOWUP_CALLBACK_CONFLICTING
    assert outcome.reason == callbacks_module.CONFLICT_SIGNATURE_UNPROVEN
    rows = await rows_of(session_maker, run_id=int(row.run_id))
    assert rows[1].status == send_ids.ACCEPTED


async def test_a_callback_for_somebody_elses_message_id_is_not_claimed(session_maker, ops_signing, send_fence) -> None:
    await _sent_one(session_maker)
    outcome = await _deliver(
        session_maker,
        status_payload(provider_message_id="wamid.someone.else", kind="delivered"),
        kind="delivered",
    )
    assert outcome.claimed is False
    assert outcome.reason == callbacks_module.SKIPPED_NOT_OURS


async def test_an_early_callback_is_kept_and_bound_by_the_commit_that_records_the_id(
    session_maker, ops_signing, send_fence
) -> None:
    """§46.3: a status can overtake the acceptance; it must not be lost."""
    batch_id, reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    run_id = outcome.run.id
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None
    phone = person_phone(1)

    class EarlyCallbackSender(CountingSender):
        async def send_voucher_template(self, **kwargs: Any) -> Any:
            # Meta delivers the `read` while this request's answer is still in
            # flight, so nothing owns the identifier yet.
            async with session_maker() as session:
                async with session.begin():
                    kept = await callbacks_module.record_unmatched_followup_callback(
                        session,
                        status=status_payload(
                            provider_message_id="wamid.followup.EARLY",
                            kind="read",
                            recipient_phone=phone,
                        ),
                        kind="read",
                        signature_verified=True,
                    )
            assert kept.stored is True
            assert kept.match_state == FOLLOWUP_CALLBACK_UNMATCHED
            return accepted("wamid.followup.EARLY")

    sender = EarlyCallbackSender()
    await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    rows = await rows_of(session_maker, run_id=run_id)
    assert rows[1].status == send_ids.READ
    assert rows[1].read_at is not None
    async with session_maker() as session:
        fact = (
            await session.execute(
                select(EasyWeekVoucherFollowupCallback).where(
                    EasyWeekVoucherFollowupCallback.provider_message_id == "wamid.followup.EARLY"
                )
            )
        ).scalar_one()
        assert str(fact.match_state) == FOLLOWUP_CALLBACK_CONFIRMED
        assert int(fact.send_id) == int(rows[1].id)


async def test_an_unsigned_early_callback_is_not_kept_at_all(session_maker, ops_signing, send_fence) -> None:
    batch_id, _reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    plan = await offer(session_maker, batch_id=batch_id)
    await confirm(session_maker, plan=plan)
    async with session_maker() as session:
        async with session.begin():
            outcome = await callbacks_module.record_unmatched_followup_callback(
                session,
                status=status_payload(provider_message_id="wamid.followup.X", kind="read"),
                kind="read",
                signature_verified=False,
            )
    assert outcome.claimed is False
    assert outcome.reason == callbacks_module.CONFLICT_SIGNATURE_UNPROVEN


# ---------------------------------------------------------------------------
# The executor (§46.8)
# ---------------------------------------------------------------------------


async def test_the_executor_claims_one_run_and_finishes_it(session_maker, ops_signing, send_fence) -> None:
    batch_id, reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    await confirm(session_maker, plan=plan)
    sender = CountingSender()
    finished = await worker_module.run_once(
        session_maker,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
        heartbeat_interval=60.0,
    )
    assert finished is not None and finished.finished
    assert sender.count == 2
    # An empty queue answers None rather than doing anything.
    assert (
        await worker_module.run_once(
            session_maker,
            owner="executor-1",
            transports=StubTransports(reader=reader, sender=CountingSender()),
            heartbeat_interval=60.0,
        )
        is None
    )


async def test_two_executors_cannot_both_take_the_same_run(session_maker, ops_signing, send_fence) -> None:
    batch_id, _reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    await confirm(session_maker, plan=plan)
    first = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    second = await ledger_module.claim_next_run(session_maker, owner="executor-2")
    assert first is not None
    assert second is None
    assert first.status == FOLLOWUP_RUN_RUNNING


async def test_an_executor_without_the_lease_cannot_claim_a_row(session_maker, ops_signing, send_fence) -> None:
    batch_id, _reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    owner = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert owner is not None
    rows = await rows_of(session_maker, run_id=outcome.run.id)
    claim = await ledger_module.claim_send(
        session_maker,
        run_id=outcome.run.id,
        send_id=int(rows[1].id),
        owner="executor-2",
        address=person_phone(1),
        easyweek_customer_uuid=person_uuid(1),
        expected_consent_fingerprint=await _fingerprint(session_maker, index=1),
        sender_digest=SENDER_DIGEST,
        recipient_digest=str(recipient_route_digest(person_phone(1))),
        source_batch_id=batch_id,
    )
    assert claim.granted is False
    assert claim.reason == send_ids.CLAIM_LEASE_LOST


async def test_a_consent_fingerprint_that_moved_refuses_the_claim(session_maker, ops_signing, send_fence) -> None:
    """The locked re-check catches a decision that landed and was answered since."""
    batch_id, _reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    owner = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert owner is not None
    rows = await rows_of(session_maker, run_id=outcome.run.id)
    claim = await ledger_module.claim_send(
        session_maker,
        run_id=outcome.run.id,
        send_id=int(rows[1].id),
        owner="executor-1",
        address=person_phone(1),
        easyweek_customer_uuid=person_uuid(1),
        expected_consent_fingerprint="0" * 64,
        sender_digest=SENDER_DIGEST,
        recipient_digest=str(recipient_route_digest(person_phone(1))),
        source_batch_id=batch_id,
    )
    assert claim.granted is False
    assert claim.reason == send_ids.CONSENT_MOVED
    rows = await rows_of(session_maker, run_id=outcome.run.id)
    assert rows[1].status == send_ids.PLANNED
    assert int(rows[1].attempt_count) == 0


# ---------------------------------------------------------------------------
# The re-check AT CONFIRMATION (§46.5)
# ---------------------------------------------------------------------------


async def test_a_refusal_between_the_preview_and_the_confirmation_is_excluded_at_the_confirmation(
    session_maker, ops_signing, send_fence
) -> None:
    """§46.5 asks for eligibility to be proven again at the confirmation.

    The row is excluded there, with its reason, rather than carried into the run
    as planned and discovered by the executor — and nobody takes its place, so
    the planned count is simply smaller than the preview's.
    """
    from altegio_bot.whatsapp_consent import ProjectedRefusal, apply_projected_refusal

    batch_id, reader = await _ready(session_maker)
    async with session_maker() as session:
        async with session.begin():
            await apply_projected_refusal(
                session,
                ProjectedRefusal(
                    wa_address=person_phone(1).lstrip("+"),
                    origin="test_projection",
                    observed_at=NOW,
                    event_at=NOW,
                    authenticity="signature_verified",
                ),
            )

    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.ready
    # The offer already states the honest number: one of the two is refused.
    assert plan.approval is not None and plan.approval.approved_recipient_count == 1
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.created is True
    assert outcome.planned == 1
    assert outcome.skipped == 1
    rows = await rows_of(session_maker, run_id=outcome.run.id)
    assert rows[1].status == send_ids.SKIPPED
    assert rows[1].reason_code == "followup_consent_refused"
    assert int(rows[1].attempt_count) == 0
    assert rows[2].status == send_ids.PLANNED

    sender = CountingSender()
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None
    await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert sender.count == 1


async def test_a_refusal_for_an_address_with_no_suppression_row_races_the_claim_safely(
    session_maker, ops_signing, send_fence
) -> None:
    """The absent-row case, raced on purpose (§46.4.6).

    This person has never been heard from, so there is no suppression row to
    lock — which is exactly the race the consent ADDRESS advisory lock exists
    for. A refusal and a claim run concurrently, and the two possible outcomes
    are both honest: either the refusal committed first and the claim is refused
    with zero requests, or the claim committed first and the refusal is recorded
    after it with its own time. What must never happen is a granted claim whose
    consent fingerprint describes a world in which the refusal already existed.
    """
    from altegio_bot.campaigns.easyweek_voucher_followup.eligibility import consent_snapshot
    from altegio_bot.models.models import WhatsAppConsentSuppression
    from altegio_bot.whatsapp_consent import ProjectedRefusal, apply_projected_refusal

    batch_id, _reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None
    rows = await rows_of(session_maker, run_id=outcome.run.id)

    # The precondition, asserted rather than assumed: no row to lock.
    async with session_maker() as session:
        existing = (await session.execute(select(func.count()).select_from(WhatsAppConsentSuppression))).scalar_one()
        assert int(existing) == 0
        snapshot = await consent_snapshot(session, phone=person_phone(1), easyweek_customer_uuid=person_uuid(1))

    async def refuse() -> Any:
        async with session_maker() as session:
            async with session.begin():
                return await apply_projected_refusal(
                    session,
                    ProjectedRefusal(
                        wa_address=person_phone(1).lstrip("+"),
                        origin="test_projection",
                        observed_at=NOW,
                        event_at=NOW,
                        authenticity="signature_verified",
                    ),
                )

    claim, _refusal = await asyncio.gather(
        ledger_module.claim_send(
            session_maker,
            run_id=outcome.run.id,
            send_id=int(rows[1].id),
            owner="executor-1",
            address=person_phone(1),
            easyweek_customer_uuid=person_uuid(1),
            expected_consent_fingerprint=snapshot.fingerprint,
            sender_digest=SENDER_DIGEST,
            recipient_digest=str(recipient_route_digest(person_phone(1))),
            source_batch_id=batch_id,
        ),
        refuse(),
    )

    # Exactly one suppression row exists either way: the advisory lock closed the
    # absent-row race rather than letting two writers both insert.
    async with session_maker() as session:
        count = (await session.execute(select(func.count()).select_from(WhatsAppConsentSuppression))).scalar_one()
    assert int(count) == 1

    rows = await rows_of(session_maker, run_id=outcome.run.id)
    if claim.granted:
        # The claim won. Its fingerprint is the one it was issued against, and
        # the refusal is recorded after it with its own time.
        assert int(rows[1].attempt_count) == 1
        assert rows[1].consent_fingerprint == snapshot.fingerprint
    else:
        assert claim.reason == send_ids.CONSENT_MOVED
        assert int(rows[1].attempt_count) == 0


async def test_a_group_that_moved_out_of_the_selection_is_excluded_not_relabelled(
    session_maker, ops_signing, send_fence
) -> None:
    """§46.5: a move out of the chosen groups excludes, and never sends silently.

    The person was chosen as «delivered, read not confirmed». A proven read
    arrives between the preview and the confirmation, so they are now in a group
    the operator did not select — and the row is excluded with that reason
    rather than quietly sent under the old selection.
    """
    from altegio_bot.models.models import (
        DELIVERY_RECEIPT_KIND_READ,
        INBOUND_AUTHENTICITY_SIGNED,
        EasyWeekVoucherProductionDeliveryReceipt,
    )

    batch_id, _reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    async with session_maker() as session:
        item = (
            await session.execute(
                select(EasyWeekVoucherProductionBatchItem).where(
                    EasyWeekVoucherProductionBatchItem.batch_id == batch_id
                )
            )
        ).scalar_one()
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(EasyWeekVoucherProductionBatchItem, int(item.id))
            assert row is not None
            row.read_at = NOW
            session.add(
                EasyWeekVoucherProductionDeliveryReceipt(
                    fact_key="late-read-proof",
                    kind=DELIVERY_RECEIPT_KIND_READ,
                    authenticity=INBOUND_AUTHENTICITY_SIGNED,
                    item_id=int(item.id),
                    batch_id=batch_id,
                    slot=int(item.slot),
                    provider_message_id=str(item.provider_message_id),
                    observed_sender_digest=SENDER_DIGEST,
                    observed_recipient_digest=str(recipient_route_digest(person_phone(1))),
                    route_proven=True,
                    observed_at=NOW,
                    event_at=NOW,
                    evidence={},
                )
            )

    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.ready
    assert plan.approval is not None and plan.approval.approved_recipient_count == 0
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None and outcome.skipped == 1
    rows = await rows_of(session_maker, run_id=outcome.run.id)
    assert rows[1].status == send_ids.SKIPPED
    assert rows[1].reason_code == "followup_group_moved_out_of_selection"


async def test_a_reply_after_the_preview_excludes_the_person(session_maker, ops_signing, send_fence) -> None:
    """§46.5: somebody who has answered is not written to again."""
    from altegio_bot.models.models import INBOUND_AUTHENTICITY_SIGNED, WhatsAppInboundReceipt

    batch_id, _reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppInboundReceipt(
                    receipt_key="reply-1",
                    message_index=0,
                    wa_address=person_phone(1).lstrip("+"),
                    command="none",
                    authenticity=INBOUND_AUTHENTICITY_SIGNED,
                    observed_at=NOW,
                    event_at=NOW,
                    # Resolved, so it is a REPLY rather than a pending hold: the
                    # two are different facts and must not be tested as one.
                    resolved_at=NOW,
                    resolution="noop",
                )
            )

    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.approval is not None and plan.approval.approved_recipient_count == 0
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    rows = await rows_of(session_maker, run_id=outcome.run.id)
    assert rows[1].reason_code == "followup_customer_replied"


async def test_an_approval_cannot_carry_any_action_but_the_one(session_maker, ops_signing, send_fence) -> None:
    """The action is a column with a CHECK, not an implication.

    So an approval can never be spent on a different verb than the one the
    operator read — not by a payload, and not by a row somebody edited.
    """
    from altegio_bot.models.models import EasyWeekVoucherFollowupApproval

    batch_id, _reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.approval is not None
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                row = await session.get(EasyWeekVoucherFollowupApproval, int(plan.approval.id))
                assert row is not None
                row.action = "followup_refund"
                await session.flush()


async def test_no_lock_is_held_while_the_external_request_is_in_flight(session_maker, ops_signing, send_fence) -> None:
    """§46.5: no row lock and no advisory lock may outlive the claim's commit.

    Proven from inside the request rather than by reading the code. While the
    POST is in flight the test takes, in a separate connection and with a short
    ``lock_timeout``, both of the locks the claim held: this recipient's consent
    ADDRESS advisory lock and the run's own row. If either were still held, the
    acquisition would block and this would fail with a lock timeout instead of
    returning.
    """
    batch_id, reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    run_id = outcome.run.id
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None
    observed: dict[str, Any] = {}

    class LockProbingSender(CountingSender):
        async def send_voucher_template(self, **kwargs: Any) -> Any:
            from altegio_bot.whatsapp_consent import lock_consent_addresses

            async with session_maker() as probe:
                async with probe.begin():
                    await probe.execute(text("SET LOCAL lock_timeout = '1500ms'"))
                    # The consent aggregate's own lock, for the number this very
                    # request is writing to.
                    await lock_consent_addresses(probe, [person_phone(1).lstrip("+")])
                    # And the run row the claim took FOR UPDATE.
                    await probe.execute(
                        text("SELECT id FROM easyweek_voucher_followup_runs WHERE id = :id FOR UPDATE"),
                        {"id": run_id},
                    )
                    observed["acquired"] = True
            return await super().send_voucher_template(**kwargs)

    sender = LockProbingSender()
    report = await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert observed.get("acquired") is True
    assert sender.count == 1
    assert report.accepted == 1


# ===========================================================================
# Review round 1 — the seven confirmed defects, as regressions
# ===========================================================================
# Each test below reproduces a path that really sent, or really lost a stop, on
# the reviewed commit. They are written as statements about the contract, so
# they keep their meaning once the hole is closed rather than describing the
# patch.


async def _set_card_phone(session_maker, *, index: int, phone: str) -> None:
    """Move one synthetic card's number, as a reassignment would.

    The card and the campaign row are both moved, because §46.5's identity
    proof compares them: moving only one of them is a different defect (an
    identity conflict), and these tests are about the row being eligible again.
    """
    from altegio_bot.models.models import CampaignRecipient, Client

    async with session_maker() as session:
        async with session.begin():
            card = (
                await session.execute(select(Client).where(Client.easyweek_customer_uuid == person_uuid(index)))
            ).scalar_one()
            card.phone_e164 = phone
            recipient = (
                await session.execute(select(CampaignRecipient).where(CampaignRecipient.client_id == int(card.id)))
            ).scalar_one()
            recipient.phone_e164 = phone


# ---------------------------------------------------------------------------
# Fix 1 — the original mailing's STOP and the follow-up claim are one order
# ---------------------------------------------------------------------------


async def test_a_source_stop_committed_after_the_confirmation_produces_zero_requests(
    session_maker, ops_signing, send_fence
) -> None:
    """§46.5: a terminal stop of the original mailing forbids executing its follow-up.

    The confirmation happened while the batch was healthy, so this is the window
    the claim is responsible for — not the one the confirmation covers.
    """
    batch_id, reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None

    await production_ledger.request_stop(session_maker, batch_id=batch_id, requested_by="owner")

    sender = CountingSender()
    report = await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert sender.count == 0
    assert report.stopped is True
    rows = await rows_of(session_maker, run_id=claimed.id)
    assert all(int(row.attempt_count) == 0 for row in rows.values())


async def test_the_claim_waits_for_the_source_batch_header_and_then_sees_the_stop(
    session_maker, ops_signing, send_fence
) -> None:
    """The ordering is a lock, not two reads that happen to be in the right order.

    A transaction holds the original batch header and writes the stop inside it.
    The claim is started while that is held: it must BLOCK — asserted, not
    assumed — and once the holder commits it must see the stop and refuse. On
    the reviewed commit the claim read the stop without the header and could
    prove "no stop" while exactly this was in flight.
    """
    from altegio_bot.campaigns.easyweek_voucher_followup.eligibility import consent_snapshot
    from altegio_bot.models.models import (
        EasyWeekVoucherProductionBatch,
        EasyWeekVoucherProductionStopRequest,
    )

    batch_id, _reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None
    rows = await rows_of(session_maker, run_id=outcome.run.id)
    async with session_maker() as session:
        snapshot = await consent_snapshot(session, phone=person_phone(1), easyweek_customer_uuid=person_uuid(1))

    blocked: list[bool] = []
    async with session_maker() as holder:
        transaction = await holder.begin()
        await holder.get(EasyWeekVoucherProductionBatch, batch_id, with_for_update=True)
        holder.add(
            EasyWeekVoucherProductionStopRequest(
                batch_id=batch_id,
                requested_by="owner",
                requested_at=NOW,
                stop_count=1,
            )
        )
        await holder.flush()

        task = asyncio.create_task(
            ledger_module.claim_send(
                session_maker,
                run_id=outcome.run.id,
                send_id=int(rows[1].id),
                owner="executor-1",
                address=person_phone(1),
                easyweek_customer_uuid=person_uuid(1),
                expected_consent_fingerprint=snapshot.fingerprint,
                sender_digest=SENDER_DIGEST,
                recipient_digest=str(recipient_route_digest(person_phone(1))),
                source_batch_id=batch_id,
            )
        )
        # Several event-loop turns: enough for the claim to reach the header and
        # wait on it, and not enough for it to finish if it is waiting.
        for _ in range(20):
            await asyncio.sleep(0.01)
            if task.done():
                break
        blocked.append(not task.done())
        await transaction.commit()
        claim = await task

    assert blocked == [True], "the claim must wait for the source batch header, not read around it"
    assert claim.granted is False
    assert claim.reason == send_ids.SOURCE_TERMINAL_STOP
    rows = await rows_of(session_maker, run_id=outcome.run.id)
    assert int(rows[1].attempt_count) == 0


async def test_an_attempt_committed_before_the_stop_is_one_attempt_and_the_rest_are_stopped(
    session_maker, ops_signing, send_fence
) -> None:
    """The other order, and its honest outcome.

    The first row's attempt is already spent when the operator presses stop, so
    it keeps it — a request that may have left cannot be pretended cancelled —
    and every row that had not started is tombstoned.
    """
    batch_id, reader = await _ready(
        session_maker, slots=[{"state": "delivered"}, {"state": "delivered"}, {"state": "delivered"}]
    )
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    run_id = outcome.run.id
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None

    class StopAfterFirst(CountingSender):
        async def send_voucher_template(self, **kwargs: Any) -> Any:
            answer = await super().send_voucher_template(**kwargs)
            if len(self.calls) == 1:
                await production_ledger.request_stop(session_maker, batch_id=batch_id, requested_by="owner")
            return answer

    sender = StopAfterFirst()
    report = await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert sender.count == 1
    assert report.stopped is True
    rows = await rows_of(session_maker, run_id=run_id)
    assert int(rows[1].attempt_count) == 1
    assert {rows[2].status, rows[3].status} == {send_ids.STOPPED}


async def test_a_repeated_source_stop_changes_nothing_about_a_spent_attempt(
    session_maker, ops_signing, send_fence
) -> None:
    batch_id, reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    sender = CountingSender()
    report = await _run(session_maker, batch_id=batch_id, sender=sender, reader=reader)
    assert sender.count == 1
    for _ in range(3):
        await production_ledger.request_stop(session_maker, batch_id=batch_id, requested_by="owner")
    rows = await rows_of(session_maker, run_id=report.run_id)
    assert rows[1].status == send_ids.ACCEPTED
    assert int(rows[1].attempt_count) == 1


# ---------------------------------------------------------------------------
# Fix 2 — the confirmation is bound to the exact offered identities
# ---------------------------------------------------------------------------


async def test_a_recipient_who_regained_eligibility_after_the_offer_is_not_added_to_the_run(
    session_maker, ops_signing, send_fence
) -> None:
    """The review's own reproduction, as a contract statement.

    Two recipients in the preview; the second one's card number does not match
    when the plan is built, so the offer covers ONE. The number is then fixed and
    the operator confirms the one they were shown. On the reviewed commit this
    created two planned rows and the executor sent twice.
    """
    batch_id, reader = await _ready(session_maker)
    await _set_card_phone(session_maker, index=2, phone="+4915100099999")

    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.approval is not None
    assert plan.approval.approved_recipient_count == 1
    assert plan.approval.offered_source_item_ids is not None
    assert len(plan.approval.offered_source_item_ids) == 1

    # The obstacle disappears between the offer and the confirmation.
    await _set_card_phone(session_maker, index=2, phone=str(person(2)["phone"]))

    outcome = await confirm(session_maker, plan=plan, count=1)
    assert outcome.created is True
    assert outcome.planned == 1
    assert outcome.skipped == 1
    rows = await rows_of(session_maker, run_id=outcome.run.id)
    assert rows[1].status == send_ids.PLANNED
    assert rows[2].status == send_ids.SKIPPED
    assert rows[2].reason_code == send_ids.NOT_OFFERED

    sender = CountingSender()
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None
    report = await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert sender.count == 1
    assert report.attempted == 1


async def test_one_person_cannot_be_swapped_for_another_at_the_same_count(
    session_maker, ops_signing, send_fence
) -> None:
    """A count comparison cannot see a substitution; pinned identities can.

    The offer covers slot 1 only. Then slot 1 breaks and slot 2 heals, so the
    number of eligible recipients is still one — and it is a different person.
    Nothing is sent: the offered row is excluded with its own reason and the
    substitute was never offered.
    """
    batch_id, reader = await _ready(session_maker)
    await _set_card_phone(session_maker, index=2, phone="+4915100099999")
    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.approval is not None and plan.approval.approved_recipient_count == 1

    await _set_card_phone(session_maker, index=1, phone="+4915100088888")
    await _set_card_phone(session_maker, index=2, phone=str(person(2)["phone"]))

    outcome = await confirm(session_maker, plan=plan, count=1)
    assert outcome.created is True
    assert outcome.planned == 0
    rows = await rows_of(session_maker, run_id=outcome.run.id)
    assert rows[1].status == send_ids.SKIPPED
    assert rows[1].reason_code == "followup_identity_conflict"
    assert rows[2].status == send_ids.SKIPPED
    assert rows[2].reason_code == send_ids.NOT_OFFERED

    sender = CountingSender()
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None
    await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert sender.count == 0


async def test_an_approval_with_no_pinned_offer_cannot_be_confirmed(session_maker, ops_signing, send_fence) -> None:
    """Fail closed: an offer whose identities are unknown authorises nobody.

    Only reachable for a row written before the column existed. Re-signed after
    the column is cleared, so the refusal is about the missing offer and not
    about a signature that no longer verifies.
    """
    from altegio_bot.models.models import EasyWeekVoucherFollowupApproval

    batch_id, _reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.approval is not None
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(EasyWeekVoucherFollowupApproval, int(plan.approval.id))
            assert row is not None
            row.offered_source_item_ids = None
            row.plan_signature = ledger_module.plan_signature(ledger_module._approval_material(row))
            signature = row.plan_signature

    refused = await confirm(session_maker, plan=plan, signature=signature)
    assert refused.run is None
    assert refused.reasons == (send_ids.OFFER_UNPROVEN,)


async def test_editing_the_pinned_offer_breaks_the_signature(session_maker, ops_signing, send_fence) -> None:
    """The identities are part of what the keyed signature covers."""
    from altegio_bot.models.models import EasyWeekVoucherFollowupApproval

    batch_id, _reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.approval is not None
    offered = list(plan.approval.offered_source_item_ids or ())
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(EasyWeekVoucherFollowupApproval, int(plan.approval.id))
            assert row is not None
            row.offered_source_item_ids = offered[:-1]

    refused = await confirm(session_maker, plan=plan)
    assert refused.reasons == (send_ids.APPROVAL_SIGNATURE_INVALID,)


async def test_an_unchanged_offered_composition_is_confirmed_in_full(session_maker, ops_signing, send_fence) -> None:
    """The positive control for the whole binding: nothing moved, everything sends."""
    batch_id, reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.approval is not None and plan.approval.approved_recipient_count == 2
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.planned == 2 and outcome.skipped == 0
    sender = CountingSender()
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None
    report = await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert sender.count == 2 and report.accepted == 2


# ---------------------------------------------------------------------------
# Fix 3 — the template and the sender are re-proven before every attempt
# ---------------------------------------------------------------------------


async def _deactivate_template(session_maker) -> None:
    from altegio_bot.models.models import MessageTemplate

    async with session_maker() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(MessageTemplate).where(MessageTemplate.code == message.FOLLOWUP_TEMPLATE_CODE)
                )
            ).scalar_one()
            row.is_active = False


async def _deactivate_sender(session_maker) -> None:
    from altegio_bot.models.models import WhatsAppSender

    async with session_maker() as session:
        async with session.begin():
            row = (await session.execute(select(WhatsAppSender))).scalars().first()
            assert row is not None
            row.is_active = False


async def test_a_template_deactivated_after_the_confirmation_sends_nobody(
    session_maker, ops_signing, send_fence
) -> None:
    """§46.6: the approved contract is proven before each attempt, not once per plan.

    On the reviewed commit the local row was proven at the plan and at the
    confirmation and never again, so switching it off afterwards changed
    nothing and the executor still sent.
    """
    batch_id, reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None

    await _deactivate_template(session_maker)

    sender = CountingSender()
    report = await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert sender.count == 0
    assert report.skipped == 2
    rows = await rows_of(session_maker, run_id=claimed.id)
    assert {row.reason_code for row in rows.values()} == {"followup_template_unproven"}
    assert all(int(row.attempt_count) == 0 for row in rows.values())


async def test_a_template_switched_off_between_two_rows_stops_the_second(
    session_maker, ops_signing, send_fence
) -> None:
    """Per ATTEMPT, not per run: the first row's proof does not cover the second."""
    batch_id, reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None

    class DeactivateAfterFirst(CountingSender):
        async def send_voucher_template(self, **kwargs: Any) -> Any:
            answer = await super().send_voucher_template(**kwargs)
            if len(self.calls) == 1:
                await _deactivate_template(session_maker)
            return answer

    sender = DeactivateAfterFirst()
    await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert sender.count == 1
    rows = await rows_of(session_maker, run_id=claimed.id)
    assert rows[1].status == send_ids.ACCEPTED
    assert rows[2].status == send_ids.SKIPPED
    assert rows[2].reason_code == "followup_template_unproven"


async def test_an_edited_local_template_body_sends_nobody(session_maker, ops_signing, send_fence) -> None:
    """A row that merely exists is not the contract; its body has to match."""
    from altegio_bot.models.models import MessageTemplate

    batch_id, reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None
    async with session_maker() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(MessageTemplate).where(MessageTemplate.code == message.FOLLOWUP_TEMPLATE_CODE)
                )
            ).scalar_one()
            row.body = "Hallo {client_name}, {booking_link}."

    sender = CountingSender()
    await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert sender.count == 0


@pytest.mark.parametrize("status", ["PAUSED", "PENDING", "DISABLED"])
async def test_a_meta_template_that_is_not_approved_right_now_sends_nobody(
    session_maker, ops_signing, send_fence, status
) -> None:
    """The live half, asked in THIS evaluation. A previous proof is not evidence."""
    from altegio_bot.tests.easyweek_voucher_followup_fixtures import approved_meta_templates

    batch_id, reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None

    listing = approved_meta_templates()
    listing[0]["status"] = status
    sender = CountingSender()
    await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender, templates=TemplateReader(listing)),
    )
    assert sender.count == 0
    rows = await rows_of(session_maker, run_id=claimed.id)
    assert {row.reason_code for row in rows.values()} == {"followup_template_unproven"}


async def test_a_meta_listing_that_cannot_be_read_sends_nobody(session_maker, ops_signing, send_fence) -> None:
    """An unreadable listing is unknown, and unknown is not permission."""
    batch_id, reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None

    sender = CountingSender()
    await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender, templates=TemplateReader([])),
    )
    assert sender.count == 0


async def test_a_sender_switched_off_between_two_rows_stops_the_second(session_maker, ops_signing, send_fence) -> None:
    """The route is re-proven per attempt too, and nothing falls back to another."""
    batch_id, reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None

    class DeactivateSenderAfterFirst(CountingSender):
        async def send_voucher_template(self, **kwargs: Any) -> Any:
            answer = await super().send_voucher_template(**kwargs)
            if len(self.calls) == 1:
                await _deactivate_sender(session_maker)
            return answer

    sender = DeactivateSenderAfterFirst()
    await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert sender.count == 1
    rows = await rows_of(session_maker, run_id=claimed.id)
    assert rows[1].status == send_ids.ACCEPTED
    assert rows[2].status == send_ids.SKIPPED
    assert rows[2].reason_code == "followup_sender_unproven"


async def test_a_sender_repointed_at_another_number_sends_nobody(session_maker, ops_signing, send_fence) -> None:
    """§46.1: the repeat message stays on the original send geography."""
    from altegio_bot.models.models import WhatsAppSender

    batch_id, reader = await _ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None
    async with session_maker() as session:
        async with session.begin():
            row = (await session.execute(select(WhatsAppSender))).scalars().first()
            assert row is not None
            row.phone_number_id = "222222222222222"

    sender = CountingSender()
    report = await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert sender.count == 0
    assert report.reasons == ("followup_sender_unproven",)
    # Not terminal: a sender that was repointed can be repointed back, so the
    # reservations are kept rather than tombstoned, and nothing was spent.
    rows = await rows_of(session_maker, run_id=claimed.id)
    assert {row.status for row in rows.values()} == {send_ids.PLANNED}
    assert all(int(row.attempt_count) == 0 for row in rows.values())


# ---------------------------------------------------------------------------
# Fix 4 — the local conditions are re-proven AFTER the external reads
# ---------------------------------------------------------------------------


class _MutatingHistoryReader(FakeHistoryReader):
    """A booking read that performs a local change while it is "in flight".

    This is the window the review found: the local pass had already run, the
    artifact and the history were being read, and anything that moved locally in
    between was simply not looked at again. Driving it from inside the reader is
    what makes the test deterministic — the change lands between the two local
    passes by construction, not by timing.
    """

    def __init__(self, *, during: Any, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._during = during
        self.fired = 0

    async def list_customer_bookings(self, customer_uuid: str, page: int, **kwargs: Any) -> Any:
        if self.fired == 0:
            self.fired += 1
            await self._during()
        return await super().list_customer_bookings(customer_uuid, page, **kwargs)


async def _mark_read(session_maker, *, batch_id: int, slot: int, index: int) -> None:
    """A PROVEN read receipt lands for this slot's original message."""
    from altegio_bot.models.models import (
        DELIVERY_RECEIPT_KIND_READ,
        INBOUND_AUTHENTICITY_SIGNED,
        EasyWeekVoucherProductionDeliveryReceipt,
    )

    async with session_maker() as session:
        item = (
            await session.execute(
                select(EasyWeekVoucherProductionBatchItem)
                .where(EasyWeekVoucherProductionBatchItem.batch_id == int(batch_id))
                .where(EasyWeekVoucherProductionBatchItem.slot == int(slot))
            )
        ).scalar_one()
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(EasyWeekVoucherProductionBatchItem, int(item.id))
            assert row is not None
            row.read_at = NOW
            session.add(
                EasyWeekVoucherProductionDeliveryReceipt(
                    fact_key=f"late-read-{batch_id}-{slot}",
                    kind=DELIVERY_RECEIPT_KIND_READ,
                    authenticity=INBOUND_AUTHENTICITY_SIGNED,
                    item_id=int(item.id),
                    batch_id=int(batch_id),
                    slot=int(slot),
                    provider_message_id=str(item.provider_message_id),
                    observed_sender_digest=SENDER_DIGEST,
                    observed_recipient_digest=str(recipient_route_digest(person_phone(index))),
                    route_proven=True,
                    observed_at=NOW,
                    event_at=NOW,
                    evidence={},
                )
            )


async def test_a_read_that_lands_during_the_external_reads_excludes_a_delivered_only_row(
    session_maker, ops_signing, send_fence
) -> None:
    """The review's own reproduction. Only ``delivered_unread`` was selected.

    A proven read arrives while the booking history is being read, so this
    person is no longer in a group the operator chose. On the reviewed commit the
    verdict still carried the first pass's values and the executor sent to them.
    """
    batch_id = await seed_ready_batch(session_maker, slots=[{"state": "delivered"}])
    base = FakeHistoryReader(batch_id=batch_id)
    await build_preview(session_maker, batch_id=batch_id, reader=base)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None and outcome.planned == 1
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None

    async def during() -> None:
        await _mark_read(session_maker, batch_id=batch_id, slot=1, index=1)

    reader = _MutatingHistoryReader(during=during, batch_id=batch_id)
    sender = CountingSender()
    report = await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert reader.fired == 1
    assert sender.count == 0
    assert report.skipped == 1
    rows = await rows_of(session_maker, run_id=claimed.id)
    assert rows[1].status == send_ids.SKIPPED
    assert rows[1].reason_code == "followup_group_moved_out_of_selection"
    assert int(rows[1].attempt_count) == 0


async def test_a_move_between_two_selected_groups_still_sends(session_maker, ops_signing, send_fence) -> None:
    """§46.5 allows the move when BOTH groups were selected and the text is one.

    The positive control for the test above: the same event, with the operator
    having chosen both groups, is not a reason to exclude anybody.
    """
    batch_id = await seed_ready_batch(session_maker, slots=[{"state": "delivered"}])
    base = FakeHistoryReader(batch_id=batch_id)
    await build_preview(
        session_maker,
        batch_id=batch_id,
        reader=base,
        groups=(FOLLOWUP_GROUP_READ, "delivered_unread"),
    )
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None and outcome.planned == 1
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None

    async def during() -> None:
        await _mark_read(session_maker, batch_id=batch_id, slot=1, index=1)

    reader = _MutatingHistoryReader(during=during, batch_id=batch_id)
    sender = CountingSender()
    report = await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert reader.fired == 1
    assert sender.count == 1
    assert report.accepted == 1


async def test_a_reply_processed_during_the_external_reads_excludes_the_person(
    session_maker, ops_signing, send_fence
) -> None:
    from altegio_bot.models.models import INBOUND_AUTHENTICITY_SIGNED, WhatsAppInboundReceipt

    batch_id = await seed_ready_batch(session_maker, slots=[{"state": "delivered"}])
    base = FakeHistoryReader(batch_id=batch_id)
    await build_preview(session_maker, batch_id=batch_id, reader=base)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None

    async def during() -> None:
        async with session_maker() as session:
            async with session.begin():
                session.add(
                    WhatsAppInboundReceipt(
                        receipt_key="reply-during-history",
                        message_index=0,
                        wa_address=person_phone(1).lstrip("+"),
                        command="none",
                        authenticity=INBOUND_AUTHENTICITY_SIGNED,
                        observed_at=NOW,
                        event_at=NOW,
                        resolved_at=NOW,
                        resolution="noop",
                    )
                )

    reader = _MutatingHistoryReader(during=during, batch_id=batch_id)
    sender = CountingSender()
    await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert reader.fired == 1
    assert sender.count == 0
    rows = await rows_of(session_maker, run_id=claimed.id)
    assert rows[1].reason_code == "followup_customer_replied"


async def test_an_identity_that_moves_during_the_external_reads_does_not_substitute_the_addressee(
    session_maker, ops_signing, send_fence
) -> None:
    batch_id = await seed_ready_batch(session_maker, slots=[{"state": "delivered"}])
    base = FakeHistoryReader(batch_id=batch_id)
    await build_preview(session_maker, batch_id=batch_id, reader=base)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None

    async def during() -> None:
        await _set_card_phone(session_maker, index=1, phone="+4915100077777")

    reader = _MutatingHistoryReader(during=during, batch_id=batch_id)
    sender = CountingSender()
    await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert reader.fired == 1
    assert sender.count == 0
    rows = await rows_of(session_maker, run_id=claimed.id)
    assert rows[1].reason_code == "followup_identity_conflict"


async def test_a_refusal_that_lands_during_the_external_reads_produces_zero_requests(
    session_maker, ops_signing, send_fence
) -> None:
    """STOP before the boundary, with the boundary reached from the other side."""
    from altegio_bot.whatsapp_consent import ProjectedRefusal, apply_projected_refusal

    batch_id = await seed_ready_batch(session_maker, slots=[{"state": "delivered"}])
    base = FakeHistoryReader(batch_id=batch_id)
    await build_preview(session_maker, batch_id=batch_id, reader=base)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None

    async def during() -> None:
        async with session_maker() as session:
            async with session.begin():
                await apply_projected_refusal(
                    session,
                    ProjectedRefusal(
                        wa_address=person_phone(1).lstrip("+"),
                        origin="test_projection",
                        observed_at=NOW,
                        event_at=NOW,
                        authenticity="signature_verified",
                    ),
                )

    reader = _MutatingHistoryReader(during=during, batch_id=batch_id)
    sender = CountingSender()
    await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert reader.fired == 1
    assert sender.count == 0
    rows = await rows_of(session_maker, run_id=claimed.id)
    assert rows[1].reason_code == "followup_consent_refused"
    assert int(rows[1].attempt_count) == 0


# ---------------------------------------------------------------------------
# Fix 6 — a proven failure outranks Meta having accepted the request
# ---------------------------------------------------------------------------


async def test_a_proven_failed_after_accepted_moves_the_row_and_says_why(
    session_maker, ops_signing, send_fence
) -> None:
    """Accepting the request is not a proven outcome; a signed failure is.

    On the reviewed commit ``failed`` ranked below ``accepted``, so the fact was
    stored and the row went on reading ``принято провайдером`` — the screen kept
    claiming a message was on its way that Meta had said it would not deliver.
    """
    _batch, row, _reader = await _sent_one(session_maker)
    outcome = await _deliver(
        session_maker,
        status_payload(
            provider_message_id="wamid.followup.A",
            kind="failed",
            recipient_phone=person_phone(1),
            error_code=131026,
        ),
        kind="failed",
    )
    assert outcome.claimed is True
    assert outcome.match_state == FOLLOWUP_CALLBACK_CONFIRMED
    assert outcome.advanced is True
    assert outcome.contradiction is False
    rows = await rows_of(session_maker, run_id=int(row.run_id))
    assert rows[1].status == send_ids.FAILED
    assert rows[1].reason_code == callbacks_module.CALLBACK_FAILED_REASON
    assert rows[1].failed_at is not None
    # The attempt stays spent and no contradiction is claimed: the provider did
    # not contradict itself, it finished its account of one message.
    assert int(rows[1].attempt_count) == 1
    assert int(rows[1].contradiction_count) == 0

    # And the live report an operator reads says so.
    counters = await ledger_module.run_counters(session_maker, run_id=int(row.run_id))
    assert counters.get(send_ids.FAILED) == 1
    assert counters.get(send_ids.ACCEPTED) is None


async def test_a_proven_failed_after_an_unknown_moves_the_row(session_maker, ops_signing, send_fence) -> None:
    """A status arriving for a message whose answer was lost is better news."""
    batch_id, reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import DELIVERY_UNKNOWN, DeliveryOutcome

    # An unknown that nevertheless carries a message id: a 2xx whose body could
    # not be read, a redirect, a timeout after the id was handed back. That is
    # the row a later callback can still find.
    sender = CountingSender(
        answers=[
            DeliveryOutcome(
                outcome=DELIVERY_UNKNOWN,
                reason="meta_timeout",
                provider_message_id="wamid.followup.U",
            )
        ]
    )
    report = await _run(session_maker, batch_id=batch_id, sender=sender, reader=reader)
    rows = await rows_of(session_maker, run_id=report.run_id)
    assert rows[1].status == send_ids.UNKNOWN

    outcome = await _deliver(
        session_maker,
        status_payload(
            provider_message_id="wamid.followup.U",
            kind="failed",
            recipient_phone=person_phone(1),
            error_code=131026,
        ),
        kind="failed",
    )
    assert outcome.advanced is True
    rows = await rows_of(session_maker, run_id=report.run_id)
    assert rows[1].status == send_ids.FAILED
    assert int(rows[1].attempt_count) == 1


async def test_an_early_failed_callback_is_bound_and_applied(session_maker, ops_signing, send_fence) -> None:
    """A failure can overtake the commit that records the message id."""
    batch_id, reader = await _ready(session_maker, slots=[{"state": "delivered"}])
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await ledger_module.claim_next_run(session_maker, owner="executor-1")
    assert claimed is not None
    phone = person_phone(1)

    class EarlyFailureSender(CountingSender):
        async def send_voucher_template(self, **kwargs: Any) -> Any:
            async with session_maker() as session:
                async with session.begin():
                    kept = await callbacks_module.record_unmatched_followup_callback(
                        session,
                        status=status_payload(
                            provider_message_id="wamid.followup.EF",
                            kind="failed",
                            recipient_phone=phone,
                            error_code=131026,
                        ),
                        kind="failed",
                        signature_verified=True,
                    )
            assert kept.stored is True
            return accepted("wamid.followup.EF")

    sender = EarlyFailureSender()
    await dispatch_module.execute_run(
        session_maker,
        run=claimed,
        owner="executor-1",
        transports=StubTransports(reader=reader, sender=sender),
    )
    rows = await rows_of(session_maker, run_id=claimed.id)
    assert rows[1].status == send_ids.FAILED
    assert rows[1].failed_at is not None
    assert int(rows[1].attempt_count) == 1


async def test_a_failed_callback_does_not_touch_the_original_message_or_add_an_attempt(
    session_maker, ops_signing, send_fence
) -> None:
    _batch, row, _reader = await _sent_one(session_maker)
    async with session_maker() as session:
        before = (
            await session.execute(
                select(
                    EasyWeekVoucherProductionBatchItem.status,
                    EasyWeekVoucherProductionBatchItem.send_attempt_count,
                    EasyWeekVoucherProductionBatchItem.delivered_at,
                    EasyWeekVoucherProductionBatchItem.read_at,
                ).where(EasyWeekVoucherProductionBatchItem.id == int(row.source_item_id))
            )
        ).all()
    await _deliver(
        session_maker,
        status_payload(
            provider_message_id="wamid.followup.A",
            kind="failed",
            recipient_phone=person_phone(1),
            error_code=131026,
        ),
        kind="failed",
    )
    async with session_maker() as session:
        after = (
            await session.execute(
                select(
                    EasyWeekVoucherProductionBatchItem.status,
                    EasyWeekVoucherProductionBatchItem.send_attempt_count,
                    EasyWeekVoucherProductionBatchItem.delivered_at,
                    EasyWeekVoucherProductionBatchItem.read_at,
                ).where(EasyWeekVoucherProductionBatchItem.id == int(row.source_item_id))
            )
        ).all()
    assert after == before
    rows = await rows_of(session_maker, run_id=int(row.run_id))
    assert int(rows[1].attempt_count) == 1


async def test_a_duplicate_and_a_reordered_failure_change_nothing_twice(session_maker, ops_signing, send_fence) -> None:
    _batch, row, _reader = await _sent_one(session_maker)
    payload = status_payload(
        provider_message_id="wamid.followup.A",
        kind="failed",
        recipient_phone=person_phone(1),
        error_code=131026,
    )
    first = await _deliver(session_maker, payload, kind="failed")
    second = await _deliver(session_maker, payload, kind="failed")
    assert first.advanced is True
    assert second.advanced is False
    # And a DELIVERED arriving after the failure is a proven success, which
    # outranks it — §46.3's precedence, from the other direction.
    late = await _deliver(
        session_maker,
        status_payload(provider_message_id="wamid.followup.A", kind="delivered", recipient_phone=person_phone(1)),
        kind="delivered",
    )
    assert late.advanced is True
    rows = await rows_of(session_maker, run_id=int(row.run_id))
    assert rows[1].status == send_ids.DELIVERED
