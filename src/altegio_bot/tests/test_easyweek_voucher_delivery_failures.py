"""Proven delivery failures of the production voucher mailing (§46.3, PR-22.1).

The property under test is narrow and the negatives carry most of the weight:
only two things count as evidence that one original send failed, and everything
that merely looks like doubt must stay unknown.

The staging helpers come from ``test_easyweek_voucher_production_mailing``,
which owns the §42 stage sequence these tests build on. Imported rather than
copied on purpose: a second freeze/create/pay ladder would drift from the real
one, and these tests would then be asserting about a batch no operator could
produce.
"""

from __future__ import annotations

import asyncio
import hashlib
import uuid as uuid_module
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import (
    DELIVERY_REJECTED,
    DELIVERY_UNKNOWN,
    REASON_NO_MESSAGE_ID,
    REASON_REJECTED,
    REASON_TIMEOUT,
    DeliveryOutcome,
)
from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production.delivery_evidence import (
    CONFLICT_AFTER_SUCCESS,
    CONFLICT_FOREIGN_ROUTE,
)
from altegio_bot.campaigns.easyweek_voucher_production.identity import STAGE_DELIVER
from altegio_bot.models.models import (
    DELIVERY_FAILURE_CLASS_PERMANENT,
    DELIVERY_FAILURE_CLASS_UNKNOWN,
    DELIVERY_FAILURE_MATCH_CONFIRMED,
    DELIVERY_FAILURE_MATCH_CONFLICTING,
    DELIVERY_FAILURE_MATCH_UNMATCHED,
    DELIVERY_FAILURE_MATCH_UNPROVEN,
    DELIVERY_FAILURE_REASON_API_REJECTED,
    DELIVERY_FAILURE_REASON_CALLBACK_FAILED,
    DELIVERY_FAILURE_SOURCE_CALLBACK,
    DELIVERY_FAILURE_SOURCE_META_API,
    DELIVERY_RECEIPT_KIND_DELIVERED,
    DELIVERY_RECEIPT_KIND_READ,
    DELIVERY_RECEIPT_MATCH_CONFIRMED,
    DELIVERY_RECEIPT_MATCH_CONFLICTING,
    DELIVERY_RECEIPT_MATCH_UNMATCHED,
    INBOUND_AUTHENTICITY_SIGNED,
    INBOUND_AUTHENTICITY_UNVERIFIED,
    VOUCHER_PRODUCTION_ITEM_DELIVERED,
    VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED,
    VOUCHER_PRODUCTION_ITEM_READ,
    VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED,
    VOUCHER_PRODUCTION_ITEM_SEND_REJECTED,
    EasyWeekVoucherProductionBatch,
    EasyWeekVoucherProductionBatchAttempt,
    EasyWeekVoucherProductionBatchItem,
    EasyWeekVoucherProductionDeliveryFailure,
    EasyWeekVoucherProductionDeliveryReceipt,
    MessageJob,
    OutboxMessage,
    WhatsAppConsentSuppression,
    WhatsAppEvent,
)
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    PHONES,
    SENDER_PHONE_NUMBER_ID,
    FakeReader,
    FakeSender,
)
from altegio_bot.tests.test_easyweek_voucher_production_mailing import _apply, _paid_batch
from altegio_bot.utils import utcnow
from altegio_bot.whatsapp_consent import recipient_route_digest, sender_route_digest
from altegio_bot.workers import whatsapp_inbox_worker as worker_module

# The sender route the §42 stage sequence really uses, so a callback in these
# tests names the route the message actually took rather than an invented one.
SENDER_PNID = SENDER_PHONE_NUMBER_ID


async def _item(session_maker, batch_id: int, slot: int = 1) -> EasyWeekVoucherProductionBatchItem:
    async with session_maker() as session:
        return (
            await session.execute(
                select(EasyWeekVoucherProductionBatchItem)
                .where(EasyWeekVoucherProductionBatchItem.batch_id == batch_id)
                .where(EasyWeekVoucherProductionBatchItem.slot == slot)
            )
        ).scalar_one()


async def _attempt(session_maker, item_id: int) -> EasyWeekVoucherProductionBatchAttempt:
    async with session_maker() as session:
        return (
            await session.execute(
                select(EasyWeekVoucherProductionBatchAttempt).where(
                    EasyWeekVoucherProductionBatchAttempt.item_id == item_id
                )
            )
        ).scalar_one()


async def _facts(session_maker) -> list[EasyWeekVoucherProductionDeliveryFailure]:
    async with session_maker() as session:
        return list(
            (
                await session.scalars(
                    select(EasyWeekVoucherProductionDeliveryFailure).order_by(
                        EasyWeekVoucherProductionDeliveryFailure.id
                    )
                )
            ).all()
        )


def _status_payload(
    *,
    wamid: str,
    status: str = "failed",
    recipient: str,
    phone_number_id: str = SENDER_PNID,
    error_code: Any = 131026,
    epoch: int = 1_760_000_500,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "id": wamid,
        "status": status,
        "timestamp": str(epoch),
        "recipient_id": recipient,
    }
    if status == "failed":
        entry["errors"] = [
            {
                "code": error_code,
                "title": "Message undeliverable",
                "message": "Please do not log this",
                "error_data": {"details": "nor this"},
            }
        ]
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA-PROD",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {"phone_number_id": phone_number_id},
                            "statuses": [entry],
                        },
                    }
                ],
            }
        ],
    }


async def _deliver_status(
    session_maker,
    payload: dict[str, Any],
    *,
    signature_verified: bool = True,
) -> int:
    """Store one status webhook and run the real status path over it."""
    async with session_maker() as session:
        async with session.begin():
            event = WhatsAppEvent(
                dedupe_key=f"wa:status-{uuid_module.uuid4().hex}",
                status="received",
                query={},
                headers={},
                payload=payload,
                inbound_authenticity=(
                    INBOUND_AUTHENTICITY_SIGNED if signature_verified else INBOUND_AUTHENTICITY_UNVERIFIED
                ),
            )
            session.add(event)
            await session.flush()
            event_id = int(event.id)

    original = worker_module.SessionLocal
    try:
        worker_module.SessionLocal = session_maker  # type: ignore[assignment]
        await worker_module.process_one_event(event_id, _SilentProvider())
    finally:
        worker_module.SessionLocal = original
    return event_id


class _SilentProvider:
    """A provider that would raise if anything tried to send. Nothing should."""

    async def send(self, *args: Any, **kwargs: Any) -> str:  # pragma: no cover - must not run
        raise AssertionError("a status webhook must never send a message")

    async def send_template(self, *args: Any, **kwargs: Any) -> str:  # pragma: no cover - must not run
        raise AssertionError("a status webhook must never send a message")


async def _delivered_batch(session_maker, *, count: int = 1, outcomes: list[DeliveryOutcome] | None = None):
    """A batch taken all the way to a real DELIVER, with its route frozen."""
    reader = FakeReader(count=count)
    _run_id, batch_id, request = await _paid_batch(session_maker, reader, count=count)
    sender = FakeSender(outcomes=outcomes)
    report = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    return batch_id, report, sender


# ===========================================================================
# The route is frozen with the attempt
# ===========================================================================


async def test_the_attempt_freezes_the_route_it_actually_used(
    session_maker, production_configuration, binding_key
) -> None:
    batch_id, report, _sender = await _delivered_batch(session_maker)
    assert report.outcome == "applied"

    item = await _item(session_maker, batch_id)
    attempt = await _attempt(session_maker, int(item.id))
    assert attempt.recipient_digest == recipient_route_digest(PHONES[0])
    assert attempt.sender_phone_number_id_digest is not None
    # The digest, not the value: no phone number and no sender id is stored.
    assert PHONES[0] not in (attempt.recipient_digest or "")


# ===========================================================================
# What counts as proof
# ===========================================================================


async def test_a_failed_callback_becomes_a_confirmed_fact(session_maker, production_configuration, binding_key) -> None:
    batch_id, _report, _sender = await _delivered_batch(session_maker)
    item = await _item(session_maker, batch_id)
    assert item.provider_message_id is not None

    await _deliver_status(
        session_maker,
        _status_payload(wamid=item.provider_message_id, recipient=PHONES[0].lstrip("+")),
    )

    facts = await _facts(session_maker)
    assert len(facts) == 1
    fact = facts[0]
    assert fact.source == DELIVERY_FAILURE_SOURCE_CALLBACK
    assert fact.reason_code == DELIVERY_FAILURE_REASON_CALLBACK_FAILED
    assert fact.match_state == DELIVERY_FAILURE_MATCH_CONFIRMED
    assert fact.failure_class == DELIVERY_FAILURE_CLASS_PERMANENT
    assert fact.route_proven is True
    assert fact.authenticity == INBOUND_AUTHENTICITY_SIGNED
    assert fact.item_id == int(item.id)
    assert fact.meta_error_code == 131026
    # Observation and the proven event moment are kept apart.
    assert fact.event_at is not None
    assert fact.observed_at is not None


async def test_the_fact_stores_no_meta_text_at_all(session_maker, production_configuration, binding_key) -> None:
    """A Meta error string can quote the request it refused, so none is kept."""
    batch_id, _report, _sender = await _delivered_batch(session_maker)
    item = await _item(session_maker, batch_id)
    await _deliver_status(
        session_maker,
        _status_payload(wamid=item.provider_message_id or "", recipient=PHONES[0].lstrip("+")),
    )

    fact = (await _facts(session_maker))[0]
    blob = f"{fact.evidence}|{fact.reason_code}|{fact.failure_class}"
    assert "Please do not log this" not in blob
    assert "nor this" not in blob
    assert "undeliverable" not in blob.lower()
    assert PHONES[0] not in blob


async def test_a_non_numeric_error_code_is_not_stored_as_one(
    session_maker, production_configuration, binding_key
) -> None:
    """An untrusted field must not arrive where a number is expected."""
    batch_id, _report, _sender = await _delivered_batch(session_maker)
    item = await _item(session_maker, batch_id)
    await _deliver_status(
        session_maker,
        _status_payload(
            wamid=item.provider_message_id or "",
            recipient=PHONES[0].lstrip("+"),
            error_code="not-a-code",
        ),
    )

    fact = (await _facts(session_maker))[0]
    assert fact.meta_error_code is None
    assert fact.failure_class == DELIVERY_FAILURE_CLASS_UNKNOWN


async def test_a_meta_rejection_of_the_post_becomes_a_confirmed_fact(
    session_maker, production_configuration, binding_key
) -> None:
    batch_id, report, _sender = await _delivered_batch(
        session_maker,
        outcomes=[DeliveryOutcome(outcome=DELIVERY_REJECTED, reason=REASON_REJECTED, http_status=400)],
    )
    assert [entry.outcome for entry in report.slots] == ["rejected"]

    facts = await _facts(session_maker)
    assert len(facts) == 1
    assert facts[0].source == DELIVERY_FAILURE_SOURCE_META_API
    assert facts[0].reason_code == DELIVERY_FAILURE_REASON_API_REJECTED
    assert facts[0].match_state == DELIVERY_FAILURE_MATCH_CONFIRMED
    assert facts[0].http_status == 400
    assert facts[0].intent_uuid is not None
    item = await _item(session_maker, batch_id)
    assert item.status == VOUCHER_PRODUCTION_ITEM_SEND_REJECTED


# ===========================================================================
# What does NOT count as proof
# ===========================================================================


@pytest.mark.parametrize(
    "outcome",
    [
        DeliveryOutcome(outcome=DELIVERY_UNKNOWN, reason=REASON_TIMEOUT),
        DeliveryOutcome(outcome=DELIVERY_UNKNOWN, reason=REASON_NO_MESSAGE_ID, http_status=200),
    ],
)
async def test_doubt_is_never_recorded_as_a_failure(
    session_maker, production_configuration, binding_key, outcome: DeliveryOutcome
) -> None:
    """A timeout and a 2xx without an id are unknown, and unknown writes nothing."""
    await _delivered_batch(session_maker, outcomes=[outcome])

    assert await _facts(session_maker) == []


async def test_an_accepted_message_with_no_callback_is_not_a_failure(
    session_maker, production_configuration, binding_key
) -> None:
    batch_id, _report, _sender = await _delivered_batch(session_maker)
    item = await _item(session_maker, batch_id)

    assert item.status == VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED
    assert await _facts(session_maker) == []


async def test_a_slot_with_no_attempt_cannot_have_a_failure(
    session_maker, production_configuration, binding_key
) -> None:
    reader = FakeReader(count=1)
    _run_id, batch_id, _request = await _paid_batch(session_maker, reader, count=1)

    item = await _item(session_maker, batch_id)
    assert item.send_attempted_at is None
    assert await _facts(session_maker) == []


# ===========================================================================
# Callback rules
# ===========================================================================


async def test_a_duplicate_callback_is_one_fact(session_maker, production_configuration, binding_key) -> None:
    batch_id, _report, _sender = await _delivered_batch(session_maker)
    item = await _item(session_maker, batch_id)
    payload = _status_payload(wamid=item.provider_message_id or "", recipient=PHONES[0].lstrip("+"))

    await _deliver_status(session_maker, payload)
    await _deliver_status(session_maker, payload)

    assert len(await _facts(session_maker)) == 1


async def test_a_callback_naming_a_foreign_recipient_does_not_touch_the_slot(
    session_maker, production_configuration, binding_key
) -> None:
    batch_id, _report, _sender = await _delivered_batch(session_maker)
    item = await _item(session_maker, batch_id)

    await _deliver_status(
        session_maker,
        _status_payload(wamid=item.provider_message_id or "", recipient="4915177999999"),
    )

    fact = (await _facts(session_maker))[0]
    assert fact.match_state == DELIVERY_FAILURE_MATCH_CONFLICTING
    assert fact.evidence.get("conflict") == CONFLICT_FOREIGN_ROUTE
    assert fact.route_proven is False
    after = await _item(session_maker, batch_id)
    assert after.evidence.get("confirmed_delivery_failure") is None


async def test_a_callback_from_a_foreign_sender_does_not_touch_the_slot(
    session_maker, production_configuration, binding_key
) -> None:
    batch_id, _report, _sender = await _delivered_batch(session_maker)
    item = await _item(session_maker, batch_id)

    await _deliver_status(
        session_maker,
        _status_payload(
            wamid=item.provider_message_id or "",
            recipient=PHONES[0].lstrip("+"),
            phone_number_id="PNID-SOMEBODY-ELSE",
        ),
    )

    fact = (await _facts(session_maker))[0]
    assert fact.match_state == DELIVERY_FAILURE_MATCH_CONFLICTING
    assert fact.evidence.get("conflict") == CONFLICT_FOREIGN_ROUTE


async def test_read_is_stronger_than_delivered_and_both_survive_a_late_failed(
    session_maker, production_configuration, binding_key
) -> None:
    """§46.3 — success is never rolled back, and the contradiction is kept."""
    batch_id, _report, _sender = await _delivered_batch(session_maker)
    item = await _item(session_maker, batch_id)
    wamid = item.provider_message_id or ""
    recipient = PHONES[0].lstrip("+")

    await _deliver_status(session_maker, _status_payload(wamid=wamid, status="delivered", recipient=recipient))
    assert (await _item(session_maker, batch_id)).status == VOUCHER_PRODUCTION_ITEM_DELIVERED

    await _deliver_status(session_maker, _status_payload(wamid=wamid, status="read", recipient=recipient))
    assert (await _item(session_maker, batch_id)).status == VOUCHER_PRODUCTION_ITEM_READ

    # A `delivered` arriving after `read` must not walk the slot backwards.
    await _deliver_status(
        session_maker,
        _status_payload(wamid=wamid, status="delivered", recipient=recipient, epoch=1_760_000_900),
    )
    assert (await _item(session_maker, batch_id)).status == VOUCHER_PRODUCTION_ITEM_READ

    await _deliver_status(session_maker, _status_payload(wamid=wamid, recipient=recipient))

    after = await _item(session_maker, batch_id)
    assert after.status == VOUCHER_PRODUCTION_ITEM_READ
    assert after.read_at is not None
    fact = (await _facts(session_maker))[0]
    assert fact.match_state == DELIVERY_FAILURE_MATCH_CONFLICTING
    assert fact.evidence.get("conflict") == CONFLICT_AFTER_SUCCESS


async def test_an_early_callback_is_kept_and_bound_when_the_id_lands(
    session_maker, production_configuration, binding_key
) -> None:
    """§46.3 — a callback that overtakes the commit must not be lost.

    The whole race is staged honestly: the status arrives while the slot is
    merely ``send_claimed`` and has no identifier at all, so nothing in this
    database can match it. It is kept unmatched, and the acceptance that records
    the identifier binds it.
    """
    reader = FakeReader(count=1)
    _run_id, batch_id, _request = await _paid_batch(session_maker, reader, count=1)
    wamid = "wamid.EARLY-RACE"

    # The slot is claimed but not answered: exactly the in-flight moment.
    claim = await ledger_module.claim_send(
        session_maker,
        identity=await _identity_for(session_maker, batch_id),
        batch_id=batch_id,
        slot=1,
        plan_digest="d" * 64,
        live_guard_reproven_at=(await _item(session_maker, batch_id)).pay_verified_at,
        template_code="new_client_voucher_10eur_v2",
        meta_template_name="meta_template",
        template_language="de",
        sender_id=None,
        sender_phone_number_id=SENDER_PNID,
        destination_phone=PHONES[0],
    )
    assert claim.granted, claim.reason

    await _deliver_status(session_maker, _status_payload(wamid=wamid, recipient=PHONES[0].lstrip("+")))

    early = await _facts(session_maker)
    assert len(early) == 1
    assert early[0].match_state == DELIVERY_FAILURE_MATCH_UNMATCHED
    assert early[0].item_id is None
    assert early[0].evidence.get("awaiting_binding") is True

    recorded = await ledger_module.record_item_outcome(
        session_maker,
        batch_id=batch_id,
        slot=1,
        status=VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED,
        expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED}),
        provider_message_id=wamid,
        verified_field="provider_accepted_at",
        reconciliation_required=False,
        attempt_outcome="provider_accepted",
    )
    assert recorded.applied

    bound = (await _facts(session_maker))[0]
    assert bound.match_state == DELIVERY_FAILURE_MATCH_CONFIRMED
    item = await _item(session_maker, batch_id)
    assert bound.item_id == int(item.id)
    assert bound.route_proven is True
    assert item.evidence.get("confirmed_delivery_failure") is True


async def _identity_for(session_maker, batch_id: int) -> Any:
    """The batch identity the claim needs, rebuilt from the live snapshot.

    The runner's own helper, so the claim in this test is checked against
    exactly the identity a real deliver would present.
    """
    import altegio_bot.campaigns.easyweek_voucher_production.runner as runner_module

    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    identity = runner_module._identity_from_snapshot(snapshot)
    assert identity is not None
    return identity


async def test_a_foreign_failed_callback_is_not_kept_as_production_evidence(
    session_maker, production_configuration, binding_key
) -> None:
    """Ordinary campaign traffic has an OutboxMessage and never reaches here."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                OutboxMessage(
                    company_id=1,
                    phone_e164="+4915100000001",
                    template_code="some_campaign",
                    language="de",
                    body="body",
                    status="sent",
                    provider_message_id="wamid.ORDINARY",
                    scheduled_at=utcnow(),
                    sent_at=utcnow(),
                    message_source="bot",
                    meta={},
                )
            )

    await _deliver_status(
        session_maker,
        _status_payload(wamid="wamid.ORDINARY", recipient="4915100000001"),
    )

    assert await _facts(session_maker) == []
    async with session_maker() as session:
        row = (
            await session.execute(select(OutboxMessage).where(OutboxMessage.provider_message_id == "wamid.ORDINARY"))
        ).scalar_one()
    assert row.status == "failed"


# ===========================================================================
# A fact is not a permission
# ===========================================================================


async def test_a_confirmed_failure_creates_no_send_no_job_and_no_second_attempt(
    session_maker, production_configuration, binding_key
) -> None:
    batch_id, _report, sender = await _delivered_batch(session_maker)
    item = await _item(session_maker, batch_id)
    calls_before = sender.calls

    await _deliver_status(
        session_maker,
        _status_payload(wamid=item.provider_message_id or "", recipient=PHONES[0].lstrip("+")),
    )

    after = await _item(session_maker, batch_id)
    assert after.send_attempt_count == 1
    assert after.status == VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED
    assert sender.calls == calls_before
    async with session_maker() as session:
        assert (await session.scalar(select(func.count()).select_from(MessageJob))) == 0


async def test_the_database_refuses_a_confirmed_fact_without_a_slot(session_maker) -> None:
    """``confirmed`` names the slot it is about, by CHECK and not by convention."""
    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                session.add(
                    EasyWeekVoucherProductionDeliveryFailure(
                        fact_key="f" * 64,
                        source=DELIVERY_FAILURE_SOURCE_CALLBACK,
                        reason_code=DELIVERY_FAILURE_REASON_CALLBACK_FAILED,
                        match_state=DELIVERY_FAILURE_MATCH_CONFIRMED,
                        failure_class=DELIVERY_FAILURE_CLASS_UNKNOWN,
                        authenticity=INBOUND_AUTHENTICITY_SIGNED,
                        provider_message_id="wamid.NOWHERE",
                        observed_at=utcnow(),
                    )
                )


async def test_the_database_refuses_a_proven_route_without_digests(session_maker) -> None:
    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                session.add(
                    EasyWeekVoucherProductionDeliveryFailure(
                        fact_key="g" * 64,
                        source=DELIVERY_FAILURE_SOURCE_CALLBACK,
                        reason_code=DELIVERY_FAILURE_REASON_CALLBACK_FAILED,
                        match_state=DELIVERY_FAILURE_MATCH_UNMATCHED,
                        failure_class=DELIVERY_FAILURE_CLASS_UNKNOWN,
                        authenticity=INBOUND_AUTHENTICITY_SIGNED,
                        provider_message_id="wamid.NOROUTE",
                        route_proven=True,
                        observed_at=utcnow(),
                    )
                )


def test_the_sender_and_recipient_digests_are_domain_separated() -> None:
    """One value must not produce the same handle on both sides of a route."""
    assert sender_route_digest("4915177991001") != recipient_route_digest("4915177991001")
    # And normalisation happens before the recipient digest, so a callback's
    # bare `recipient_id` matches a stored `+`-prefixed destination.
    assert recipient_route_digest("4915177991001") == recipient_route_digest("+4915177991001")


async def test_a_local_check_refusal_sends_nothing_and_proves_nothing(
    session_maker, production_configuration, binding_key, monkeypatch
) -> None:
    """§46.3 — a refusal OUR OWN checks make is not a delivery failure.

    The deliver plan refuses because the booking link is not provable, which is
    the reachable local refusal in this phase: nothing leaves the process, so
    there is nothing for Meta to have refused and no evidence to record.
    """
    from altegio_bot.tests.easyweek_voucher_production_fixtures import location_map

    reader = FakeReader(count=1)
    _run_id, _batch_id, request = await _paid_batch(session_maker, reader, count=1)

    monkeypatch.setattr(settings, "easyweek_location_map", location_map(booking_link=""), raising=False)
    sender = FakeSender()
    report = await _apply(
        session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender, expect_ready=False
    )

    assert report.outcome == "refused"
    assert sender.calls == 0
    assert await _facts(session_maker) == []


async def test_the_same_rejected_status_without_the_proven_flag_writes_nothing(
    session_maker, production_configuration, binding_key
) -> None:
    """The two callers of one status, kept apart by one argument.

    ``send_rejected`` is written both for Meta's own refusal and for the
    runner's parameter refusal, and §46.3 says in as many words that the state
    alone is not proof. This is that distinction, exercised through the ledger
    the runner calls: the same compare-and-set, the same resulting status, and a
    failure fact only when the caller states the refusal was external.
    """
    reader = FakeReader(count=1)
    _run_id, batch_id, _request = await _paid_batch(session_maker, reader, count=1)

    claim = await ledger_module.claim_send(
        session_maker,
        identity=await _identity_for(session_maker, batch_id),
        batch_id=batch_id,
        slot=1,
        plan_digest="e" * 64,
        live_guard_reproven_at=(await _item(session_maker, batch_id)).pay_verified_at,
        template_code="new_client_voucher_10eur_v2",
        meta_template_name="meta_template",
        template_language="de",
        sender_id=None,
        sender_phone_number_id=SENDER_PNID,
        destination_phone=PHONES[0],
    )
    assert claim.granted, claim.reason

    recorded = await ledger_module.record_item_outcome(
        session_maker,
        batch_id=batch_id,
        slot=1,
        status=VOUCHER_PRODUCTION_ITEM_SEND_REJECTED,
        expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED}),
        reason_code="template_parameters_unproven",
        reconciliation_required=True,
        attempt_outcome="rejected",
    )

    assert recorded.applied
    item = await _item(session_maker, batch_id)
    assert item.status == VOUCHER_PRODUCTION_ITEM_SEND_REJECTED
    assert item.send_attempt_count == 1
    assert await _facts(session_maker) == []


async def test_a_create_stage_error_is_not_a_delivery_failure(
    session_maker, production_configuration, binding_key
) -> None:
    """§46.3 — a CREATE error is an excluded state, not a failed send.

    The slot never reaches an attempt, so there is no original send for a
    failure to be about. Driven through the real CREATE stage with the mutation
    failing, which is how a stage error actually happens.
    """
    from altegio_bot.campaigns.easyweek_voucher_production.identity import STAGE_CREATE
    from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationUnknown
    from altegio_bot.tests.easyweek_voucher_production_fixtures import (
        FakeMutator,
        marker_orders,
        production_request,
        seed_production_preview,
        seed_template_and_sender,
    )
    from altegio_bot.tests.test_easyweek_voucher_production_mailing import _freeze

    run_id, _ = await seed_production_preview(session_maker, count=1)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=1)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=1)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))

    mutator = FakeMutator(create_sequence=[EasyWeekVoucherMutationUnknown("timeout")])
    await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)

    item = await _item(session_maker, batch_id)
    assert item.send_attempted_at is None
    assert await _facts(session_maker) == []


async def test_a_callback_that_names_no_route_cannot_confirm_a_frozen_send(
    session_maker, production_configuration, binding_key
) -> None:
    """§46.3 — the stricter check must not be bypassable by omission.

    A real Meta status always carries its recipient and its sender metadata. A
    fact that names neither cannot be compared against a route this send did
    freeze, so it is kept for audit and changes nothing — rather than being
    confirmed on the message id alone, which would be a looser rule reachable
    by leaving two fields out.
    """
    from altegio_bot.campaigns.easyweek_voucher_production.delivery_evidence import (
        CONFLICT_ROUTE_UNVERIFIABLE,
        record_callback_failure,
    )

    batch_id, _report, _sender = await _delivered_batch(session_maker)
    item = await _item(session_maker, batch_id)

    async with session_maker() as session:
        async with session.begin():
            outcome = await record_callback_failure(
                session,
                status={
                    "provider_message_id": item.provider_message_id,
                    "status": "failed",
                    "error_code": 131026,
                    # No recipient_id and no phone_number_id at all.
                    "recipient_id": None,
                    "phone_number_id": None,
                },
            )

    assert outcome.claimed is True
    assert outcome.match_state == DELIVERY_FAILURE_MATCH_CONFLICTING
    assert outcome.reason == CONFLICT_ROUTE_UNVERIFIABLE
    after = await _item(session_maker, batch_id)
    assert after.evidence.get("confirmed_delivery_failure") is None


async def test_a_historical_attempt_without_a_frozen_route_is_not_proof(
    session_maker, production_configuration, binding_key
) -> None:
    """A callback about a send whose route was never frozen proves nothing.

    This replaced a test that asserted the opposite. The reasoning that looked
    sound — "the message id names the right slot, and the missing route is just
    an old row being honest" — skips the question §46.3 actually asks: WAS this
    the message that failed? The frozen route is what answers it, and with
    nothing to compare against, a callback naming any route at all is an
    unchecked claim. It is kept, as ``unproven``, and marks nothing.

    The attempt's digests are cleared to make it look like a row written before
    this stage existed, which is exactly the population this matters for.
    """
    from altegio_bot.campaigns.easyweek_voucher_production.delivery_evidence import UNPROVEN_ROUTE_UNKNOWN

    batch_id, _report, _sender = await _delivered_batch(session_maker)
    item = await _item(session_maker, batch_id)
    async with session_maker() as session:
        async with session.begin():
            await session.execute(
                EasyWeekVoucherProductionBatchAttempt.__table__.update()
                .values(sender_phone_number_id_digest=None, recipient_digest=None)
                .where(EasyWeekVoucherProductionBatchAttempt.item_id == int(item.id))
            )

    await _deliver_status(
        session_maker,
        _status_payload(wamid=item.provider_message_id or "", recipient=PHONES[0].lstrip("+")),
    )

    fact = (await _facts(session_maker))[0]
    assert fact.match_state == DELIVERY_FAILURE_MATCH_UNPROVEN
    assert fact.route_proven is False
    assert fact.evidence.get("unproven") == UNPROVEN_ROUTE_UNKNOWN
    assert fact.item_id == int(item.id), "it still names the slot it was tested against"
    after = await _item(session_maker, batch_id)
    assert after.evidence.get("confirmed_delivery_failure") is None


async def test_an_unsigned_callback_is_kept_and_confirms_nothing(
    session_maker, production_configuration, binding_key
) -> None:
    """§46.3 — an unattributed claim is not evidence, however well it matches.

    Everything else about this callback is right: the exact message id, the real
    frozen route, no delivered/read to contradict it. The one thing missing is
    the signature, which is the only reason to believe it came from Meta at all
    — and on a deployment with no app secret that is EVERY callback. Confirming
    on "nothing contradicts it" would mean anybody who can reach the webhook can
    write this phase's failure evidence.
    """
    from altegio_bot.campaigns.easyweek_voucher_production.delivery_evidence import UNPROVEN_CALLBACK_UNVERIFIED

    batch_id, _report, _sender = await _delivered_batch(session_maker)
    item = await _item(session_maker, batch_id)

    await _deliver_status(
        session_maker,
        _status_payload(wamid=item.provider_message_id or "", recipient=PHONES[0].lstrip("+")),
        signature_verified=False,
    )

    fact = (await _facts(session_maker))[0]
    assert fact.authenticity == INBOUND_AUTHENTICITY_UNVERIFIED
    assert fact.match_state == DELIVERY_FAILURE_MATCH_UNPROVEN
    assert fact.evidence.get("unproven") == UNPROVEN_CALLBACK_UNVERIFIED
    # The route WAS proven; that half is recorded truthfully either way.
    assert fact.route_proven is True
    after = await _item(session_maker, batch_id)
    assert after.evidence.get("confirmed_delivery_failure") is None


async def test_a_signed_callback_on_the_frozen_route_is_still_confirmed(
    session_maker, production_configuration, binding_key
) -> None:
    """The positive case, stated beside the negatives it is contrasted with."""
    batch_id, _report, _sender = await _delivered_batch(session_maker)
    item = await _item(session_maker, batch_id)

    await _deliver_status(
        session_maker,
        _status_payload(wamid=item.provider_message_id or "", recipient=PHONES[0].lstrip("+")),
        signature_verified=True,
    )

    fact = (await _facts(session_maker))[0]
    assert fact.match_state == DELIVERY_FAILURE_MATCH_CONFIRMED
    assert fact.authenticity == INBOUND_AUTHENTICITY_SIGNED
    assert fact.route_proven is True
    assert (await _item(session_maker, batch_id)).evidence.get("confirmed_delivery_failure") is True


async def test_a_historical_api_rejection_is_confirmed_without_a_frozen_route(
    session_maker, production_configuration, binding_key
) -> None:
    """The other source keeps its own proof contract, and must not be collapsed.

    An API rejection was watched by this process on its own request: there is no
    webhook, so there is no signature, and a historical attempt froze no route.
    Applying the callback rule here would quietly stop every real Meta rejection
    from ever being confirmed — including the ones the historical reconciliation
    exists to record.
    """
    batch_id, _report, _sender = await _delivered_batch(
        session_maker,
        outcomes=[DeliveryOutcome(DELIVERY_REJECTED, reason=REASON_REJECTED, http_status=400)],
    )
    item = await _item(session_maker, batch_id)
    async with session_maker() as session:
        async with session.begin():
            await session.execute(
                EasyWeekVoucherProductionBatchAttempt.__table__.update()
                .values(sender_phone_number_id_digest=None, recipient_digest=None)
                .where(EasyWeekVoucherProductionBatchAttempt.item_id == int(item.id))
            )
            fact = (await session.execute(select(EasyWeekVoucherProductionDeliveryFailure))).scalar_one()
            await session.delete(fact)

    async with session_maker() as session:
        async with session.begin():
            from altegio_bot.campaigns.easyweek_voucher_production import delivery_evidence

            fresh = (
                await session.execute(
                    select(EasyWeekVoucherProductionBatchItem).where(
                        EasyWeekVoucherProductionBatchItem.id == int(item.id)
                    )
                )
            ).scalar_one()
            outcome = await delivery_evidence.record_api_rejection(session, item=fresh, http_status=400)

    assert outcome.match_state == DELIVERY_FAILURE_MATCH_CONFIRMED
    stored = (await _facts(session_maker))[0]
    assert stored.source == DELIVERY_FAILURE_SOURCE_META_API
    assert stored.authenticity == INBOUND_AUTHENTICITY_UNVERIFIED
    assert stored.route_proven is False


async def test_an_unproven_callback_does_not_disturb_delivered_or_read(
    session_maker, production_configuration, binding_key
) -> None:
    """A success already recorded stays recorded, and outranks the doubt."""
    batch_id, _report, _sender = await _delivered_batch(session_maker)
    item = await _item(session_maker, batch_id)
    wamid = item.provider_message_id or ""

    await _deliver_status(session_maker, _status_payload(wamid=wamid, status="read", recipient=PHONES[0].lstrip("+")))
    await _deliver_status(
        session_maker,
        _status_payload(wamid=wamid, recipient=PHONES[0].lstrip("+")),
        signature_verified=False,
    )

    after = await _item(session_maker, batch_id)
    assert after.status == VOUCHER_PRODUCTION_ITEM_READ
    assert after.read_at is not None
    assert after.evidence.get("confirmed_delivery_failure") is None
    fact = (await _facts(session_maker))[0]
    # The success contradiction is reported ahead of the missing signature: it
    # is the stronger statement about the same fact.
    assert fact.match_state == DELIVERY_FAILURE_MATCH_CONFLICTING
    assert fact.evidence.get("conflict") == CONFLICT_AFTER_SUCCESS


async def test_the_database_refuses_a_confirmed_callback_without_its_proofs(session_maker) -> None:
    """The callback rule is a CHECK, not only a code path.

    Written directly, bypassing every function in this phase, because the point
    is that an unsigned or route-unproven CONFIRMED callback is unrepresentable
    rather than merely unreachable.
    """
    for authenticity, route_proven in (
        (INBOUND_AUTHENTICITY_UNVERIFIED, True),
        (INBOUND_AUTHENTICITY_SIGNED, False),
    ):
        with pytest.raises(IntegrityError):
            async with session_maker() as session:
                async with session.begin():
                    session.add(
                        EasyWeekVoucherProductionDeliveryFailure(
                            fact_key=f"forced-{authenticity}-{route_proven}",
                            source=DELIVERY_FAILURE_SOURCE_CALLBACK,
                            reason_code=DELIVERY_FAILURE_REASON_CALLBACK_FAILED,
                            match_state=DELIVERY_FAILURE_MATCH_CONFIRMED,
                            failure_class=DELIVERY_FAILURE_CLASS_UNKNOWN,
                            authenticity=authenticity,
                            item_id=None,
                            provider_message_id="wamid.FORCED",
                            observed_sender_digest="s" * 64,
                            observed_recipient_digest="r" * 64,
                            route_proven=route_proven,
                            observed_at=utcnow(),
                            evidence={},
                        )
                    )


# ===========================================================================
# Review fix 6 — an early callback is bound whichever transaction commits first
# ===========================================================================


def _internal_status(
    *,
    wamid: str,
    recipient: str = PHONES[0],
    phone_number_id: str = SENDER_PNID,
    error_code: Any = 131026,
) -> dict[str, Any]:
    """One status in the shape the worker hands to the evidence layer."""
    return {
        "provider_message_id": wamid,
        "status": "failed",
        "recipient_id": recipient.lstrip("+"),
        "phone_number_id": phone_number_id,
        "error_code": error_code,
        "event_at": None,
    }


async def _claimed_slot(session_maker) -> tuple[int, Any]:
    """A batch whose slot is claimed for delivery and has no identifier yet."""
    reader = FakeReader(count=1)
    _run_id, batch_id, _request = await _paid_batch(session_maker, reader, count=1)
    claim = await ledger_module.claim_send(
        session_maker,
        identity=await _identity_for(session_maker, batch_id),
        batch_id=batch_id,
        slot=1,
        plan_digest="d" * 64,
        live_guard_reproven_at=(await _item(session_maker, batch_id)).pay_verified_at,
        template_code="new_client_voucher_10eur_v2",
        meta_template_name="meta_template",
        template_language="de",
        sender_id=None,
        sender_phone_number_id=SENDER_PNID,
        destination_phone=PHONES[0],
    )
    assert claim.granted, claim.reason
    return batch_id, claim


async def _accept(session_maker, batch_id: int, wamid: str, *, slot: int = 1) -> Any:
    return await ledger_module.record_item_outcome(
        session_maker,
        batch_id=batch_id,
        slot=slot,
        status=VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED,
        expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED}),
        provider_message_id=wamid,
        verified_field="provider_accepted_at",
        reconciliation_required=False,
        attempt_outcome="provider_accepted",
    )


async def test_an_acceptance_that_commits_first_lets_the_callback_find_the_slot(
    session_maker, production_configuration, binding_key
) -> None:
    """The easy order, stated so the hard one below has something to differ from."""
    batch_id, _claim = await _claimed_slot(session_maker)
    wamid = "wamid.ORDER-A-FIRST"
    assert (await _accept(session_maker, batch_id, wamid)).applied

    await _deliver_status(session_maker, _status_payload(wamid=wamid, recipient=PHONES[0].lstrip("+")))

    facts = await _facts(session_maker)
    assert len(facts) == 1
    assert facts[0].match_state == DELIVERY_FAILURE_MATCH_CONFIRMED
    assert facts[0].item_id == int((await _item(session_maker, batch_id)).id)


async def test_an_uncommitted_callback_cannot_be_overtaken_by_the_acceptance(
    session_maker, production_configuration, binding_key
) -> None:
    """§46.3 — the window two re-reads cannot close, closed by one lock.

    The interleaving staged here is the one that loses a real failure for good:
    the callback transaction looks for the slot before the identifier is
    committed (finds nothing, so it will keep the fact unmatched), and the
    acceptance looks for unmatched facts before the callback is committed (finds
    nothing, so it binds nothing). Both of them are then correct about what they
    saw, and the fact ends up owned by nobody.

    With the per-identifier lock the second half cannot run: the acceptance
    BLOCKS while the callback transaction is open — asserted here, because a
    test that only checked the end state would pass just as happily on a
    re-read-based implementation that still has the hole — and once the callback
    commits, the acceptance sees the fact and binds it.
    """
    from altegio_bot.campaigns.easyweek_voucher_production import delivery_evidence

    batch_id, _claim = await _claimed_slot(session_maker)
    wamid = "wamid.ORDER-RACE"
    callback_open = asyncio.Event()
    release = asyncio.Event()
    blocked_while_open: list[bool] = []

    async def _callback() -> None:
        async with session_maker() as session:
            async with session.begin():
                missed = await delivery_evidence.record_callback_failure(
                    session,
                    status=_internal_status(wamid=wamid),
                    signature_verified=True,
                )
                # Nobody owns the identifier yet: the first half of the race.
                assert missed.claimed is False
                kept = await delivery_evidence.record_unmatched_callback_failure(
                    session,
                    status=_internal_status(wamid=wamid),
                    signature_verified=True,
                )
                assert kept.stored is True
                callback_open.set()
                await release.wait()

    async def _acceptance() -> Any:
        await callback_open.wait()
        task = asyncio.create_task(_accept(session_maker, batch_id, wamid))
        done, _pending = await asyncio.wait({task}, timeout=1.0)
        # It cannot finish: the identifier lock is held by the open callback.
        blocked_while_open.append(not done)
        release.set()
        return await task

    _, recorded = await asyncio.gather(_callback(), _acceptance())

    assert blocked_while_open == [True]
    assert recorded.applied
    facts = await _facts(session_maker)
    assert len(facts) == 1
    item = await _item(session_maker, batch_id)
    assert facts[0].match_state == DELIVERY_FAILURE_MATCH_CONFIRMED
    assert facts[0].item_id == int(item.id)
    assert facts[0].route_proven is True
    assert item.evidence.get("confirmed_delivery_failure") is True


async def test_the_same_callback_delivered_twice_through_the_race_is_one_fact(
    session_maker, production_configuration, binding_key
) -> None:
    """Idempotency survives the binding protocol, in both orders."""
    batch_id, _claim = await _claimed_slot(session_maker)
    wamid = "wamid.ORDER-DUP"

    await _deliver_status(session_maker, _status_payload(wamid=wamid, recipient=PHONES[0].lstrip("+")))
    await _deliver_status(session_maker, _status_payload(wamid=wamid, recipient=PHONES[0].lstrip("+")))
    assert (await _accept(session_maker, batch_id, wamid)).applied
    await _deliver_status(session_maker, _status_payload(wamid=wamid, recipient=PHONES[0].lstrip("+")))

    facts = await _facts(session_maker)
    assert len(facts) == 1
    assert facts[0].match_state == DELIVERY_FAILURE_MATCH_CONFIRMED


async def test_an_early_callback_from_a_foreign_route_binds_as_a_conflict(
    session_maker, production_configuration, binding_key
) -> None:
    """Waiting for its slot never upgrades a fact. It is re-judged from scratch."""
    batch_id, _claim = await _claimed_slot(session_maker)
    wamid = "wamid.ORDER-FOREIGN"

    await _deliver_status(
        session_maker,
        _status_payload(wamid=wamid, recipient="4915100009999"),
    )
    assert (await _accept(session_maker, batch_id, wamid)).applied

    fact = (await _facts(session_maker))[0]
    assert fact.match_state == DELIVERY_FAILURE_MATCH_CONFLICTING
    assert fact.evidence.get("conflict") == CONFLICT_FOREIGN_ROUTE
    assert fact.route_proven is False
    assert (await _item(session_maker, batch_id)).evidence.get("confirmed_delivery_failure") is None


async def test_an_early_unsigned_callback_is_not_promoted_by_being_bound(
    session_maker, production_configuration, binding_key
) -> None:
    """The authenticity stored when it arrived is the one it is judged by."""
    from altegio_bot.campaigns.easyweek_voucher_production.delivery_evidence import UNPROVEN_CALLBACK_UNVERIFIED

    batch_id, _claim = await _claimed_slot(session_maker)
    wamid = "wamid.ORDER-UNSIGNED"

    await _deliver_status(
        session_maker,
        _status_payload(wamid=wamid, recipient=PHONES[0].lstrip("+")),
        signature_verified=False,
    )
    assert (await _accept(session_maker, batch_id, wamid)).applied

    fact = (await _facts(session_maker))[0]
    assert fact.match_state == DELIVERY_FAILURE_MATCH_UNPROVEN
    assert fact.evidence.get("unproven") == UNPROVEN_CALLBACK_UNVERIFIED
    assert fact.item_id == int((await _item(session_maker, batch_id)).id)
    assert (await _item(session_maker, batch_id)).evidence.get("confirmed_delivery_failure") is None


# ===========================================================================
# Round 2, review fix 5B — one order for the WHOLE callback transaction
# ===========================================================================


def _multi_status_payload(*entries: tuple[str, str], recipient: str, epoch: int = 1_760_000_500) -> dict[str, Any]:
    """One delivery carrying several statuses, which is what Meta really sends.

    The whole point of this group: a payload is not one status, and handling it
    one status at a time took locks in an order the acceptance could cross.
    """
    statuses: list[dict[str, Any]] = []
    for wamid, status in entries:
        entry: dict[str, Any] = {
            "id": wamid,
            "status": status,
            "timestamp": str(epoch),
            "recipient_id": recipient,
        }
        if status == "failed":
            entry["errors"] = [{"code": 131026, "title": "Message undeliverable"}]
        statuses.append(entry)
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA-PROD",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {"phone_number_id": SENDER_PNID},
                            "statuses": statuses,
                        },
                    }
                ],
            }
        ],
    }


async def _claim(session_maker, batch_id: int, slot: int) -> Any:
    item = await _item(session_maker, batch_id, slot)
    return await ledger_module.claim_send(
        session_maker,
        identity=await _identity_for(session_maker, batch_id),
        batch_id=batch_id,
        slot=slot,
        plan_digest="d" * 64,
        live_guard_reproven_at=item.pay_verified_at,
        template_code="new_client_voucher_10eur_v2",
        meta_template_name="meta_template",
        template_language="de",
        sender_id=None,
        sender_phone_number_id=SENDER_PNID,
        destination_phone=PHONES[slot - 1],
    )


async def _one_sent_one_claimed(session_maker, *, first_wamid: str) -> int:
    """A two-slot batch mid-delivery: slot 1 accepted, slot 2 claimed.

    The shape the executor really passes through, slot by slot — claim, send,
    accept, claim the next — and the shape the deadlock needed: one message of
    the batch already has an identifier a callback can arrive about, while the
    next one is still being accepted.
    """
    reader = FakeReader(count=2)
    _run_id, batch_id, _request = await _paid_batch(session_maker, reader, count=2)
    assert (await _claim(session_maker, batch_id, 1)).granted
    assert (await _accept(session_maker, batch_id, first_wamid, slot=1)).applied
    assert (await _claim(session_maker, batch_id, 2)).granted
    return batch_id


async def _wait_until_a_transaction_is_queued(session_maker, *, timeout: float = 15.0) -> None:
    """Wait until PostgreSQL reports somebody queued on an advisory lock."""
    from sqlalchemy import text as sql_text

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        async with session_maker() as probe:
            waiting = await probe.scalar(
                sql_text("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted")
            )
        if int(waiting or 0) >= 1:
            return
        assert loop.time() < deadline, "nothing ever queued on an advisory lock"
        await asyncio.sleep(0.05)


async def test_a_mixed_callback_does_not_deadlock_the_acceptance_of_another_slot(
    session_maker, production_configuration, binding_key, monkeypatch
) -> None:
    """The review's schedule B, reproduced and shown closed.

    One callback carries ``delivered`` for slot 1's message and ``failed`` for
    slot 2's. Handling them one at a time took the batch header to record the
    delivery and then asked for slot 2's identifier lock — while the transaction
    accepting slot 2 held that identifier and was waiting for the same header.
    PostgreSQL broke the cycle by aborting one of them, and the one it aborted
    was the acceptance: Meta already had the message, the slot stayed
    ``send_claimed`` with no identifier, slot 2's failure stayed unmatched, and
    the executor gave up on an operation it could no longer describe.

    Here the acceptance is parked between its identifier lock and the header,
    which is exactly where it was when the cycle used to close. The callback
    must then be waiting WITHOUT the header, so the acceptance can take it.
    """
    wamid_delivered = "wamid.MIXED-DELIVERED"
    wamid_failed = "wamid.MIXED-FAILED"
    batch_id = await _one_sent_one_claimed(session_maker, first_wamid=wamid_delivered)

    parked = asyncio.Event()
    release = asyncio.Event()
    real_header = ledger_module._header_by_id
    first = {"pending": True}

    async def _parking_header(session: Any, header_id: int, *, for_update: bool = False) -> Any:
        if first["pending"]:
            first["pending"] = False
            parked.set()
            await release.wait()
        return await real_header(session, header_id, for_update=for_update)

    monkeypatch.setattr(ledger_module, "_header_by_id", _parking_header)
    acceptance = asyncio.create_task(_accept(session_maker, batch_id, wamid_failed, slot=2))
    await asyncio.wait_for(parked.wait(), timeout=10)

    callback = asyncio.create_task(
        _deliver_status(
            session_maker,
            _multi_status_payload(
                (wamid_delivered, "delivered"),
                (wamid_failed, "failed"),
                recipient=PHONES[1].lstrip("+"),
            ),
        )
    )
    await _wait_until_a_transaction_is_queued(session_maker)
    release.set()

    recorded = await asyncio.wait_for(acceptance, timeout=20)
    callback_event_id = await asyncio.wait_for(callback, timeout=20)

    assert recorded.applied, recorded.reason
    second = await _item(session_maker, batch_id, 2)
    assert second.provider_message_id == wamid_failed
    facts = [fact for fact in await _facts(session_maker) if fact.provider_message_id == wamid_failed]
    assert len(facts) == 1
    # Bound to the slot in either commit order, and nothing was re-sent: the
    # provider double in this module raises if anything tries.
    assert facts[0].item_id == int(second.id)
    assert facts[0].match_state == DELIVERY_FAILURE_MATCH_CONFIRMED
    first_item = await _item(session_maker, batch_id, 1)
    assert first_item.delivered_at is not None
    # And the delivery itself was processed rather than left behind: an aborted
    # callback transaction used to strand the event as well as lose the fact.
    async with session_maker() as session:
        event = await session.get(WhatsAppEvent, callback_event_id)
    assert event.status == "processed"
    assert event.error is None


async def test_a_callback_takes_its_message_locks_in_sorted_order(
    session_maker, production_configuration, binding_key
) -> None:
    """Two failures in one payload, in the order that is NOT the sorted one.

    A transaction that locked identifiers in payload order could cross another
    one carrying the same two in the opposite order. Here the sorted-first
    identifier is held by a third transaction, so the callback has to be queued
    on it while holding nothing — which is what lets the holder take the second
    identifier without a cycle.
    """
    from altegio_bot.campaigns.easyweek_voucher_production.delivery_evidence import lock_message_identity

    low = "wamid.AAA-SORTS-FIRST"
    high = "wamid.ZZZ-SORTS-LAST"
    assert low < high
    batch_id = await _one_sent_one_claimed(session_maker, first_wamid=low)
    assert (await _accept(session_maker, batch_id, high, slot=2)).applied

    release = asyncio.Event()
    took_the_second = asyncio.Event()

    async def _holder() -> None:
        async with session_maker() as session:
            async with session.begin():
                await lock_message_identity(session, low)
                await release.wait()
                await asyncio.wait_for(lock_message_identity(session, high), timeout=10)
                took_the_second.set()

    holder = asyncio.create_task(_holder())
    await asyncio.sleep(0)
    callback = asyncio.create_task(
        _deliver_status(
            session_maker,
            # Payload order is high, low — the reverse of the sorted order.
            _multi_status_payload((high, "failed"), (low, "failed"), recipient=PHONES[0].lstrip("+")),
        )
    )
    await _wait_until_a_transaction_is_queued(session_maker)
    release.set()
    await asyncio.wait_for(took_the_second.wait(), timeout=10)
    await asyncio.wait_for(holder, timeout=10)
    await asyncio.wait_for(callback, timeout=20)

    facts = {fact.provider_message_id: fact for fact in await _facts(session_maker)}
    assert set(facts) == {low, high}
    assert all(fact.item_id is not None for fact in facts.values())


async def test_two_callbacks_with_the_same_pair_in_opposite_orders_both_land(
    session_maker, production_configuration, binding_key
) -> None:
    """The same two identifiers, two deliveries, opposite payload orders.

    Both transactions sort the same way, so one simply waits for the other.
    Neither is aborted, and the facts are idempotent across the pair.
    """
    first_id = "wamid.PAIR-ONE"
    second_id = "wamid.PAIR-TWO"
    batch_id = await _one_sent_one_claimed(session_maker, first_wamid=first_id)
    assert (await _accept(session_maker, batch_id, second_id, slot=2)).applied

    await asyncio.gather(
        _deliver_status(
            session_maker,
            _multi_status_payload((first_id, "failed"), (second_id, "failed"), recipient=PHONES[0].lstrip("+")),
        ),
        _deliver_status(
            session_maker,
            _multi_status_payload((second_id, "failed"), (first_id, "failed"), recipient=PHONES[0].lstrip("+")),
        ),
    )

    facts = await _facts(session_maker)
    by_message = {fact.provider_message_id: fact for fact in facts}
    assert set(by_message) == {first_id, second_id}
    assert len(facts) == 2, "the same fact delivered twice is still one row"
    for fact in facts:
        assert fact.item_id is not None
        assert fact.match_state in {DELIVERY_FAILURE_MATCH_CONFIRMED, DELIVERY_FAILURE_MATCH_CONFLICTING}


async def test_a_delivered_in_the_same_payload_still_outranks_its_own_failure(
    session_maker, production_configuration, binding_key
) -> None:
    """Locking the scope up front changes no outcome, only the order of waits.

    ``read`` and ``delivered`` keep outranking a ``failed`` about the same
    message, including when both arrive in one delivery.
    """
    wamid = "wamid.SAME-PAYLOAD"
    batch_id = await _one_sent_one_claimed(session_maker, first_wamid=wamid)

    await _deliver_status(
        session_maker,
        _multi_status_payload((wamid, "delivered"), (wamid, "failed"), recipient=PHONES[0].lstrip("+")),
    )

    item = await _item(session_maker, batch_id, 1)
    assert item.delivered_at is not None
    assert item.status in {VOUCHER_PRODUCTION_ITEM_DELIVERED, VOUCHER_PRODUCTION_ITEM_READ}
    facts = await _facts(session_maker)
    assert len(facts) == 1
    assert facts[0].match_state == DELIVERY_FAILURE_MATCH_CONFLICTING
    assert facts[0].evidence.get("conflict") == CONFLICT_AFTER_SUCCESS
    assert item.evidence.get("confirmed_delivery_failure") is not True


# ===========================================================================
# Round 3, review fix 4 — the locks are gone before anything external happens
# ===========================================================================


def _mixed_delivery_payload(
    *entries: tuple[str, str],
    recipient: str,
    inbound_text: str | None = None,
    inbound_from: str = "4915177991001",
    epoch: int = 1_760_000_500,
) -> dict[str, Any]:
    """One delivery carrying statuses and, optionally, an inbound message.

    Meta really does combine them, and the combination is the whole point: the
    status half locks voucher batch headers, the inbound half makes external
    calls.
    """
    statuses: list[dict[str, Any]] = []
    for wamid, status in entries:
        entry: dict[str, Any] = {
            "id": wamid,
            "status": status,
            "timestamp": str(epoch),
            "recipient_id": recipient,
        }
        if status == "failed":
            entry["errors"] = [{"code": 131026, "title": "Message undeliverable"}]
        statuses.append(entry)
    value: dict[str, Any] = {
        "metadata": {"phone_number_id": SENDER_PNID},
        "statuses": statuses,
    }
    if inbound_text is not None:
        value["messages"] = [
            {
                "from": inbound_from,
                "id": f"wamid.INBOUND-{inbound_text[:6]}",
                "timestamp": str(epoch),
                "type": "text",
                "text": {"body": inbound_text},
            }
        ]
    return {
        "object": "whatsapp_business_account",
        "entry": [{"id": "WABA-PROD", "changes": [{"field": "messages", "value": value}]}],
    }


class _LockProbingProvider:
    """A provider that checks, at the moment of the ACK, that the locks are gone.

    This is the assertion the review asks for, taken from inside the external
    call itself: the acknowledgement is where a slow WhatsApp or Chatwoot
    response used to park while the batch header was still held, and
    ``FOR UPDATE NOWAIT`` from another session is exactly how the executor finds
    out — ``55P03``, a refused claim, an acceptance that cannot be saved and an
    operator STOP that cannot be applied.
    """

    def __init__(self, session_maker, batch_id: int, wamids: tuple[str, ...]) -> None:
        self.session_maker = session_maker
        self.batch_id = batch_id
        self.wamids = wamids
        self.header_free: list[bool] = []
        self.messages_free: list[bool] = []
        self.sent: list[tuple[int, str, str]] = []

    async def _probe(self) -> None:
        from sqlalchemy import text as sql_text

        from altegio_bot.campaigns.easyweek_voucher_production.delivery_evidence import _MESSAGE_LOCK_DOMAIN

        async with self.session_maker() as probe:
            async with probe.begin():
                try:
                    await probe.execute(
                        select(EasyWeekVoucherProductionBatch)
                        .where(EasyWeekVoucherProductionBatch.id == self.batch_id)
                        .with_for_update(nowait=True)
                    )
                    self.header_free.append(True)
                except Exception:
                    self.header_free.append(False)
        async with self.session_maker() as probe:
            async with probe.begin():
                taken = True
                for wamid in self.wamids:
                    key = int.from_bytes(
                        hashlib.sha256(f"{_MESSAGE_LOCK_DOMAIN}:{wamid}".encode("utf-8", "surrogatepass")).digest()[:8],
                        "big",
                        signed=True,
                    )
                    got = await probe.scalar(sql_text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": key})
                    taken = taken and bool(got)
                self.messages_free.append(taken)

    async def send(self, sender_id: int, phone_e164: str, text: str, contact_name: str | None = None) -> str:
        await self._probe()
        self.sent.append((sender_id, phone_e164, text))
        return f"wamid.ACK{len(self.sent)}"

    async def send_template(self, *args: Any, **kwargs: Any) -> str:  # pragma: no cover - not used here
        raise AssertionError("no template is sent from an inbound command")


async def _seed_inbound_sender(session_maker) -> None:
    from altegio_bot.models.models import WhatsAppSender

    async with session_maker() as session:
        async with session.begin():
            existing = await session.scalar(
                select(WhatsAppSender.id).where(WhatsAppSender.phone_number_id == SENDER_PNID)
            )
            if existing is None:
                session.add(
                    WhatsAppSender(
                        company_id=1,
                        sender_code="probe",
                        phone_number_id=SENDER_PNID,
                        display_phone="+4930000009",
                        is_active=True,
                    )
                )


async def _run_mixed(session_maker, payload: dict[str, Any], provider: Any) -> int:
    async with session_maker() as session:
        async with session.begin():
            event = WhatsAppEvent(
                dedupe_key=f"wa:mixed-{uuid_module.uuid4().hex}",
                status="received",
                query={},
                headers={},
                payload=payload,
                inbound_authenticity=INBOUND_AUTHENTICITY_SIGNED,
            )
            session.add(event)
            await session.flush()
            event_id = int(event.id)

    original = worker_module.SessionLocal
    try:
        worker_module.SessionLocal = session_maker  # type: ignore[assignment]
        await worker_module.process_one_event(event_id, provider)
    finally:
        worker_module.SessionLocal = original
    return event_id


@pytest.mark.parametrize("status_kind", ["sent", "delivered", "read", "failed"])
async def test_a_batch_header_is_free_while_the_acknowledgement_is_in_flight(
    session_maker, production_configuration, binding_key, status_kind: str
) -> None:
    """The regression of review fix 4, for every status kind in one payload.

    Before this phase a ``sent`` callback locked nothing. With the callback
    scope declared up front it locked the batch header — and the transaction
    then went on to acknowledge an inbound STOP over the network, holding it. A
    delay at Meta or Chatwoot became a refused claim for the whole batch.
    """
    wamid = "wamid.R3FREE-" + status_kind.upper()
    batch_id = await _one_sent_one_claimed(session_maker, first_wamid=wamid)
    await _seed_inbound_sender(session_maker)

    provider = _LockProbingProvider(session_maker, batch_id, (wamid,))
    await _run_mixed(
        session_maker,
        _mixed_delivery_payload((wamid, status_kind), recipient=PHONES[0].lstrip("+"), inbound_text="STOP"),
        provider,
    )

    assert len(provider.sent) == 1, "the inbound STOP really was acknowledged"
    assert provider.header_free == [True], "the batch header was still locked during the ACK"
    assert provider.messages_free == [True], "the message identifier was still locked during the ACK"


async def test_the_callback_facts_are_already_committed_before_the_acknowledgement(
    session_maker, production_configuration, binding_key
) -> None:
    """Releasing the locks early does not mean deciding anything later.

    The evidence is written and committed in its own transaction, so it is on
    disk before the first external call — which is also what makes an
    acknowledgement that fails unable to roll it back.
    """
    wamid = "wamid.R3COMMITTED"
    batch_id = await _one_sent_one_claimed(session_maker, first_wamid=wamid)
    await _seed_inbound_sender(session_maker)

    observed: list[int] = []

    class _Observer(_LockProbingProvider):
        async def send(self, sender_id: int, phone_e164: str, text: str, contact_name: str | None = None) -> str:
            async with self.session_maker() as probe:
                observed.append(
                    int(
                        await probe.scalar(select(func.count()).select_from(EasyWeekVoucherProductionDeliveryFailure))
                        or 0
                    )
                )
            return await super().send(sender_id, phone_e164, text, contact_name)

    provider = _Observer(session_maker, batch_id, (wamid,))
    await _run_mixed(
        session_maker,
        _mixed_delivery_payload((wamid, "failed"), recipient=PHONES[0].lstrip("+"), inbound_text="STOP"),
        provider,
    )

    assert observed == [1], "the failure fact was not committed before the ACK"
    facts = await _facts(session_maker)
    assert len(facts) == 1
    assert facts[0].match_state == DELIVERY_FAILURE_MATCH_CONFIRMED
    item = await _item(session_maker, batch_id, 1)
    assert facts[0].item_id == int(item.id)


async def test_a_failing_acknowledgement_keeps_the_evidence_and_sends_once(
    session_maker, production_configuration, binding_key
) -> None:
    """An external failure after the evidence is committed changes none of it."""
    wamid = "wamid.R3ACKFAIL"
    batch_id = await _one_sent_one_claimed(session_maker, first_wamid=wamid)
    await _seed_inbound_sender(session_maker)
    attempts: list[str] = []

    class _Broken:
        async def send(self, sender_id: int, phone_e164: str, text: str, contact_name: str | None = None) -> str:
            attempts.append(text)
            raise RuntimeError("meta is down")

        async def send_template(self, *args: Any, **kwargs: Any) -> str:  # pragma: no cover
            raise AssertionError("no template here")

    event_id = await _run_mixed(
        session_maker,
        _mixed_delivery_payload((wamid, "failed"), recipient=PHONES[0].lstrip("+"), inbound_text="STOP"),
        _Broken(),
    )

    assert len(attempts) == 1, "exactly one attempt, and no second POST"
    facts = await _facts(session_maker)
    assert len(facts) == 1
    assert facts[0].match_state == DELIVERY_FAILURE_MATCH_CONFIRMED
    item = await _item(session_maker, batch_id, 1)
    assert item.delivered_at is None
    # The refusal itself survived the failure, which is §46.4.7.
    async with session_maker() as session:
        suppression = (
            await session.execute(
                select(WhatsAppConsentSuppression).where(WhatsAppConsentSuppression.wa_address == "+4915177991001")
            )
        ).scalar_one()
        event = await session.get(WhatsAppEvent, event_id)
    assert suppression.state == "opted_out"
    assert event.status == "processed"
    assert event.error is not None and "Ack send failed" in event.error


async def test_a_statuses_only_payload_still_records_everything(
    session_maker, production_configuration, binding_key
) -> None:
    """No inbound half: the separate transaction must not change the outcome."""
    wamid = "wamid.R3ONLY"
    batch_id = await _one_sent_one_claimed(session_maker, first_wamid=wamid)

    await _run_mixed(
        session_maker,
        _mixed_delivery_payload((wamid, "delivered"), recipient=PHONES[0].lstrip("+")),
        _SilentProvider(),
    )

    item = await _item(session_maker, batch_id, 1)
    assert item.delivered_at is not None
    assert item.status in {VOUCHER_PRODUCTION_ITEM_DELIVERED, VOUCHER_PRODUCTION_ITEM_READ}


# ---------------------------------------------------------------------------
# §46.3 — an EARLY delivered/read, kept and bound (review round 2, finding 3)
# ---------------------------------------------------------------------------


async def _receipts(session_maker) -> list[EasyWeekVoucherProductionDeliveryReceipt]:
    async with session_maker() as session:
        return list(
            (
                await session.scalars(
                    select(EasyWeekVoucherProductionDeliveryReceipt).order_by(
                        EasyWeekVoucherProductionDeliveryReceipt.id
                    )
                )
            ).all()
        )


async def test_an_early_read_is_kept_and_bound_when_the_id_lands(
    session_maker, production_configuration, binding_key
) -> None:
    """The reproduction exactly as review round 2 reported it.

    A correctly signed ``read`` naming the slot's own sender and recipient
    arrived while the slot was ``send_claimed``. The worker finished the event
    as processed, the acceptance then stored the POST result, and the slot was
    left at ``provider_accepted`` with an empty ``read_at`` and no receipt —
    so the follow-up audience read ``unknown`` for somebody who had demonstrably
    read the message. A repeat callback is not guaranteed and a general replay
    of the handler is not a fix.
    """
    batch_id, _claim = await _claimed_slot(session_maker)
    recipient = PHONES[0].lstrip("+")
    wamid = "wamid.EARLY-READ"

    await _deliver_status(session_maker, _status_payload(wamid=wamid, status="read", recipient=recipient))

    early = await _receipts(session_maker)
    assert len(early) == 1
    assert early[0].match_state == DELIVERY_RECEIPT_MATCH_UNMATCHED
    assert early[0].item_id is None
    assert early[0].batch_id is None
    assert early[0].route_proven is False
    assert early[0].authenticity == INBOUND_AUTHENTICITY_SIGNED
    assert early[0].evidence.get("awaiting_binding") is True
    # Nothing was guessed about the slot while the identifier was unowned.
    assert (await _item(session_maker, batch_id)).read_at is None

    assert (await _accept(session_maker, batch_id, wamid)).applied

    bound = await _receipts(session_maker)
    assert len(bound) == 1
    item = await _item(session_maker, batch_id)
    assert bound[0].match_state == DELIVERY_RECEIPT_MATCH_CONFIRMED
    assert bound[0].item_id == int(item.id)
    assert bound[0].slot == 1
    assert bound[0].route_proven is True
    assert bound[0].kind == DELIVERY_RECEIPT_KIND_READ
    # And the slot's own record agrees with the proof beside it.
    assert item.status == VOUCHER_PRODUCTION_ITEM_READ
    assert item.read_at is not None
    assert item.delivered_at is not None


async def test_an_early_delivered_binds_and_advances_to_delivered(
    session_maker, production_configuration, binding_key
) -> None:
    batch_id, _claim = await _claimed_slot(session_maker)
    recipient = PHONES[0].lstrip("+")
    wamid = "wamid.EARLY-DELIVERED"
    await _deliver_status(session_maker, _status_payload(wamid=wamid, status="delivered", recipient=recipient))
    assert (await _accept(session_maker, batch_id, wamid)).applied
    item = await _item(session_maker, batch_id)
    assert item.status == VOUCHER_PRODUCTION_ITEM_DELIVERED
    assert item.read_at is None
    assert (await _receipts(session_maker))[0].kind == DELIVERY_RECEIPT_KIND_DELIVERED


async def test_two_early_callbacks_bind_with_read_winning(session_maker, production_configuration, binding_key) -> None:
    """Read arriving BEFORE delivered still leaves the slot at read."""
    batch_id, _claim = await _claimed_slot(session_maker)
    recipient = PHONES[0].lstrip("+")
    wamid = "wamid.EARLY-BOTH"
    await _deliver_status(session_maker, _status_payload(wamid=wamid, status="read", recipient=recipient))
    await _deliver_status(session_maker, _status_payload(wamid=wamid, status="delivered", recipient=recipient))
    assert len(await _receipts(session_maker)) == 2
    assert (await _accept(session_maker, batch_id, wamid)).applied
    item = await _item(session_maker, batch_id)
    assert item.status == VOUCHER_PRODUCTION_ITEM_READ
    assert item.read_at is not None
    assert {row.match_state for row in await _receipts(session_maker)} == {DELIVERY_RECEIPT_MATCH_CONFIRMED}


async def test_a_duplicate_early_callback_is_one_receipt(session_maker, production_configuration, binding_key) -> None:
    """Idempotent on the proven contents, so a Resend adds nothing."""
    batch_id, _claim = await _claimed_slot(session_maker)
    recipient = PHONES[0].lstrip("+")
    wamid = "wamid.EARLY-DUP"
    payload = _status_payload(wamid=wamid, status="read", recipient=recipient)
    await _deliver_status(session_maker, payload)
    await _deliver_status(session_maker, payload)
    assert len(await _receipts(session_maker)) == 1
    assert (await _accept(session_maker, batch_id, wamid)).applied
    # And the SAME callback arriving once more, now that the slot owns the id,
    # lands on the same key through the matched path.
    await _deliver_status(session_maker, payload)
    rows = await _receipts(session_maker)
    assert len(rows) == 1
    assert rows[0].match_state == DELIVERY_RECEIPT_MATCH_CONFIRMED


async def test_an_early_callback_outlives_a_restart_before_binding(
    session_maker, production_configuration, binding_key
) -> None:
    """The kept proof is durable, so a process death between the two halves is safe."""
    batch_id, _claim = await _claimed_slot(session_maker)
    recipient = PHONES[0].lstrip("+")
    wamid = "wamid.EARLY-RESTART"
    await _deliver_status(session_maker, _status_payload(wamid=wamid, status="read", recipient=recipient))
    # Whatever else the deployment does in between must not consume it.
    await _deliver_status(
        session_maker,
        _status_payload(wamid="wamid.SOMEBODY-ELSE", status="delivered", recipient="4915100000999"),
    )
    assert (await _accept(session_maker, batch_id, wamid)).applied
    item = await _item(session_maker, batch_id)
    assert item.status == VOUCHER_PRODUCTION_ITEM_READ
    ours = [row for row in await _receipts(session_maker) if row.provider_message_id == wamid]
    assert [row.match_state for row in ours] == [DELIVERY_RECEIPT_MATCH_CONFIRMED]


async def test_an_unsigned_early_callback_is_not_kept_at_all(
    session_maker, production_configuration, binding_key
) -> None:
    """Authenticity cannot be established later, so there is nothing to keep."""
    batch_id, _claim = await _claimed_slot(session_maker)
    recipient = PHONES[0].lstrip("+")
    wamid = "wamid.EARLY-UNSIGNED"
    await _deliver_status(
        session_maker,
        _status_payload(wamid=wamid, status="read", recipient=recipient),
        signature_verified=False,
    )
    assert await _receipts(session_maker) == []
    assert (await _accept(session_maker, batch_id, wamid)).applied
    assert await _receipts(session_maker) == []
    assert (await _item(session_maker, batch_id)).status == VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED


async def test_an_early_callback_on_a_foreign_sender_binds_as_conflicting(
    session_maker, production_configuration, binding_key
) -> None:
    """A route that is not the frozen one is not this slot's evidence."""
    batch_id, _claim = await _claimed_slot(session_maker)
    recipient = PHONES[0].lstrip("+")
    wamid = "wamid.EARLY-FOREIGN-SENDER"
    await _deliver_status(
        session_maker,
        _status_payload(wamid=wamid, status="read", recipient=recipient, phone_number_id="PNID_SOMEBODY_ELSE"),
    )
    assert (await _accept(session_maker, batch_id, wamid)).applied
    row = (await _receipts(session_maker))[0]
    assert row.match_state == DELIVERY_RECEIPT_MATCH_CONFLICTING
    assert row.evidence.get("conflict") == CONFLICT_FOREIGN_ROUTE
    assert (await _item(session_maker, batch_id)).status == VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED


async def test_an_early_callback_on_a_foreign_recipient_binds_as_conflicting(
    session_maker, production_configuration, binding_key
) -> None:
    batch_id, _claim = await _claimed_slot(session_maker)
    wamid = "wamid.EARLY-FOREIGN-RECIPIENT"
    await _deliver_status(
        session_maker,
        _status_payload(wamid=wamid, status="read", recipient="4915100000999"),
    )
    assert (await _accept(session_maker, batch_id, wamid)).applied
    assert (await _receipts(session_maker))[0].match_state == DELIVERY_RECEIPT_MATCH_CONFLICTING
    assert (await _item(session_maker, batch_id)).read_at is None


async def test_an_early_callback_for_another_message_id_is_never_bound(
    session_maker, production_configuration, binding_key
) -> None:
    """Matched on the exact identifier, never on time or phone similarity."""
    batch_id, _claim = await _claimed_slot(session_maker)
    recipient = PHONES[0].lstrip("+")
    await _deliver_status(
        session_maker,
        _status_payload(wamid="wamid.NOT-THIS-ONE", status="read", recipient=recipient),
    )
    assert (await _accept(session_maker, batch_id, "wamid.THE-REAL-ONE")).applied
    row = (await _receipts(session_maker))[0]
    assert row.match_state == DELIVERY_RECEIPT_MATCH_UNMATCHED
    assert row.item_id is None
    assert (await _item(session_maker, batch_id)).read_at is None


async def test_a_late_failed_does_not_undo_a_bound_read(session_maker, production_configuration, binding_key) -> None:
    """§46.3: success is not rolled back, and the contradiction is kept."""
    batch_id, _claim = await _claimed_slot(session_maker)
    recipient = PHONES[0].lstrip("+")
    wamid = "wamid.EARLY-THEN-FAILED"
    await _deliver_status(session_maker, _status_payload(wamid=wamid, status="read", recipient=recipient))
    assert (await _accept(session_maker, batch_id, wamid)).applied
    await _deliver_status(session_maker, _status_payload(wamid=wamid, recipient=recipient))
    item = await _item(session_maker, batch_id)
    assert item.status == VOUCHER_PRODUCTION_ITEM_READ
    assert item.read_at is not None
    assert (await _facts(session_maker))[0].match_state == DELIVERY_FAILURE_MATCH_CONFLICTING
    assert (await _receipts(session_maker))[0].match_state == DELIVERY_RECEIPT_MATCH_CONFIRMED


async def test_a_concurrent_early_callback_and_acceptance_agree(
    session_maker, production_configuration, binding_key
) -> None:
    """Either order leaves the same answer, and neither loses the status.

    The two halves take the message-identity lock first, in the one global
    order, so they serialise on this identifier rather than racing: whichever
    commits first, the slot ends at ``read`` with one confirmed receipt.
    """
    batch_id, _claim = await _claimed_slot(session_maker)
    recipient = PHONES[0].lstrip("+")
    wamid = "wamid.EARLY-RACE-2"
    await asyncio.gather(
        _deliver_status(session_maker, _status_payload(wamid=wamid, status="read", recipient=recipient)),
        _accept(session_maker, batch_id, wamid),
    )
    item = await _item(session_maker, batch_id)
    rows = await _receipts(session_maker)
    assert len(rows) == 1
    assert rows[0].match_state == DELIVERY_RECEIPT_MATCH_CONFIRMED
    assert item.status == VOUCHER_PRODUCTION_ITEM_READ
    assert item.read_at is not None
