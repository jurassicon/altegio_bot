"""The historical consent reconciliation, and its limits (§46.4.4, PR-22.1).

Two properties carry this module. The first is that the projection is safe:
dry run by default, no external effect at any point, nothing replayed, and a
second run that changes nothing the first run did not. The second is that it is
honest: what it cannot establish is counted and reported rather than rounded up
into a fact.
"""

from __future__ import annotations

import uuid as uuid_module
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import func, select

from altegio_bot.models.models import (
    CONSENT_SOURCE_INBOUND,
    CONSENT_SOURCE_PROJECTION,
    CONSENT_STATE_OPTED_IN,
    CONSENT_STATE_OPTED_OUT,
    INBOUND_AUTHENTICITY_SIGNED,
    INBOUND_AUTHENTICITY_UNVERIFIED,
    PROVIDER_EASYWEEK,
    CampaignRecipient,
    CampaignRun,
    Client,
    WhatsAppConsentSuppression,
    WhatsAppEvent,
)
from altegio_bot.whatsapp_consent import (
    CONSENT_COMMAND_START,
    CONSENT_COMMAND_STOP,
    apply_consent_decision,
    apply_inbound_consent,
    consent_decision,
    lock_consent_address,
)
from altegio_bot.whatsapp_consent_projection import (
    ORIGIN_CLIENT_FLAG,
    ORIGIN_RECIPIENT_FLAG,
    ORIGIN_STORED_EVENT,
    SKIP_ADDRESS_UNNORMALISABLE,
    SKIP_HISTORICAL_START,
    project,
)

ADDRESS = "+4915177995001"
RAW_FROM = "4915177995001"
OTHER_ADDRESS = "+4915177995002"
OTHER_RAW_FROM = "4915177995002"
BASE_EPOCH = 1_755_000_000


def _stored_payload(*, text: str, sender: str = RAW_FROM, msg_id: str, epoch: int = BASE_EPOCH) -> dict[str, Any]:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA-HIST",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {"phone_number_id": "PNID-HIST"},
                            "messages": [
                                {
                                    "from": sender,
                                    "id": msg_id,
                                    "timestamp": str(epoch),
                                    "type": "text",
                                    "text": {"body": text},
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


async def _store_event(
    session_maker,
    payload: dict[str, Any],
    *,
    dedupe_key: str,
    status: str = "processed",
) -> int:
    """A stored event exactly as the OLD ingress left it: no receipt, no proof."""
    async with session_maker() as session:
        async with session.begin():
            event = WhatsAppEvent(
                dedupe_key=dedupe_key,
                status=status,
                query={},
                headers={},
                payload=payload,
            )
            session.add(event)
            await session.flush()
            return int(event.id)


async def _add_opted_out_card(session_maker, address: str, *, moment: datetime | None = None) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider="altegio",
                    company_id=1,
                    altegio_client_id=int(address[-6:]),
                    phone_e164=address,
                    display_name="Historical",
                    raw={},
                    wa_opted_out=True,
                    wa_opted_out_at=moment or datetime.now(timezone.utc) - timedelta(days=30),
                    wa_opt_out_reason="wa:stop",
                )
            )


async def _suppressions(session_maker) -> list[WhatsAppConsentSuppression]:
    async with session_maker() as session:
        return list(
            (
                await session.scalars(
                    select(WhatsAppConsentSuppression).order_by(WhatsAppConsentSuppression.wa_address)
                )
            ).all()
        )


# ===========================================================================
# Dry run is the default, and it writes nothing
# ===========================================================================


async def test_a_dry_run_writes_nothing_and_still_reports_everything(session_maker) -> None:
    await _add_opted_out_card(session_maker, ADDRESS)
    await _store_event(
        session_maker,
        _stored_payload(text="стоп", sender=OTHER_RAW_FROM, msg_id="wamid.HIST1"),
        dedupe_key="wa:hist-1",
    )

    report = await project(session_maker)

    assert report.applied is False
    assert report.refusals_to_write == 2
    assert report.by_origin[ORIGIN_CLIENT_FLAG] == 1
    assert report.by_origin[ORIGIN_STORED_EVENT] == 1
    assert await _suppressions(session_maker) == []


async def test_a_previously_unrecognised_cyrillic_stop_is_found(session_maker) -> None:
    """The old keyword set had no «стоп», so this refusal was never recorded."""
    await _store_event(
        session_maker,
        _stored_payload(text="СТОП", msg_id="wamid.CYR"),
        dedupe_key="wa:cyr-1",
    )

    report = await project(session_maker, apply=True)

    assert report.cyrillic_stop_events == 1
    rows = await _suppressions(session_maker)
    assert [row.wa_address for row in rows] == [ADDRESS]
    assert rows[0].state == CONSENT_STATE_OPTED_OUT
    assert rows[0].decision_source == CONSENT_SOURCE_PROJECTION
    # A stored event cannot prove its own signature: the ingress stripped the
    # header before storing it, and the projection says so rather than guessing.
    assert rows[0].decision_authenticity == INBOUND_AUTHENTICITY_UNVERIFIED


async def test_applying_twice_changes_nothing_the_first_run_did_not(session_maker) -> None:
    await _add_opted_out_card(session_maker, ADDRESS)
    first = await project(session_maker, apply=True)
    assert first.refusals_written == 1
    before = (await _suppressions(session_maker))[0].consent_version

    second = await project(session_maker, apply=True)

    assert second.refusals_written == 0
    assert second.refusals_already_suppressed == 1
    assert (await _suppressions(session_maker))[0].consent_version == before


async def test_a_historical_start_is_never_projected(session_maker) -> None:
    """Its authenticity is unrecoverable, so it could not lift anything anyway."""
    await _store_event(
        session_maker,
        _stored_payload(text="START", msg_id="wamid.HSTART"),
        dedupe_key="wa:hstart-1",
    )

    report = await project(session_maker, apply=True)

    assert report.historical_starts_seen == 1
    assert report.skipped[SKIP_HISTORICAL_START] == 1
    assert await _suppressions(session_maker) == []


async def test_a_historical_start_cannot_lift_a_projected_refusal(session_maker) -> None:
    """§46.4.4 — an unprovable START is not permission, in either order."""
    await _store_event(
        session_maker,
        _stored_payload(text="стоп", msg_id="wamid.ORDER1", epoch=BASE_EPOCH),
        dedupe_key="wa:order-1",
    )
    await _store_event(
        session_maker,
        _stored_payload(text="START", msg_id="wamid.ORDER2", epoch=BASE_EPOCH + 3600),
        dedupe_key="wa:order-2",
    )

    await project(session_maker, apply=True)

    rows = await _suppressions(session_maker)
    assert len(rows) == 1
    assert rows[0].state == CONSENT_STATE_OPTED_OUT


async def test_the_projection_does_not_touch_the_stored_events(session_maker) -> None:
    """It is a projection, not a replay: nothing is reprocessed or re-marked."""
    event_id = await _store_event(
        session_maker,
        _stored_payload(text="стоп", msg_id="wamid.UNTOUCHED"),
        dedupe_key="wa:untouched-1",
        status="received",
    )

    await project(session_maker, apply=True)

    async with session_maker() as session:
        event = await session.get(WhatsAppEvent, event_id)
        assert event is not None
        assert event.status == "received"
        assert event.processed_at is None


async def test_a_chatwoot_mirror_is_not_counted_twice(session_maker) -> None:
    await _store_event(
        session_maker,
        _stored_payload(text="STOP", msg_id="wamid.REAL"),
        dedupe_key="wa:real-1",
    )
    await _store_event(
        session_maker,
        {"_chatwoot": {"source": "mirror"}, **_stored_payload(text="STOP", msg_id="wamid.MIRROR")},
        dedupe_key="chatwoot:mirror-1",
    )

    report = await project(session_maker)

    assert report.by_origin[ORIGIN_STORED_EVENT] == 1


async def test_a_flag_with_no_usable_number_is_counted_not_guessed(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider="altegio",
                    company_id=1,
                    altegio_client_id=990777,
                    phone_e164="not a phone",
                    display_name="Unusable",
                    raw={},
                    wa_opted_out=True,
                    wa_opted_out_at=datetime.now(timezone.utc),
                )
            )

    report = await project(session_maker, apply=True)

    assert report.skipped[SKIP_ADDRESS_UNNORMALISABLE] == 1
    assert await _suppressions(session_maker) == []


async def test_a_recipient_snapshot_is_projected_with_its_own_origin(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                provider=PROVIDER_EASYWEEK,
                campaign_code="hist",
                mode="preview",
                company_ids=[322579],
                period_start=datetime(2026, 2, 1, tzinfo=timezone.utc),
                period_end=datetime(2026, 2, 28, tzinfo=timezone.utc),
                status="completed",
            )
            session.add(run)
            await session.flush()
            session.add(
                CampaignRecipient(
                    campaign_run_id=run.id,
                    provider=PROVIDER_EASYWEEK,
                    company_id=322579,
                    phone_e164=ADDRESS,
                    display_name="Snapshot",
                    status="skipped",
                    is_opted_out=True,
                )
            )

    report = await project(session_maker, apply=True)

    assert report.by_origin[ORIGIN_RECIPIENT_FLAG] == 1
    assert (await _suppressions(session_maker))[0].state == CONSENT_STATE_OPTED_OUT


async def test_the_report_names_the_gap_it_can_never_close(session_maker) -> None:
    report = await project(session_maker)

    assert "historical_signature_unrecoverable" in report.source_gaps


async def test_a_truncated_scan_is_declared_incomplete(session_maker) -> None:
    """§46.4.4 — completeness is never claimed when the data does not prove it."""
    for index in range(3):
        await _store_event(
            session_maker,
            _stored_payload(text="стоп", sender=f"49151779950{index:02d}", msg_id=f"wamid.T{index}"),
            dedupe_key=f"wa:trunc-{index}",
        )

    report = await project(session_maker, event_limit=2)

    assert report.truncated is True
    assert any(gap.startswith("stored_events_truncated_at_") for gap in report.source_gaps)


# ===========================================================================
# Live ingress racing the projection
# ===========================================================================


async def test_a_live_refusal_landing_mid_run_is_not_overwritten(session_maker) -> None:
    """A stale read must converge on the newer state, never lower it.

    The projection reads its candidates, a live STOP then lands and records its
    own decision, and the apply pass runs afterwards against that stale read.
    Because every write goes through the ledger under the address lock, the live
    decision stays the recorded one and the projection is a no-op.
    """
    await _add_opted_out_card(session_maker, ADDRESS)
    dry = await project(session_maker)
    assert dry.refusals_to_write == 1

    moment = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, ADDRESS)
            live = await apply_consent_decision(
                session,
                wa_address=ADDRESS,
                command=CONSENT_COMMAND_STOP,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source="inbound_command",
                observed_at=moment,
                event_at=moment,
            )
    assert live.applied

    report = await project(session_maker, apply=True)

    rows = await _suppressions(session_maker)
    assert len(rows) == 1
    assert rows[0].decision_source == "inbound_command"
    assert rows[0].decision_authenticity == INBOUND_AUTHENTICITY_SIGNED
    assert report.refusals_written == 0


async def test_a_projection_cannot_undo_a_proven_opt_in(session_maker) -> None:
    """The reproduction from the review, with a candidate that really exists.

    The scenario this test is about needs THREE things, and the version it
    replaced had only the last two: a STOP sitting in a source the projection
    reads, a proven opt-in that came after it, and a real reconciliation run.
    Without the stored event the projection had nothing to collect, so it could
    not have undone anything and the test proved only that.

    With it, the run re-collects a STOP the ledger has already moved past. It
    must stay moved past: a verified, strictly-newer opt-in is the one decision
    in this phase that was expensive to establish, and a reconciliation that
    quietly reverted it every time it ran would be worse than no reconciliation.
    """
    from altegio_bot.whatsapp_consent import NOOP_STOP_SUPERSEDED

    stop_at = datetime.now(timezone.utc) - timedelta(hours=2)
    start_at = stop_at + timedelta(minutes=30)

    # The source: a stored event carrying the original STOP, exactly as the old
    # ingress left it.
    await _store_event(
        session_maker,
        _stored_payload(text="STOP", msg_id="wamid.HIST-STOP", epoch=int(stop_at.timestamp())),
        dedupe_key="wa:hist-stop",
    )
    # And the live decisions that followed it: the same STOP applied, then a
    # proven, verified, strictly newer START.
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, ADDRESS)
            await apply_consent_decision(
                session,
                wa_address=ADDRESS,
                command=CONSENT_COMMAND_STOP,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source="inbound_command",
                observed_at=stop_at,
                event_at=stop_at,
            )
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, ADDRESS)
            lifted = await apply_consent_decision(
                session,
                wa_address=ADDRESS,
                command=CONSENT_COMMAND_START,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source="inbound_command",
                observed_at=start_at,
                event_at=start_at,
            )
    assert lifted.applied

    dry = await project(session_maker)
    assert dry.refusals_seen >= 1, "the stored STOP must really be a candidate"

    report = await project(session_maker, apply=True)

    rows = await _suppressions(session_maker)
    assert len(rows) == 1
    assert rows[0].state == CONSENT_STATE_OPTED_IN
    assert rows[0].decision_source == "inbound_command"
    assert rows[0].evidence.get("superseded_stop_event_at") is not None
    assert report.refusals_written == 0
    assert report.refusals_noop >= 1
    async with session_maker() as session:
        decision = await consent_decision(session, phone=ADDRESS, include_pending=False)
    assert decision.suppressed is False

    # And again: a reconciliation that is safe to run once is not safe at all
    # unless it is safe to run every night.
    second = await project(session_maker, apply=True)
    assert second.refusals_written == 0
    rows = await _suppressions(session_maker)
    assert len(rows) == 1 and rows[0].state == CONSENT_STATE_OPTED_IN
    assert NOOP_STOP_SUPERSEDED  # the reason code the ledger records for it


async def test_a_projection_racing_a_live_start_converges_on_the_opt_in(session_maker) -> None:
    """The stale-read version of the same thing: the live decision wins.

    The projection reads its candidates while the refusal is still in force,
    and the proven START lands before the apply pass runs. The apply pass is
    therefore working from a snapshot in which this person is banned — and must
    still not re-ban them, because every write re-reads the ledger under the
    address lock.
    """
    stop_at = datetime.now(timezone.utc) - timedelta(hours=3)
    await _store_event(
        session_maker,
        _stored_payload(text="СТОП", msg_id="wamid.HIST-RACE", epoch=int(stop_at.timestamp())),
        dedupe_key="wa:hist-race",
    )
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, ADDRESS)
            await apply_consent_decision(
                session,
                wa_address=ADDRESS,
                command=CONSENT_COMMAND_STOP,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source="inbound_command",
                observed_at=stop_at,
                event_at=stop_at,
            )

    # The projection's own read happens here, while the ban is live.
    staged = await project(session_maker)
    assert staged.refusals_seen >= 1

    later = stop_at + timedelta(hours=1)
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, ADDRESS)
            lifted = await apply_consent_decision(
                session,
                wa_address=ADDRESS,
                command=CONSENT_COMMAND_START,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source="inbound_command",
                observed_at=later,
                event_at=later,
            )
    assert lifted.applied

    await project(session_maker, apply=True)

    rows = await _suppressions(session_maker)
    assert len(rows) == 1
    assert rows[0].state == CONSENT_STATE_OPTED_IN


async def test_a_projected_refusal_of_unknown_time_keeps_every_start_out(session_maker) -> None:
    """A card flag has no event time, and nothing can be ordered against it.

    This is the conservative half of the ordering rule, and it is the one an
    operator has to know about before running ``--apply``: a refusal projected
    from a legacy flag can never be lifted by a START, however well signed and
    however recent, because "strictly newer" has no answer. The alternative —
    comparing against our own reception clock — would be inventing an order
    Meta never promised.
    """
    from altegio_bot.whatsapp_consent import REFUSED_START_BAN_TIME_UNKNOWN

    await _add_opted_out_card(session_maker, ADDRESS)
    report = await project(session_maker, apply=True)
    assert report.refusals_written == 1

    rows = await _suppressions(session_maker)
    assert rows[0].state == CONSENT_STATE_OPTED_OUT
    assert rows[0].decided_event_at is None

    moment = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, ADDRESS)
            refused = await apply_consent_decision(
                session,
                wa_address=ADDRESS,
                command=CONSENT_COMMAND_START,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source="inbound_command",
                observed_at=moment,
                event_at=moment,
            )
    assert refused.applied is False
    assert refused.reason == REFUSED_START_BAN_TIME_UNKNOWN
    assert (await _suppressions(session_maker))[0].state == CONSENT_STATE_OPTED_OUT


# ===========================================================================
# The sender route decision of the day is honoured, never re-made
# ===========================================================================


async def test_an_event_the_ingress_ignored_is_never_projected(session_maker) -> None:
    """A deliberate refusal must not become a global suppression.

    ``status = "ignored"`` is the ingress saying this delivery named no sender
    route this deployment serves. The command in it was never executed, and
    projecting it would make the reconciliation the one way to apply exactly the
    commands the configuration exists to refuse — on an address that may belong
    to somebody else's customer entirely.
    """
    from altegio_bot.whatsapp_consent_projection import SKIP_ROUTE_NOT_ALLOWED

    await _store_event(
        session_maker,
        _stored_payload(text="STOP", msg_id="wamid.IGNORED-HIST"),
        dedupe_key="wa:hist-ignored",
        status="ignored",
    )

    report = await project(session_maker, apply=True)

    assert report.events_route_not_allowed == 1
    assert report.skipped.get(SKIP_ROUTE_NOT_ALLOWED) == 1
    assert await _suppressions(session_maker) == []


async def test_a_message_refused_by_its_own_route_is_never_projected(session_maker) -> None:
    """Per message, from the verdict the ingress recorded on the receipt.

    The delivery as a whole was accepted — one of its changes named an allowed
    route — and the message in question was refused individually. The receipt is
    where that distinction lives, so the reconciliation reads it rather than
    re-deciding against today's allow list, which may have changed since.
    """
    from altegio_bot.models.models import INBOUND_RESOLUTION_IGNORED_ROUTE, WhatsAppInboundReceipt
    from altegio_bot.whatsapp_consent import receipt_key
    from altegio_bot.whatsapp_consent_projection import SKIP_ROUTE_NOT_ALLOWED

    event_id = await _store_event(
        session_maker,
        _stored_payload(text="STOP", msg_id="wamid.REFUSED-ROUTE"),
        dedupe_key="wa:hist-refused-route",
    )
    moment = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppInboundReceipt(
                    receipt_key=receipt_key("wa:hist-refused-route", 0),
                    whatsapp_event_id=event_id,
                    message_index=0,
                    wa_address=ADDRESS,
                    command=CONSENT_COMMAND_STOP,
                    authenticity=INBOUND_AUTHENTICITY_UNVERIFIED,
                    route_allowed=False,
                    observed_at=moment,
                    resolved_at=moment,
                    resolution=INBOUND_RESOLUTION_IGNORED_ROUTE,
                    resolution_reason="ingress_sender_route_not_allowed",
                )
            )

    report = await project(session_maker, apply=True)

    assert report.messages_route_not_allowed == 1
    assert report.skipped.get(SKIP_ROUTE_NOT_ALLOWED) == 1
    assert await _suppressions(session_maker) == []


async def test_an_accepted_message_is_still_projected_beside_a_refused_one(session_maker) -> None:
    """The filter is per message, so it must not swallow the allowed one too."""
    from altegio_bot.models.models import INBOUND_RESOLUTION_IGNORED_ROUTE, WhatsAppInboundReceipt
    from altegio_bot.whatsapp_consent import receipt_key

    payload = _stored_payload(text="STOP", msg_id="wamid.MIXED-A")
    payload["entry"][0]["changes"][0]["value"]["messages"].append(
        {
            "from": OTHER_RAW_FROM,
            "id": "wamid.MIXED-B",
            "timestamp": str(BASE_EPOCH),
            "type": "text",
            "text": {"body": "STOP"},
        }
    )
    event_id = await _store_event(session_maker, payload, dedupe_key="wa:hist-mixed")
    moment = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppInboundReceipt(
                    receipt_key=receipt_key("wa:hist-mixed", 0),
                    whatsapp_event_id=event_id,
                    message_index=0,
                    wa_address=ADDRESS,
                    command=CONSENT_COMMAND_STOP,
                    authenticity=INBOUND_AUTHENTICITY_UNVERIFIED,
                    route_allowed=False,
                    observed_at=moment,
                    resolved_at=moment,
                    resolution=INBOUND_RESOLUTION_IGNORED_ROUTE,
                    resolution_reason="ingress_sender_route_not_allowed",
                )
            )

    report = await project(session_maker, apply=True)

    assert report.messages_route_not_allowed == 1
    addresses = [row.wa_address for row in await _suppressions(session_maker)]
    assert addresses == [OTHER_ADDRESS]


async def test_the_report_names_the_route_provenance_it_does_not_have(session_maker) -> None:
    """Said every run, because it never stops being true of old rows."""
    report = await project(session_maker)
    assert "historical_per_message_route_unrecorded" in report.as_safe_dict()["source_gaps"]


async def test_the_projection_creates_no_jobs_and_no_outbound_rows(session_maker) -> None:
    from altegio_bot.models.models import MessageJob, OutboxMessage

    await _add_opted_out_card(session_maker, ADDRESS)
    await _store_event(
        session_maker,
        _stored_payload(text="стоп", sender=OTHER_RAW_FROM, msg_id="wamid.NOEFFECT"),
        dedupe_key="wa:noeffect-1",
    )

    await project(session_maker, apply=True)

    async with session_maker() as session:
        assert (await session.scalar(select(func.count()).select_from(MessageJob))) == 0
        assert (await session.scalar(select(func.count()).select_from(OutboxMessage))) == 0


async def test_a_uuid_bound_historical_refusal_still_follows_the_identity(session_maker) -> None:
    customer = uuid_module.uuid4()
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=322579,
                    altegio_client_id=990888,
                    easyweek_customer_uuid=customer,
                    phone_e164=ADDRESS,
                    display_name="Identity",
                    raw={},
                    wa_opted_out=True,
                    wa_opted_out_at=datetime.now(timezone.utc),
                )
            )

    await project(session_maker, apply=True)

    rows = await _suppressions(session_maker)
    assert rows[0].banned_easyweek_customer_uuid == customer


# ===========================================================================
# Round 2, review fix 4 — the projection keeps the order of the facts
# ===========================================================================


async def _refuse_at(session_maker, *, address: str = ADDRESS, offset: int = 0) -> None:
    moment = datetime.fromtimestamp(BASE_EPOCH + offset, tz=timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, address)
            outcome = await apply_consent_decision(
                session,
                wa_address=address,
                command=CONSENT_COMMAND_STOP,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source=CONSENT_SOURCE_INBOUND,
                observed_at=moment,
                event_at=moment,
            )
    assert outcome.applied


async def _permit_at(session_maker, *, address: str = ADDRESS, offset: int) -> None:
    moment = datetime.fromtimestamp(BASE_EPOCH + offset, tz=timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, address)
            outcome = await apply_consent_decision(
                session,
                wa_address=address,
                command=CONSENT_COMMAND_START,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source=CONSENT_SOURCE_INBOUND,
                observed_at=moment,
                event_at=moment,
            )
    assert outcome.applied, outcome


async def _permit_now(session_maker, *, address: str = ADDRESS) -> None:
    """A proven opt-in observed NOW, whatever the historical sources say."""
    moment = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, address)
            outcome = await apply_consent_decision(
                session,
                wa_address=address,
                command=CONSENT_COMMAND_START,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source=CONSENT_SOURCE_INBOUND,
                observed_at=moment,
                event_at=moment,
            )
    assert outcome.applied, outcome


async def _add_opted_out_recipient(session_maker, address: str, *, created_at: datetime) -> None:
    """A preview-time snapshot of a refusal, with its own moment.

    ``CampaignRecipient.created_at`` is a plain column here rather than a server
    default, because WHEN the snapshot was taken is the whole question.
    """
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                provider=PROVIDER_EASYWEEK,
                campaign_code="historical",
                mode="preview",
                company_ids=[322579],
                period_start=datetime.now(timezone.utc) - timedelta(days=60),
                period_end=datetime.now(timezone.utc) - timedelta(days=30),
                status="completed",
            )
            session.add(run)
            await session.flush()
            session.add(
                CampaignRecipient(
                    campaign_run_id=run.id,
                    provider=PROVIDER_EASYWEEK,
                    company_id=322579,
                    phone_e164=address,
                    display_name="Snapshot",
                    status="skipped",
                    is_opted_out=True,
                    created_at=created_at,
                )
            )


async def test_a_snapshot_already_answered_by_a_proven_opt_in_is_not_reapplied(session_maker) -> None:
    """The review's sequence A, with every source a real stored row.

    STOP at 10:00, a preview snapshot taken at 10:10, a proven START at 10:30.
    The aggregate read gets this right already — a snapshot older than a proven
    opt-in is answered by it — but ``project(apply=True)`` wrote the snapshot as
    a FRESH refusal anyway. And a snapshot has no event time of its own, so what
    it created was a refusal of unknown order: by §46.4.4 permanent, liftable by
    nothing, from a source that was only ever a copy of a state the ledger had
    already moved past.
    """
    await _refuse_at(session_maker, offset=0)
    await _add_opted_out_recipient(
        session_maker, ADDRESS, created_at=datetime.fromtimestamp(BASE_EPOCH + 600, tz=timezone.utc)
    )
    await _permit_at(session_maker, offset=1800)

    async with session_maker() as session:
        before = await consent_decision(session, phone=ADDRESS, include_pending=False)
    assert before.suppressed is False

    report = await project(session_maker, apply=True)

    rows = await _suppressions(session_maker)
    assert [row.state for row in rows] == [CONSENT_STATE_OPTED_IN]
    assert rows[0].stop_event_time_unknown_at is None
    assert report.refusals_superseded == 1
    assert report.refusals_written == 0
    assert report.newly_suppressed_addresses == 0
    async with session_maker() as session:
        after = await consent_decision(session, phone=ADDRESS, include_pending=False)
    assert after.suppressed is False


async def test_a_card_flag_already_answered_by_a_proven_opt_in_is_not_reapplied(session_maker) -> None:
    """The same rule for the live card flag, which also carries no event time.

    The worker clears this flag when the ledger proves an opt-in, so a flag that
    is still set and older than the opt-in is a row the projection read a moment
    too early — not a refusal to make permanent.
    """
    await _refuse_at(session_maker, offset=0)
    await _add_opted_out_card(session_maker, ADDRESS, moment=datetime.fromtimestamp(BASE_EPOCH + 600, tz=timezone.utc))
    await _permit_at(session_maker, offset=1800)

    report = await project(session_maker, apply=True)

    rows = await _suppressions(session_maker)
    assert [row.state for row in rows] == [CONSENT_STATE_OPTED_IN]
    assert report.refusals_superseded == 1
    assert report.skipped.get("superseded_by_proven_opt_in") == 1


async def test_a_newer_stored_stop_moves_the_boundary_even_when_already_refused(session_maker) -> None:
    """The review's sequence B: an active refusal used to stop the search.

    The ledger refuses from 10:00 and a stored delivery nobody ever processed
    carries a STOP from 11:00. The collection-time shortcut skipped it — a
    refusal was already active, so there was "nothing to do" — and the boundary
    stayed at 10:00. A START from 10:30 then lifted a refusal this database
    could prove was older than a STOP it already held.
    """
    await _refuse_at(session_maker, offset=0)
    await _store_event(
        session_maker,
        _stored_payload(text="стоп", msg_id="wamid.R4NEWER", epoch=BASE_EPOCH + 3600),
        dedupe_key="wa:r4-newer",
    )

    report = await project(session_maker, apply=True)

    rows = await _suppressions(session_maker)
    assert len(rows) == 1
    assert rows[0].state == CONSENT_STATE_OPTED_OUT
    assert rows[0].last_stop_event_at == datetime.fromtimestamp(BASE_EPOCH + 3600, tz=timezone.utc)
    # Nothing was newly suppressed: the refusal was already active. The run
    # reports a no-op, which is what it was.
    assert report.refusals_written == 0
    assert report.refusals_noop == 1
    assert report.newly_suppressed_addresses == 0

    # And the START that used to win no longer can.
    moment = datetime.fromtimestamp(BASE_EPOCH + 1800, tz=timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, ADDRESS)
            outcome = await apply_consent_decision(
                session,
                wa_address=ADDRESS,
                command=CONSENT_COMMAND_START,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source=CONSENT_SOURCE_INBOUND,
                observed_at=moment,
                event_at=moment,
            )
    assert outcome.applied is False
    assert outcome.reason == "start_not_newer_than_stop"


async def test_a_stored_stop_with_an_unknown_time_stays_fail_closed(session_maker) -> None:
    """An unparseable timestamp is not a time, and the refusal says so.

    It is still applied — a refusal somebody sent is a refusal — and it records
    that its order cannot be established, which is what keeps a later START from
    claiming to beat it.
    """
    await _refuse_at(session_maker, offset=0)
    payload = _stored_payload(text="стоп", msg_id="wamid.R4UNK", epoch=BASE_EPOCH + 3600)
    payload["entry"][0]["changes"][0]["value"]["messages"][0]["timestamp"] = "not-a-timestamp"
    await _store_event(session_maker, payload, dedupe_key="wa:r4-unknown")

    await project(session_maker, apply=True)

    rows = await _suppressions(session_maker)
    assert len(rows) == 1
    assert rows[0].state == CONSENT_STATE_OPTED_OUT
    assert rows[0].stop_event_time_unknown_at is not None

    moment = datetime.fromtimestamp(BASE_EPOCH + 999_999, tz=timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, ADDRESS)
            outcome = await apply_consent_decision(
                session,
                wa_address=ADDRESS,
                command=CONSENT_COMMAND_START,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source=CONSENT_SOURCE_INBOUND,
                observed_at=moment,
                event_at=moment,
            )
    assert outcome.applied is False
    assert outcome.reason == "stop_event_time_unproven"


async def test_applying_the_new_rules_twice_still_changes_nothing(session_maker) -> None:
    """Idempotence, now that a stored STOP is applied even when already refused."""
    await _refuse_at(session_maker, offset=0)
    await _store_event(
        session_maker,
        _stored_payload(text="стоп", msg_id="wamid.R4TWICE", epoch=BASE_EPOCH + 3600),
        dedupe_key="wa:r4-twice",
    )
    await _add_opted_out_card(session_maker, OTHER_ADDRESS)

    first = await project(session_maker, apply=True)
    before = [(row.wa_address, row.state, int(row.consent_version)) for row in await _suppressions(session_maker)]
    second = await project(session_maker, apply=True)
    after = [(row.wa_address, row.state, int(row.consent_version)) for row in await _suppressions(session_maker)]

    assert before == after
    assert first.refusals_written == 1
    assert second.refusals_written == 0
    assert second.newly_suppressed_addresses == 0


async def test_a_proven_start_between_the_read_and_the_write_wins(session_maker, monkeypatch) -> None:
    """A SNAPSHOT collected while it still looked live, overtaken by an opt-in.

    This is the case the write-time re-decision exists for, and it has to be a
    snapshot: a preview row copies a state, so one taken before a proven opt-in
    has already been answered by it. Deciding only at collection time would
    write it anyway — and since a snapshot carries no event time, the refusal it
    created would be one §46.4.4 makes permanent.
    """
    import altegio_bot.whatsapp_consent as consent_module
    import altegio_bot.whatsapp_consent_projection as projection_module

    await _refuse_at(session_maker, offset=0)
    await _permit_at(session_maker, offset=600)
    await _add_opted_out_recipient(
        session_maker, ADDRESS, created_at=datetime.fromtimestamp(BASE_EPOCH + 1200, tz=timezone.utc)
    )

    real = projection_module.apply_projected_refusal
    state = {"armed": True}

    async def _patched(session: Any, refusal: Any) -> Any:
        if state["armed"]:
            state["armed"] = False
            # A refusal and then a proven, strictly newer opt-in: the ledger has
            # moved past this snapshot while the run was in flight.
            await _refuse_at(session_maker, offset=1800)
            await _permit_now(session_maker)
        return await real(session, refusal)

    monkeypatch.setattr(projection_module, "apply_projected_refusal", _patched)
    report = await project(session_maker, apply=True)

    assert state["armed"] is False
    rows = await _suppressions(session_maker)
    assert [row.state for row in rows] == [CONSENT_STATE_OPTED_IN]
    # The whole point: no refusal of unknown order was created on top of the
    # proven opt-in, so the person is not banned for good by a stale read.
    assert rows[0].stop_event_time_unknown_at is None
    assert report.refusals_superseded == 1
    assert report.refusals_written == 0
    assert report.cards_converged == 0
    assert consent_module.NOOP_PROJECTION_SUPERSEDED == "projection_older_than_proven_opt_in"


# ---------------------------------------------------------------------------
# Round 3, review fix 3 — a stored message is not a snapshot
# ---------------------------------------------------------------------------


async def _delayed_start_setup(session_maker) -> int:
    """The review's sequence: a STOP nobody can order, behind a late START.

    * a known STOP, event and observed time 10:00;
    * a stored STOP whose timestamp cannot be read, RECEIVED at 10:20;
    * a signed START whose own event time is 10:10, received at 10:30.

    The START is proven strictly newer than the refusal it lifts, so it applies.
    What must not follow is that the stored STOP counts as stale: it was only
    received earlier, which says nothing about when it was sent.
    """
    await _refuse_at(session_maker, offset=0)
    payload = _stored_payload(text="стоп", msg_id="wamid.R3DELAY", epoch=BASE_EPOCH + 600)
    payload["entry"][0]["changes"][0]["value"]["messages"][0]["timestamp"] = "not-a-timestamp"
    event_id = await _store_event(session_maker, payload, dedupe_key="wa:r3-delayed")
    async with session_maker() as session:
        async with session.begin():
            event = await session.get(WhatsAppEvent, event_id)
            event.received_at = datetime.fromtimestamp(BASE_EPOCH + 1200, tz=timezone.utc)

    moment = datetime.fromtimestamp(BASE_EPOCH + 1800, tz=timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, ADDRESS)
            lifted = await apply_consent_decision(
                session,
                wa_address=ADDRESS,
                command=CONSENT_COMMAND_START,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source=CONSENT_SOURCE_INBOUND,
                observed_at=moment,
                event_at=datetime.fromtimestamp(BASE_EPOCH + 600, tz=timezone.utc),
            )
    assert lifted.applied, lifted
    return event_id


@pytest.mark.parametrize("path", ["projection", "live"])
async def test_a_stored_stop_of_unknown_order_refuses_on_both_paths(session_maker, path: str) -> None:
    """One message, two ways of reaching it, and they must agree.

    The projection used to drop it — "received before the opt-in, therefore
    stale" — while ``apply_inbound_consent`` applied it and refused. Whether
    somebody could be messaged then depended on which of the two got to the
    event first, which is the one thing §46.4.4 rules out: an unproven order is
    not a permission.
    """
    event_id = await _delayed_start_setup(session_maker)

    if path == "projection":
        report = await project(session_maker, apply=True)
        assert report.refusals_written == 1
        assert report.refusals_superseded == 0
    else:
        await apply_inbound_consent(session_maker, event_id=event_id)

    rows = await _suppressions(session_maker)
    assert len(rows) == 1
    assert rows[0].state == CONSENT_STATE_OPTED_OUT
    assert rows[0].stop_event_time_unknown_at is not None
    async with session_maker() as session:
        decision = await consent_decision(session, phone=ADDRESS, include_pending=False)
    assert decision.suppressed is True

    # And nothing can lift it afterwards: the order cannot be established.
    moment = datetime.fromtimestamp(BASE_EPOCH + 99_999, tz=timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, ADDRESS)
            outcome = await apply_consent_decision(
                session,
                wa_address=ADDRESS,
                command=CONSENT_COMMAND_START,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source=CONSENT_SOURCE_INBOUND,
                observed_at=moment,
                event_at=moment,
            )
    assert outcome.applied is False
    assert outcome.reason == "stop_event_time_unproven"


async def test_a_stored_stop_collected_before_a_concurrent_start_still_refuses(session_maker, monkeypatch) -> None:
    """Collected while the ledger refused, written after a proven opt-in landed.

    A snapshot in this position is dropped. A real message is not: it goes
    through the ordinary ledger rules, and an unknown order refuses there.
    """
    import altegio_bot.whatsapp_consent_projection as projection_module

    await _refuse_at(session_maker, offset=0)
    payload = _stored_payload(text="стоп", msg_id="wamid.R3RACE2", epoch=BASE_EPOCH + 600)
    payload["entry"][0]["changes"][0]["value"]["messages"][0]["timestamp"] = "not-a-timestamp"
    event_id = await _store_event(session_maker, payload, dedupe_key="wa:r3-race2")
    async with session_maker() as session:
        async with session.begin():
            event = await session.get(WhatsAppEvent, event_id)
            event.received_at = datetime.fromtimestamp(BASE_EPOCH + 600, tz=timezone.utc)

    real = projection_module.apply_projected_refusal
    state = {"armed": True}

    async def _patched(session: Any, refusal: Any) -> Any:
        if state["armed"]:
            state["armed"] = False
            await _permit_now(session_maker)
        return await real(session, refusal)

    monkeypatch.setattr(projection_module, "apply_projected_refusal", _patched)
    report = await project(session_maker, apply=True)

    assert state["armed"] is False
    rows = await _suppressions(session_maker)
    assert [row.state for row in rows] == [CONSENT_STATE_OPTED_OUT]
    assert rows[0].stop_event_time_unknown_at is not None
    assert report.refusals_written == 1
    assert report.refusals_superseded == 0


async def test_the_projection_brings_the_card_flag_with_it(session_maker) -> None:
    """A refusal this run writes also lands on the flag every sender reads.

    Without it a run left the ledger refusing and ``Client.wa_opted_out`` saying
    sendable, and that flag is what ``message_planner``, the outbox worker and
    every campaign path read — so the refusal was ignored by everything except
    the new consent guard.
    """
    from altegio_bot.campaigns.segment import compute_excluded_reason

    await _store_event(
        session_maker,
        _stored_payload(text="стоп", msg_id="wamid.R3CARD", epoch=BASE_EPOCH),
        dedupe_key="wa:r3-card",
    )
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider="altegio",
                    company_id=1,
                    altegio_client_id=995111,
                    phone_e164=ADDRESS,
                    display_name="Sendable",
                    raw={},
                    wa_opted_out=False,
                )
            )

    report = await project(session_maker, apply=True)

    assert report.refusals_written == 1
    assert report.cards_converged == 1
    async with session_maker() as session:
        card = (await session.execute(select(Client).where(Client.phone_e164 == ADDRESS))).scalar_one()
    assert card.wa_opted_out is True
    assert card.wa_opt_out_reason == "wa:consent_projection"
    assert (
        compute_excluded_reason(
            wa_opted_out=bool(card.wa_opted_out),
            phone_e164=card.phone_e164,
            count_before=0,
            lash_count=1,
            attended_lash_count=1,
        )
        == "opted_out"
    )


async def test_a_card_that_already_refuses_is_not_restamped(session_maker) -> None:
    """``wa_opted_out_at`` says when somebody refused, not when a run noticed."""
    await _add_opted_out_card(session_maker, ADDRESS)
    await _store_event(
        session_maker,
        _stored_payload(text="стоп", msg_id="wamid.R3KEEP", epoch=BASE_EPOCH),
        dedupe_key="wa:r3-keep",
    )
    async with session_maker() as session:
        before = (await session.execute(select(Client).where(Client.phone_e164 == ADDRESS))).scalar_one()
        stamped, reason = before.wa_opted_out_at, before.wa_opt_out_reason

    report = await project(session_maker, apply=True)

    assert report.cards_converged == 0
    async with session_maker() as session:
        after = (await session.execute(select(Client).where(Client.phone_e164 == ADDRESS))).scalar_one()
    assert after.wa_opted_out is True
    assert after.wa_opted_out_at == stamped
    assert after.wa_opt_out_reason == reason


async def test_the_dry_run_of_the_new_rules_writes_nothing(session_maker) -> None:
    """Including the stored STOP that WOULD now be applied over an active refusal."""
    await _refuse_at(session_maker, offset=0)
    await _store_event(
        session_maker,
        _stored_payload(text="стоп", msg_id="wamid.R4DRY", epoch=BASE_EPOCH + 3600),
        dedupe_key="wa:r4-dry",
    )
    before = [
        (row.wa_address, int(row.consent_version), row.last_stop_event_at) for row in await _suppressions(session_maker)
    ]

    report = await project(session_maker)

    assert report.applied is False
    assert report.refusals_to_write == 1
    assert report.refusals_written == 0
    after = [
        (row.wa_address, int(row.consent_version), row.last_stop_event_at) for row in await _suppressions(session_maker)
    ]
    assert before == after


async def test_an_ignored_route_event_is_still_excluded_under_the_new_rules(session_maker) -> None:
    """The route decision of the day is still honoured, refusal active or not."""
    await _refuse_at(session_maker, offset=0)
    await _store_event(
        session_maker,
        _stored_payload(text="стоп", msg_id="wamid.R4IGN", epoch=BASE_EPOCH + 3600),
        dedupe_key="wa:r4-ignored",
        status="ignored",
    )

    report = await project(session_maker, apply=True)

    assert report.events_route_not_allowed == 1
    rows = await _suppressions(session_maker)
    assert rows[0].last_stop_event_at == datetime.fromtimestamp(BASE_EPOCH, tz=timezone.utc)
    assert report.refusals_written == 0
    assert report.refusals_noop == 0


async def test_the_report_counts_what_the_writes_actually_did(session_maker) -> None:
    """Three candidates, three different outcomes, one honest report."""
    # 1. A stored STOP for somebody with no ledger row at all: a real new refusal.
    await _store_event(
        session_maker,
        _stored_payload(text="стоп", sender=OTHER_RAW_FROM, msg_id="wamid.R4R1", epoch=BASE_EPOCH),
        dedupe_key="wa:r4-report-1",
    )
    # 2. A stored STOP for an address already refused: a no-op that still moves
    #    the boundary.
    await _refuse_at(session_maker, offset=0)
    await _store_event(
        session_maker,
        _stored_payload(text="стоп", msg_id="wamid.R4R2", epoch=BASE_EPOCH + 3600),
        dedupe_key="wa:r4-report-2",
    )
    # 3. A snapshot for a third address answered by a proven opt-in: superseded.
    third = "+4915177995003"
    await _refuse_at(session_maker, address=third, offset=0)
    await _add_opted_out_recipient(
        session_maker, third, created_at=datetime.fromtimestamp(BASE_EPOCH + 600, tz=timezone.utc)
    )
    await _permit_at(session_maker, address=third, offset=1800)

    report = await project(session_maker, apply=True)

    assert report.refusals_written == 1
    assert report.refusals_noop == 1
    assert report.refusals_superseded == 1
    assert report.newly_suppressed_addresses == 1
    safe = report.as_safe_dict()
    assert safe["refusals"]["written"] == 1
    assert safe["refusals"]["superseded"] == 1
    assert safe["refusals"]["newly_suppressed_addresses"] == 1
    # And the states really are those three.
    rows = {row.wa_address: row.state for row in await _suppressions(session_maker)}
    assert rows == {
        ADDRESS: CONSENT_STATE_OPTED_OUT,
        OTHER_ADDRESS: CONSENT_STATE_OPTED_OUT,
        third: CONSENT_STATE_OPTED_IN,
    }
