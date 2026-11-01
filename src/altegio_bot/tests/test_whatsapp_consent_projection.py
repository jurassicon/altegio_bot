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

from sqlalchemy import func, select

from altegio_bot.models.models import (
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
    CONSENT_COMMAND_STOP,
    apply_consent_decision,
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
    """The one direction that needs proof keeps needing it, from any source.

    A proven, verified opt-in is in the ledger; the historical card flag is
    still set, because nothing clears a legacy snapshot. The projection must not
    be able to turn that stale flag into a new refusal that outranks the proven
    decision — and it does not, because the aggregate read sees the card flag as
    a live refusal only while the card itself says so.
    """
    moment = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, ADDRESS)
            await apply_consent_decision(
                session,
                wa_address=ADDRESS,
                command=CONSENT_COMMAND_STOP,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source="inbound_command",
                observed_at=moment - timedelta(minutes=10),
                event_at=moment - timedelta(minutes=10),
            )
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, ADDRESS)
            lifted = await apply_consent_decision(
                session,
                wa_address=ADDRESS,
                command="start",
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source="inbound_command",
                observed_at=moment,
                event_at=moment,
            )
    assert lifted.applied

    report = await project(session_maker, apply=True)

    rows = await _suppressions(session_maker)
    assert len(rows) == 1
    assert rows[0].state == CONSENT_STATE_OPTED_IN
    assert report.refusals_written == 0
    async with session_maker() as session:
        decision = await consent_decision(session, phone=ADDRESS, include_pending=False)
    assert decision.suppressed is False


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
