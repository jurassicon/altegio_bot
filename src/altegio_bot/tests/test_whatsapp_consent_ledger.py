"""Durable WhatsApp consent: recognition, durability and ordering (§46.4, PR-22.1).

Driven through the real ingress and the real worker entry point wherever the
property under test is about the production path. A helper that writes a
suppression row directly would prove the helper works and nothing about whether
a STOP arriving at the webhook actually stops anything.

Nothing here sends: the provider is a capture double, and several tests assert
that the count of outbound messages is exactly zero.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import uuid as uuid_module
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select

import altegio_bot.webhooks.whatsapp as wa_module
from altegio_bot.main import app
from altegio_bot.models.models import (
    CONSENT_COMMAND_NONE,
    CONSENT_COMMAND_START,
    CONSENT_COMMAND_STOP,
    CONSENT_STATE_OPTED_IN,
    CONSENT_STATE_OPTED_OUT,
    INBOUND_AUTHENTICITY_SIGNED,
    INBOUND_AUTHENTICITY_UNVERIFIED,
    INBOUND_RESOLUTION_IGNORED_ROUTE,
    INBOUND_RESOLUTION_REFUSED,
    PROVIDER_EASYWEEK,
    CampaignRecipient,
    CampaignRun,
    Client,
    WhatsAppConsentSuppression,
    WhatsAppEvent,
    WhatsAppInboundReceipt,
    WhatsAppSender,
)
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.settings import settings
from altegio_bot.whatsapp_consent import (
    GUARD_ALLOWED,
    GUARD_PENDING_INBOUND,
    GUARD_SUPPRESSED,
    REFUSED_START_IDENTIFIER_CONFLICT,
    REFUSED_START_NOT_NEWER,
    REFUSED_START_UNVERIFIED,
    SOURCE_CLIENT_FLAG,
    SOURCE_RECIPIENT_FLAG,
    SOURCE_SUPPRESSION,
    apply_inbound_consent,
    consent_decision,
    consent_unchanged,
    final_send_guard,
    is_suppressed,
)
from altegio_bot.workers import whatsapp_inbox_worker as worker_module

URL = "/webhook/whatsapp"
APP_SECRET = "meta-app-secret-for-tests"
PNID = "PNID-CONSENT"
ADDRESS = "+4915177991001"
RAW_FROM = "4915177991001"
OTHER_ADDRESS = "+4915177991002"
OTHER_RAW_FROM = "4915177991002"

BASE_EPOCH = 1_760_000_000


@pytest.fixture(autouse=True)
def _allow_the_test_sender_route(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make this module's sender route an allowed one.

    The endpoint marks a delivery from an unlisted ``phone_number_id`` as
    ``ignored`` and no worker ever processes it, so without this every test here
    would be measuring the ignored-route path instead of the consent path. The
    one test that is about that path overrides this itself.
    """
    monkeypatch.setattr(settings, "whatsapp_allowed_phone_number_ids", PNID)


class CaptureProvider(WhatsAppProvider):
    """Counts what would have been sent. Never sends."""

    def __init__(self) -> None:
        self.sent: list[tuple[int, str, str]] = []

    async def send(self, sender_id: int, phone_e164: str, text: str, contact_name: str | None = None) -> str:
        self.sent.append((sender_id, phone_e164, text))
        return f"wamid.ACK{len(self.sent)}"


def _message(
    *,
    text: str,
    sender: str = RAW_FROM,
    msg_id: str = "wamid.M1",
    epoch: int = BASE_EPOCH,
    msg_type: str = "text",
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "from": sender,
        "id": msg_id,
        "timestamp": str(epoch),
        "type": msg_type,
    }
    if msg_type == "text":
        body["text"] = {"body": text}
    elif msg_type == "reaction":
        body["reaction"] = {"emoji": text, "message_id": "wamid.TARGET"}
    return body


def _payload(*messages: dict[str, Any], phone_number_id: str = PNID) -> dict[str, Any]:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA-CONSENT",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {"phone_number_id": phone_number_id},
                            "messages": list(messages),
                        },
                    }
                ],
            }
        ],
    }


def _sign(body: bytes, secret: str = APP_SECRET) -> str:
    return "sha256=" + hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


async def _ingest(
    session_maker,
    payload: dict[str, Any],
    *,
    signed: bool = True,
    monkeypatch: pytest.MonkeyPatch | None = None,
) -> Any:
    """POST one payload at the real endpoint, with or without a valid signature.

    Signing is the only way a test can make the ingress record
    ``signature_verified``, which is deliberate: the proof is the HMAC over the
    raw bytes and nothing else may stand in for it.

    ``signed=False`` means a deployment with NO app secret configured, which is
    the only way an unverified delivery is accepted at all: with a secret set,
    an unsigned request is a 403 and never becomes an event. That is exactly why
    "unverified" is a real production state worth testing — it is what every
    delivery looks like until somebody configures ``META_APP_SECRET``.
    """
    assert monkeypatch is not None
    body = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if signed:
        monkeypatch.setattr(settings, "meta_app_secret", APP_SECRET)
        headers["X-Hub-Signature-256"] = _sign(body)
    else:
        monkeypatch.setattr(settings, "meta_app_secret", "")
    original = wa_module.SessionLocal
    try:
        wa_module.SessionLocal = session_maker  # type: ignore[assignment]
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            return await client.post(URL, content=body, headers=headers)
    finally:
        wa_module.SessionLocal = original


async def _process(session_maker, provider: WhatsAppProvider, event_id: int) -> None:
    """Run the production per-event path, with its own session factory."""
    original = worker_module.SessionLocal
    try:
        worker_module.SessionLocal = session_maker  # type: ignore[assignment]
        await worker_module.process_one_event(event_id, provider)
    finally:
        worker_module.SessionLocal = original


async def _seed_sender(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            existing = await session.scalar(select(WhatsAppSender.id).where(WhatsAppSender.phone_number_id == PNID))
            if existing is None:
                session.add(
                    WhatsAppSender(
                        company_id=1,
                        sender_code="default",
                        phone_number_id=PNID,
                        display_phone="+4930000000",
                        is_active=True,
                    )
                )


async def _event_ids(session_maker) -> list[int]:
    async with session_maker() as session:
        return [int(value) for value in (await session.scalars(select(WhatsAppEvent.id).order_by(WhatsAppEvent.id)))]


async def _suppression(session_maker, address: str = ADDRESS) -> WhatsAppConsentSuppression | None:
    async with session_maker() as session:
        return (
            await session.execute(
                select(WhatsAppConsentSuppression).where(WhatsAppConsentSuppression.wa_address == address)
            )
        ).scalar_one_or_none()


async def _receipts(session_maker) -> list[WhatsAppInboundReceipt]:
    async with session_maker() as session:
        return list(
            (
                await session.scalars(
                    select(WhatsAppInboundReceipt).order_by(
                        WhatsAppInboundReceipt.whatsapp_event_id, WhatsAppInboundReceipt.message_index
                    )
                )
            ).all()
        )


async def _deliver_stop(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    *,
    text: str = "STOP",
    sender: str = RAW_FROM,
    msg_id: str = "wamid.STOP1",
    epoch: int = BASE_EPOCH,
    signed: bool = True,
) -> CaptureProvider:
    """Ingest and process one command end to end."""
    await _seed_sender(session_maker)
    provider = CaptureProvider()
    await _ingest(
        session_maker,
        _payload(_message(text=text, sender=sender, msg_id=msg_id, epoch=epoch)),
        signed=signed,
        monkeypatch=monkeypatch,
    )
    for event_id in await _event_ids(session_maker):
        await _process(session_maker, provider, event_id)
    return provider


# ===========================================================================
# Task 1 — recognition, without narrowing what already worked
# ===========================================================================


@pytest.mark.parametrize(
    "text",
    [
        "STOP",
        "stop",
        "СтОп",
        "СТОП",
        "«СТОП»",
        "  stop  ",
        "stop.",
        "*STOP*",
        "STOP please",
        "стоп, bitte",
        "unsubscribe",
        "abmelden",
    ],
)
async def test_every_spelling_of_a_refusal_is_recorded(session_maker, monkeypatch, text: str) -> None:
    await _deliver_stop(session_maker, monkeypatch, text=text)

    row = await _suppression(session_maker)
    assert row is not None, text
    assert row.state == CONSENT_STATE_OPTED_OUT


@pytest.mark.parametrize("text", ["nonstop", "bitte stop", "stop-messages", "hallo", "Stopp mal bitte?"])
async def test_a_word_that_merely_contains_the_command_is_not_one(session_maker, monkeypatch, text: str) -> None:
    """A substring is not a command, and the FIRST token is still the rule.

    ``Stopp`` is included deliberately: it is one letter away from the German
    spelling and must not be accepted, because accepting near-misses is how a
    refusal starts being invented for people who did not make one.
    """
    await _deliver_stop(session_maker, monkeypatch, text=text)

    assert await _suppression(session_maker) is None


async def test_a_reaction_is_never_a_command(session_maker, monkeypatch) -> None:
    await _seed_sender(session_maker)
    provider = CaptureProvider()
    await _ingest(
        session_maker,
        _payload(_message(text="🛑", msg_type="reaction", msg_id="wamid.REACT")),
        monkeypatch=monkeypatch,
    )
    for event_id in await _event_ids(session_maker):
        await _process(session_maker, provider, event_id)

    assert await _suppression(session_maker) is None
    receipts = await _receipts(session_maker)
    assert [receipt.command for receipt in receipts] == [CONSENT_COMMAND_NONE]


async def test_a_refusal_in_the_second_message_counts(session_maker, monkeypatch) -> None:
    """The old path read ``actions[0]`` only, so this STOP was recorded nowhere."""
    await _seed_sender(session_maker)
    provider = CaptureProvider()
    await _ingest(
        session_maker,
        _payload(
            _message(text="hallo, eine Frage", msg_id="wamid.A"),
            _message(text="стоп", msg_id="wamid.B", epoch=BASE_EPOCH + 1),
        ),
        monkeypatch=monkeypatch,
    )
    for event_id in await _event_ids(session_maker):
        await _process(session_maker, provider, event_id)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT


async def test_two_senders_in_one_payload_are_judged_separately(session_maker, monkeypatch) -> None:
    """Each message against its OWN sender, never against the payload's first."""
    await _seed_sender(session_maker)
    provider = CaptureProvider()
    await _ingest(
        session_maker,
        _payload(
            _message(text="STOP", sender=RAW_FROM, msg_id="wamid.S1"),
            _message(text="hallo", sender=OTHER_RAW_FROM, msg_id="wamid.S2"),
        ),
        monkeypatch=monkeypatch,
    )
    for event_id in await _event_ids(session_maker):
        await _process(session_maker, provider, event_id)

    assert (await _suppression(session_maker, ADDRESS)) is not None
    assert (await _suppression(session_maker, OTHER_ADDRESS)) is None


async def test_a_chatwoot_mirror_does_not_execute_the_command_again(session_maker, monkeypatch) -> None:
    """A mirror of a Meta message is the SAME message, not a second one."""
    await _deliver_stop(session_maker, monkeypatch)
    first = await _suppression(session_maker)
    assert first is not None

    async with session_maker() as session:
        async with session.begin():
            mirror = WhatsAppEvent(
                dedupe_key="chatwoot:mirror-1",
                status="received",
                query={},
                headers={},
                payload={
                    "_chatwoot": {"source": "mirror"},
                    **_payload(_message(text="START", msg_id="wamid.MIRROR", epoch=BASE_EPOCH + 99)),
                },
            )
            session.add(mirror)
            await session.flush()
            mirror_id = int(mirror.id)

    await _process(session_maker, CaptureProvider(), mirror_id)

    after = await _suppression(session_maker)
    assert after is not None
    assert after.state == CONSENT_STATE_OPTED_OUT
    assert after.consent_version == first.consent_version


async def test_a_redelivered_payload_changes_nothing(session_maker, monkeypatch) -> None:
    """Dedupe at the ingress, idempotence at the ledger. Both, not either."""
    await _deliver_stop(session_maker, monkeypatch)
    first = await _suppression(session_maker)
    assert first is not None

    await _deliver_stop(session_maker, monkeypatch)

    after = await _suppression(session_maker)
    assert after is not None
    assert after.consent_version == first.consent_version
    async with session_maker() as session:
        assert (await session.scalar(select(func.count()).select_from(WhatsAppEvent))) == 1


async def test_reprocessing_the_same_event_is_a_no_op(session_maker, monkeypatch) -> None:
    """A recovery pass re-examining an event must converge, not re-apply."""
    await _deliver_stop(session_maker, monkeypatch)
    first = await _suppression(session_maker)
    assert first is not None

    for event_id in await _event_ids(session_maker):
        await _process(session_maker, CaptureProvider(), event_id)

    after = await _suppression(session_maker)
    assert after is not None
    assert after.consent_version == first.consent_version


# ===========================================================================
# Task 2 — a refusal that does not need a card
# ===========================================================================


async def test_a_refusal_survives_having_no_client_card_at_all(session_maker, monkeypatch) -> None:
    await _deliver_stop(session_maker, monkeypatch)

    async with session_maker() as session:
        assert (await session.scalar(select(func.count()).select_from(Client).where(Client.phone_e164 == ADDRESS))) == 0
        assert await is_suppressed(session, phone=ADDRESS)


async def test_creating_a_card_afterwards_does_not_lift_the_refusal(session_maker, monkeypatch) -> None:
    await _deliver_stop(session_maker, monkeypatch)

    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=322579,
                    altegio_client_id=990001,
                    phone_e164=ADDRESS,
                    display_name="Later card",
                    raw={},
                    wa_opted_out=False,
                )
            )

    async with session_maker() as session:
        decision = await consent_decision(session, phone=ADDRESS)
        assert decision.suppressed
        assert SOURCE_SUPPRESSION in decision.sources


async def test_a_refusal_follows_a_proven_uuid_through_a_number_change(session_maker, monkeypatch) -> None:
    """§46.4.2 — the ban travels with the identity, not with the string."""
    customer = uuid_module.uuid4()
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=322579,
                    altegio_client_id=990002,
                    easyweek_customer_uuid=customer,
                    phone_e164=ADDRESS,
                    display_name="Before the change",
                    raw={},
                )
            )

    await _deliver_stop(session_maker, monkeypatch)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.banned_easyweek_customer_uuid == customer

    # The number changes. The old address is no longer on any card.
    async with session_maker() as session:
        async with session.begin():
            card = (await session.execute(select(Client).where(Client.easyweek_customer_uuid == customer))).scalar_one()
            card.phone_e164 = OTHER_ADDRESS

    async with session_maker() as session:
        assert await is_suppressed(session, phone=OTHER_ADDRESS, easyweek_customer_uuid=customer)


async def test_an_ambiguous_phone_match_binds_no_uuid_and_still_refuses(session_maker, monkeypatch) -> None:
    """A phone match bans. It never proves whose identity it is."""
    async with session_maker() as session:
        async with session.begin():
            for index, company in enumerate((322579, 322580)):
                session.add(
                    Client(
                        provider=PROVIDER_EASYWEEK,
                        company_id=company,
                        altegio_client_id=990010 + index,
                        easyweek_customer_uuid=uuid_module.uuid4(),
                        phone_e164=ADDRESS,
                        display_name=f"Card {index}",
                        raw={},
                    )
                )

    await _deliver_stop(session_maker, monkeypatch)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.banned_easyweek_customer_uuid is None
    assert row.evidence.get("uuid_binding") == "ambiguous"
    async with session_maker() as session:
        assert await is_suppressed(session, phone=ADDRESS)


async def test_an_altegio_card_flag_still_vetoes_easyweek_work(session_maker) -> None:
    """§46.4.3 — a refusal is not scoped to the provider that recorded it."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider="altegio",
                    company_id=1,
                    altegio_client_id=990020,
                    phone_e164=ADDRESS,
                    display_name="Altegio card",
                    raw={},
                    wa_opted_out=True,
                    wa_opted_out_at=datetime.now(timezone.utc),
                    wa_opt_out_reason="wa:stop",
                )
            )

    async with session_maker() as session:
        decision = await consent_decision(session, phone=ADDRESS)
        assert decision.suppressed
        assert SOURCE_CLIENT_FLAG in decision.sources


async def test_a_historical_recipient_snapshot_still_vetoes(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                provider=PROVIDER_EASYWEEK,
                campaign_code="whatever",
                mode="preview",
                company_ids=[322579],
                period_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
                period_end=datetime(2026, 1, 31, tzinfo=timezone.utc),
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

    async with session_maker() as session:
        decision = await consent_decision(session, phone=ADDRESS)
        assert decision.suppressed
        assert SOURCE_RECIPIENT_FLAG in decision.sources


# ===========================================================================
# STOP / START ordering
# ===========================================================================


async def test_an_unverified_start_never_lifts_a_refusal(session_maker, monkeypatch) -> None:
    """§46.4.7 — a check that is not configured is not a check.

    The whole scenario runs on a deployment with no ``META_APP_SECRET``, because
    that is the only way an unverified delivery exists: with a secret set, an
    unsigned request never gets past the 403. The STOP still applies — a refusal
    does not need proof — and the START, which does, is refused.
    """
    await _deliver_stop(session_maker, monkeypatch, epoch=BASE_EPOCH, signed=False)

    await _seed_sender(session_maker)
    await _ingest(
        session_maker,
        _payload(_message(text="START", msg_id="wamid.START1", epoch=BASE_EPOCH + 60)),
        signed=False,
        monkeypatch=monkeypatch,
    )
    for event_id in await _event_ids(session_maker):
        await _process(session_maker, CaptureProvider(), event_id)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    assert row.evidence.get("last_refused_start_reason") == REFUSED_START_UNVERIFIED


async def test_a_proven_newer_start_lifts_the_refusal(session_maker, monkeypatch) -> None:
    await _deliver_stop(session_maker, monkeypatch, epoch=BASE_EPOCH)

    await _ingest(
        session_maker,
        _payload(_message(text="START", msg_id="wamid.START2", epoch=BASE_EPOCH + 60)),
        monkeypatch=monkeypatch,
    )
    for event_id in await _event_ids(session_maker):
        await _process(session_maker, CaptureProvider(), event_id)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_IN
    assert row.decision_authenticity == INBOUND_AUTHENTICITY_SIGNED
    assert row.decided_event_at is not None


async def test_an_older_start_arriving_late_does_not_undo_a_newer_stop(session_maker, monkeypatch) -> None:
    await _deliver_stop(session_maker, monkeypatch, epoch=BASE_EPOCH + 600, msg_id="wamid.STOPLATE")

    await _ingest(
        session_maker,
        _payload(_message(text="START", msg_id="wamid.STARTOLD", epoch=BASE_EPOCH)),
        monkeypatch=monkeypatch,
    )
    for event_id in await _event_ids(session_maker):
        await _process(session_maker, CaptureProvider(), event_id)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    assert row.evidence.get("last_refused_start_reason") == REFUSED_START_NOT_NEWER


async def test_an_equal_timestamp_is_not_newer(session_maker, monkeypatch) -> None:
    """§46.4.7 — an unproven order must not grant, and a tie is unproven."""
    await _deliver_stop(session_maker, monkeypatch, epoch=BASE_EPOCH)

    await _ingest(
        session_maker,
        _payload(_message(text="START", msg_id="wamid.STARTTIE", epoch=BASE_EPOCH)),
        monkeypatch=monkeypatch,
    )
    for event_id in await _event_ids(session_maker):
        await _process(session_maker, CaptureProvider(), event_id)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    assert row.evidence.get("last_refused_start_reason") == REFUSED_START_NOT_NEWER


async def test_a_start_with_an_unparseable_timestamp_refuses(session_maker, monkeypatch) -> None:
    await _deliver_stop(session_maker, monkeypatch, epoch=BASE_EPOCH)

    await _seed_sender(session_maker)
    body_message = _message(text="START", msg_id="wamid.STARTBAD", epoch=BASE_EPOCH + 60)
    body_message["timestamp"] = "not-a-number"
    await _ingest(session_maker, _payload(body_message), monkeypatch=monkeypatch)
    for event_id in await _event_ids(session_maker):
        await _process(session_maker, CaptureProvider(), event_id)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT


async def test_a_start_reusing_another_addresss_message_id_refuses(session_maker, monkeypatch) -> None:
    """§46.4.7 — a contradictory identifier must not be able to lift a refusal."""
    shared_id = "wamid.SHARED"
    await _seed_sender(session_maker)
    # Somebody else's message, under the identifier the START will claim.
    await _ingest(
        session_maker,
        _payload(_message(text="hallo", sender=OTHER_RAW_FROM, msg_id=shared_id, epoch=BASE_EPOCH)),
        monkeypatch=monkeypatch,
    )
    await _ingest(
        session_maker,
        _payload(_message(text="STOP", msg_id="wamid.STOPX", epoch=BASE_EPOCH + 10)),
        monkeypatch=monkeypatch,
    )
    await _ingest(
        session_maker,
        _payload(_message(text="START", msg_id=shared_id, epoch=BASE_EPOCH + 600)),
        monkeypatch=monkeypatch,
    )
    for event_id in await _event_ids(session_maker):
        await _process(session_maker, CaptureProvider(), event_id)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    assert row.evidence.get("last_refused_start_reason") == REFUSED_START_IDENTIFIER_CONFLICT
    refused = [r for r in await _receipts(session_maker) if r.resolution == INBOUND_RESOLUTION_REFUSED]
    assert refused and refused[-1].resolution_reason == REFUSED_START_IDENTIFIER_CONFLICT


async def test_a_start_with_no_refusal_creates_no_permission_record(session_maker, monkeypatch) -> None:
    """The absence of a ban is not consent, and must not be written down as one."""
    await _seed_sender(session_maker)
    await _ingest(
        session_maker,
        _payload(_message(text="START", msg_id="wamid.STARTONLY", epoch=BASE_EPOCH)),
        monkeypatch=monkeypatch,
    )
    for event_id in await _event_ids(session_maker):
        await _process(session_maker, CaptureProvider(), event_id)

    assert await _suppression(session_maker) is None


async def test_a_lifted_refusal_does_not_revive_cancelled_jobs(session_maker, monkeypatch) -> None:
    """§46.4.7 — an opt-in is not a resume."""
    from altegio_bot.models.models import MessageJob, Record

    async with session_maker() as session:
        async with session.begin():
            client = Client(
                provider="altegio",
                company_id=1,
                altegio_client_id=990030,
                phone_e164=ADDRESS,
                display_name="Jobs",
                raw={},
            )
            session.add(client)
            await session.flush()
            record = Record(company_id=1, altegio_record_id=990030, client_id=client.id, raw={})
            session.add(record)
            await session.flush()
            session.add(
                MessageJob(
                    company_id=1,
                    record_id=record.id,
                    client_id=client.id,
                    job_type="review_3d",
                    run_at=datetime.now(timezone.utc),
                    status="queued",
                    dedupe_key="consent-job-1",
                    payload={},
                )
            )

    await _deliver_stop(session_maker, monkeypatch, epoch=BASE_EPOCH)
    async with session_maker() as session:
        assert (await session.scalars(select(MessageJob.status))).all() == ["canceled"]

    await _ingest(
        session_maker,
        _payload(_message(text="START", msg_id="wamid.STARTJOBS", epoch=BASE_EPOCH + 60)),
        monkeypatch=monkeypatch,
    )
    for event_id in await _event_ids(session_maker):
        await _process(session_maker, CaptureProvider(), event_id)

    async with session_maker() as session:
        assert (await session.scalars(select(MessageJob.status))).all() == ["canceled"]


# ===========================================================================
# Task 3 — the pending-inbound boundary and the final send guard
# ===========================================================================


async def test_the_ingress_makes_the_refusal_pending_before_any_worker_runs(session_maker, monkeypatch) -> None:
    """The durable boundary of §46.4.6, proven without the worker at all."""
    await _ingest(session_maker, _payload(_message(text="СТОП", msg_id="wamid.PEND")), monkeypatch=monkeypatch)

    receipts = await _receipts(session_maker)
    assert len(receipts) == 1
    assert receipts[0].command == CONSENT_COMMAND_STOP
    assert receipts[0].authenticity == INBOUND_AUTHENTICITY_SIGNED
    assert receipts[0].resolved_at is None

    async with session_maker() as session:
        guard = await final_send_guard(session, phone=ADDRESS)
    assert guard.allowed is False
    assert guard.reason == GUARD_PENDING_INBOUND


async def test_an_unprocessed_ordinary_message_also_holds_the_send(session_maker, monkeypatch) -> None:
    """A queue that is behind must not read as permission.

    The message may BE the refusal; nothing has looked at it yet. Holding is the
    only honest answer, and it is a hold rather than a ban: once the worker
    resolves it, the send is allowed again.
    """
    await _seed_sender(session_maker)
    await _ingest(session_maker, _payload(_message(text="hallo", msg_id="wamid.ORD")), monkeypatch=monkeypatch)

    async with session_maker() as session:
        held = await final_send_guard(session, phone=ADDRESS)
    assert held.allowed is False
    assert held.reason == GUARD_PENDING_INBOUND

    for event_id in await _event_ids(session_maker):
        await _process(session_maker, CaptureProvider(), event_id)

    async with session_maker() as session:
        released = await final_send_guard(session, phone=ADDRESS)
    assert released.allowed is True
    assert released.reason == GUARD_ALLOWED


async def test_an_event_from_a_disallowed_sender_route_does_not_hold_forever(session_maker, monkeypatch) -> None:
    """Nothing will ever process it, so a permanent hold would be a bug."""
    monkeypatch.setattr(settings, "whatsapp_allowed_phone_number_ids", "PNID-ONLY-THIS")
    await _ingest(
        session_maker,
        _payload(_message(text="hallo", msg_id="wamid.IGN"), phone_number_id="PNID-SOMETHING-ELSE"),
        monkeypatch=monkeypatch,
    )

    receipts = await _receipts(session_maker)
    assert len(receipts) == 1
    assert receipts[0].resolution == INBOUND_RESOLUTION_IGNORED_ROUTE
    assert receipts[0].resolved_at is not None

    async with session_maker() as session:
        guard = await final_send_guard(session, phone=ADDRESS)
    assert guard.allowed is True


async def test_the_guard_refuses_a_recorded_refusal(session_maker, monkeypatch) -> None:
    await _deliver_stop(session_maker, monkeypatch)

    async with session_maker() as session:
        guard = await final_send_guard(session, phone=ADDRESS)
    assert guard.allowed is False
    assert guard.reason == GUARD_SUPPRESSED


async def test_a_refusal_landing_after_a_guard_invalidates_its_fingerprint(session_maker, monkeypatch) -> None:
    """The claim-time re-check of §46.4.6, which is what makes the order single.

    The guard is taken while nothing is recorded, a STOP is then made durable,
    and the fingerprint the guard handed out no longer matches — so a sender that
    re-checks it at its claim refuses, and no request is made.
    """
    await _seed_sender(session_maker)
    async with session_maker() as session:
        guard = await final_send_guard(session, phone=ADDRESS)
    assert guard.allowed is True

    provider = await _deliver_stop(session_maker, monkeypatch)
    assert provider.sent and "abgemeldet" in provider.sent[0][2].lower()

    async with session_maker() as session:
        assert await consent_unchanged(session, phone=ADDRESS, expected_fingerprint=guard.fingerprint) is False


async def test_a_failing_acknowledgement_does_not_undo_the_refusal(session_maker, monkeypatch) -> None:
    """§46.4.7 — the refusal is committed before the ack is even attempted."""

    class FailingProvider(WhatsAppProvider):
        async def send(self, sender_id: int, phone_e164: str, text: str, contact_name: str | None = None) -> str:
            raise RuntimeError("meta is unreachable")

    await _seed_sender(session_maker)
    await _ingest(session_maker, _payload(_message(text="STOP", msg_id="wamid.ACKFAIL")), monkeypatch=monkeypatch)
    for event_id in await _event_ids(session_maker):
        await _process(session_maker, FailingProvider(), event_id)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT


async def test_a_handler_failure_does_not_undo_the_refusal(session_maker, monkeypatch) -> None:
    """Anything after the consent write may fail; the refusal still stands."""
    await _seed_sender(session_maker)
    await _ingest(session_maker, _payload(_message(text="стоп", msg_id="wamid.BOOM")), monkeypatch=monkeypatch)

    async def _explode(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("handler exploded")

    monkeypatch.setattr(worker_module, "handle_event", _explode)
    for event_id in await _event_ids(session_maker):
        await _process(session_maker, CaptureProvider(), event_id)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    async with session_maker() as session:
        statuses = (await session.scalars(select(WhatsAppEvent.status))).all()
    assert list(statuses) == ["failed"]


async def test_a_restart_before_processing_keeps_the_hold_and_then_applies_it(session_maker, monkeypatch) -> None:
    """Receipts are on disk, so a restart resumes rather than loses the refusal."""
    await _seed_sender(session_maker)
    await _ingest(session_maker, _payload(_message(text="STOP", msg_id="wamid.RESTART")), monkeypatch=monkeypatch)

    async with session_maker() as session:
        assert (await final_send_guard(session, phone=ADDRESS)).allowed is False

    # "Restart": nothing in memory, the event is simply processed later.
    for event_id in await _event_ids(session_maker):
        await apply_inbound_consent(session_maker, event_id=event_id)

    async with session_maker() as session:
        guard = await final_send_guard(session, phone=ADDRESS)
    assert guard.allowed is False
    assert guard.reason == GUARD_SUPPRESSED


async def test_two_concurrent_first_refusals_for_one_address_converge(session_maker) -> None:
    """The absent-row race: two writers, one row, no exception.

    Both transactions decide to create the first suppression for an address
    nobody has a row for. Without the address lock one of them loses on the
    unique index; with it they serialise and the second one reads the first's row
    and makes it a no-op.
    """
    import asyncio

    from altegio_bot.whatsapp_consent import ProjectedRefusal, apply_projected_refusal

    moment = datetime.now(timezone.utc)

    async def _write(origin: str) -> str:
        async with session_maker() as session:
            async with session.begin():
                outcome = await apply_projected_refusal(
                    session,
                    ProjectedRefusal(wa_address=ADDRESS, origin=origin, observed_at=moment),
                )
                return outcome.resolution

    first, second = await asyncio.gather(_write("a"), _write("b"))

    assert {first, second} == {"applied", "noop"}
    async with session_maker() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(WhatsAppConsentSuppression)
                .where(WhatsAppConsentSuppression.wa_address == ADDRESS)
            )
        ) == 1


async def test_an_unverifiable_address_is_never_cleared(session_maker) -> None:
    """A number we cannot normalise is a number we cannot clear."""
    async with session_maker() as session:
        guard = await final_send_guard(session, phone="not a phone")
    assert guard.allowed is False


async def test_the_database_refuses_an_opt_in_without_proof(session_maker) -> None:
    """The START rules are a CHECK, not only a code path.

    Written directly, bypassing every function in this phase, because the point
    is that an unsigned or time-unknown opt-in is unrepresentable rather than
    merely unreachable.
    """
    from sqlalchemy.exc import IntegrityError

    moment = datetime.now(timezone.utc)
    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                session.add(
                    WhatsAppConsentSuppression(
                        wa_address=ADDRESS,
                        state=CONSENT_STATE_OPTED_IN,
                        consent_version=1,
                        decision_command=CONSENT_COMMAND_START,
                        decision_source="inbound_command",
                        decision_authenticity=INBOUND_AUTHENTICITY_UNVERIFIED,
                        decided_observed_at=moment,
                        decided_event_at=moment - timedelta(minutes=1),
                    )
                )


def test_the_consent_writer_and_the_worker_agree_about_chatwoot_origin() -> None:
    """One rule, checked in two places, pinned to stay one rule.

    The consent writer cannot import the inbox worker — the worker imports it —
    so the mirror/relay markers are spelled out in both. This test is what keeps
    the duplication from drifting: a marker added to the worker's loop
    prevention and forgotten here would let a mirrored START get a second
    attempt at lifting a refusal.
    """
    from types import SimpleNamespace

    from altegio_bot.whatsapp_consent import _is_chatwoot_sourced

    cases = (
        ("chatwoot:mirror-1", {"_chatwoot": {"source": "mirror"}}),
        ("wa:plain", {"_chatwoot": {}}),
        ("chatwoot:plain", {}),
        ("wa:plain", {}),
        ("wa:relay", {"_chatwoot_operator_relay": {"conversation_id": 1}}),
    )
    for dedupe_key, payload in cases:
        event = SimpleNamespace(dedupe_key=dedupe_key, payload=payload, id=1)
        worker_says = worker_module._is_chatwoot_origin(event, payload) or worker_module._is_operator_relay(payload)
        assert _is_chatwoot_sourced(event, payload) == worker_says, (dedupe_key, payload)


async def test_the_refusal_is_already_durable_when_the_handler_runs(session_maker, monkeypatch) -> None:
    """The ordering §46.4.7 rests on, observed from inside the handler.

    ``process_one_event`` applies consent in a transaction of its own BEFORE the
    event transaction opens. That is what makes an acknowledgement failure, a
    Chatwoot outage or a crash in the handler unable to undo a refusal — so the
    handler is replaced here by one that reads the ledger and records what it
    found. It must find the refusal already committed.

    The negative half is the pass that was failing before this test existed: if
    the consent pass were ever moved inside the event transaction, or skipped,
    this records ``False`` and fails.
    """
    await _seed_sender(session_maker)
    await _ingest(session_maker, _payload(_message(text="СТОП", msg_id="wamid.ORDER")), monkeypatch=monkeypatch)

    seen: list[bool] = []

    async def _observe(session: Any, event: Any, provider: Any) -> None:
        async with session_maker() as reader:
            seen.append(await is_suppressed(reader, phone=ADDRESS))

    monkeypatch.setattr(worker_module, "handle_event", _observe)
    for event_id in await _event_ids(session_maker):
        await _process(session_maker, CaptureProvider(), event_id)

    assert seen == [True]
