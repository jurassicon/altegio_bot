"""Durable WhatsApp consent: recognition, durability and ordering (§46.4, PR-22.1).

Driven through the real ingress and the real worker entry point wherever the
property under test is about the production path. A helper that writes a
suppression row directly would prove the helper works and nothing about whether
a STOP arriving at the webhook actually stops anything.

Nothing here sends: the provider is a capture double, and several tests assert
that the count of outbound messages is exactly zero.
"""

from __future__ import annotations

import asyncio
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
    REFUSED_START_BAN_TIME_UNKNOWN,
    REFUSED_START_IDENTIFIER_CONFLICT,
    REFUSED_START_NOT_NEWER,
    REFUSED_START_UNVERIFIED,
    SOURCE_CLIENT_FLAG,
    SOURCE_RECIPIENT_FLAG,
    SOURCE_SUPPRESSION,
    SOURCE_SUPPRESSION_UUID,
    apply_inbound_consent,
    consent_decision,
    final_send_guard,
    is_suppressed,
    recheck_consent_boundary,
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


async def _process_pending(session_maker, provider: WhatsAppProvider) -> None:
    """Run the worker over the events it would really claim, and no others.

    ``status == "received"`` is the claim condition of ``lock_next_batch``, so
    this is the production selection. Re-running every stored event instead —
    which some older tests here do — would process a delivery twice and send a
    second acknowledgement for it, which is not something the worker does and
    not something a test about acknowledgements can tolerate.
    """
    async with session_maker() as session:
        ids = [
            int(value)
            for value in (
                await session.scalars(
                    select(WhatsAppEvent.id).where(WhatsAppEvent.status == "received").order_by(WhatsAppEvent.id)
                )
            ).all()
        ]
    for event_id in ids:
        await _process(session_maker, provider, event_id)


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
        assert (
            await recheck_consent_boundary(session, phone=ADDRESS, expected_fingerprint=guard.fingerprint)
        ).allowed is False


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


# ===========================================================================
# Review fix 1 — the card flag and the acknowledgement follow the LEDGER
# ===========================================================================


def _mixed_payload(*groups: tuple[str, list[dict[str, Any]]]) -> dict[str, Any]:
    """One delivery of several changes, each with its own sender route.

    Meta really does send these, and the whole payload used to be judged by the
    FIRST change's ``phone_number_id`` — which is the defect these fixtures
    exist to drive.
    """
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
                    for phone_number_id, messages in groups
                ],
            }
        ],
    }


async def _card(session_maker, address: str = ADDRESS, *, opted_out: bool = False) -> int:
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                provider="altegio",
                company_id=1,
                altegio_client_id=991000 + (0 if address == ADDRESS else 1),
                phone_e164=address,
                display_name="Card",
                raw={},
                wa_opted_out=opted_out,
                wa_opted_out_at=datetime.now(timezone.utc) if opted_out else None,
                wa_opt_out_reason="seeded" if opted_out else None,
            )
            session.add(client)
            await session.flush()
            return int(client.id)


async def _client_row(session_maker, client_id: int) -> Client:
    async with session_maker() as session:
        return await session.get(Client, client_id)


def _marketing_verdict(client: Client) -> str | None:
    """What the existing Altegio campaign classifier makes of this card.

    The real function, not a re-implementation: ``compute_excluded_reason`` is
    the single point of truth the campaign pipeline and its debug endpoint both
    use, and it reads exactly the flag this worker writes. Asserting through it
    is what makes "the ledger and the senders agree" a checked statement rather
    than a hopeful one.
    """
    from altegio_bot.campaigns.segment import compute_excluded_reason

    return compute_excluded_reason(
        wa_opted_out=bool(client.wa_opted_out),
        phone_e164=client.phone_e164,
        count_before=0,
        lash_count=1,
        attended_lash_count=1,
    )


async def test_a_stale_start_does_not_clear_the_card_flag(session_maker, monkeypatch) -> None:
    """The reproduction from the review, end to end through the real worker.

    A STOP at 10:30, then a START whose own event time is 10:10. The ledger
    refuses it — it cannot prove it is newer — and the card flag the senders
    actually read must say the same thing. It used to be cleared, because the
    flag was derived from the COMMAND rather than from the decision.
    """
    client_id = await _card(session_maker)
    await _deliver_stop(session_maker, monkeypatch, epoch=BASE_EPOCH + 1800, msg_id="wamid.STOPNEW")

    await _ingest(
        session_maker,
        _payload(_message(text="START", msg_id="wamid.STARTSTALE", epoch=BASE_EPOCH + 600)),
        monkeypatch=monkeypatch,
    )
    provider = CaptureProvider()
    await _process_pending(session_maker, provider)

    row = await _suppression(session_maker)
    assert row is not None and row.state == CONSENT_STATE_OPTED_OUT
    assert row.evidence.get("last_refused_start_reason") == REFUSED_START_NOT_NEWER

    client = await _client_row(session_maker, client_id)
    assert client.wa_opted_out is True
    assert _marketing_verdict(client) == "opted_out"
    async with session_maker() as session:
        assert await is_suppressed(session, phone=ADDRESS)

    # One acknowledgement, for the STOP. The refused START is answered with
    # silence, never with "you are subscribed again".
    assert [text for _sender, _phone, text in provider.sent] == []
    async with session_maker() as session:
        events = list((await session.scalars(select(WhatsAppEvent).order_by(WhatsAppEvent.id))).all())
    assert "not honoured" in str(events[-1].error)
    assert REFUSED_START_NOT_NEWER in str(events[-1].error)


async def test_an_unverified_start_does_not_clear_the_card_flag(session_maker, monkeypatch) -> None:
    """The same divergence on an unsigned deployment, which is most of them."""
    client_id = await _card(session_maker)
    await _deliver_stop(session_maker, monkeypatch, epoch=BASE_EPOCH, signed=False)

    await _ingest(
        session_maker,
        _payload(_message(text="START", msg_id="wamid.STARTUNSIGNED", epoch=BASE_EPOCH + 600)),
        signed=False,
        monkeypatch=monkeypatch,
    )
    provider = CaptureProvider()
    await _process_pending(session_maker, provider)

    row = await _suppression(session_maker)
    assert row is not None and row.state == CONSENT_STATE_OPTED_OUT
    client = await _client_row(session_maker, client_id)
    assert client.wa_opted_out is True
    assert _marketing_verdict(client) == "opted_out"
    assert provider.sent == []


async def test_a_refused_start_behind_an_ordinary_message_is_judged_the_same_way(session_maker, monkeypatch) -> None:
    """The multi-message path reaches the same verdict as the single one.

    ``[hallo, START]`` in one delivery: the START is still a START, it is still
    refused, and the ordinary message must not smuggle it past the proofs.
    """
    client_id = await _card(session_maker)
    await _deliver_stop(session_maker, monkeypatch, epoch=BASE_EPOCH + 1800, msg_id="wamid.STOPFIRST")

    await _ingest(
        session_maker,
        _payload(
            _message(text="hallo", msg_id="wamid.ORD2", epoch=BASE_EPOCH + 1900),
            _message(text="START", msg_id="wamid.STARTBEHIND", epoch=BASE_EPOCH + 600),
        ),
        monkeypatch=monkeypatch,
    )
    await _process_pending(session_maker, CaptureProvider())

    row = await _suppression(session_maker)
    assert row is not None and row.state == CONSENT_STATE_OPTED_OUT
    client = await _client_row(session_maker, client_id)
    assert client.wa_opted_out is True
    assert _marketing_verdict(client) == "opted_out"


async def test_a_proven_newer_start_clears_the_card_and_is_acknowledged(session_maker, monkeypatch) -> None:
    """The positive half: when the ledger lifts the refusal, everything agrees."""
    client_id = await _card(session_maker)
    await _deliver_stop(session_maker, monkeypatch, epoch=BASE_EPOCH, msg_id="wamid.STOPOK")

    assert (await _client_row(session_maker, client_id)).wa_opted_out is True

    await _ingest(
        session_maker,
        _payload(_message(text="START", msg_id="wamid.STARTOK", epoch=BASE_EPOCH + 600)),
        monkeypatch=monkeypatch,
    )
    provider = CaptureProvider()
    await _process_pending(session_maker, provider)

    row = await _suppression(session_maker)
    assert row is not None and row.state == CONSENT_STATE_OPTED_IN
    client = await _client_row(session_maker, client_id)
    assert client.wa_opted_out is False
    assert client.wa_opted_out_at is None
    assert _marketing_verdict(client) is None
    assert any("angemeldet" in text for _sender, _phone, text in provider.sent)


async def test_a_refused_start_does_not_revive_cancelled_jobs(session_maker, monkeypatch) -> None:
    """§46.4.7 — and a command the ledger refused is not even an opt-in."""
    from altegio_bot.models.models import MessageJob, Record

    async with session_maker() as session:
        async with session.begin():
            client = Client(
                provider="altegio",
                company_id=1,
                altegio_client_id=991010,
                phone_e164=ADDRESS,
                display_name="With jobs",
                raw={},
            )
            session.add(client)
            await session.flush()
            record = Record(company_id=1, altegio_record_id=771010, client_id=int(client.id), raw={})
            session.add(record)
            await session.flush()
            session.add(
                MessageJob(
                    company_id=1,
                    record_id=int(record.id),
                    client_id=int(client.id),
                    job_type="review_3d",
                    status="queued",
                    run_at=datetime.now(timezone.utc) + timedelta(days=3),
                    dedupe_key="review_3d:771010",
                    payload={},
                )
            )

    await _deliver_stop(session_maker, monkeypatch, epoch=BASE_EPOCH + 1800, msg_id="wamid.STOPJOBS")
    async with session_maker() as session:
        assert (await session.scalars(select(MessageJob.status))).all() == ["canceled"]

    await _ingest(
        session_maker,
        _payload(_message(text="START", msg_id="wamid.STARTJOBS", epoch=BASE_EPOCH + 600)),
        monkeypatch=monkeypatch,
    )
    await _process_pending(session_maker, CaptureProvider())

    async with session_maker() as session:
        assert (await session.scalars(select(MessageJob.status))).all() == ["canceled"]


# ===========================================================================
# Review fix 2A — a refusal whose order cannot be proven keeps its priority
# ===========================================================================


async def test_a_newer_stop_with_an_unknown_time_outranks_an_older_proven_one(session_maker, monkeypatch) -> None:
    """The reproduction: a provable timestamp must not beat an unprovable STOP.

    A STOP at 10:00 is provable. A second STOP arrives later with no usable
    timestamp at all, so nothing can be ordered against it. A START then shows
    up claiming 10:10 — newer than the FIRST refusal, and silent about the
    second. It used to win, because the high-water mark only ever moved to a
    known time and the unknown one left no trace.
    """
    await _deliver_stop(session_maker, monkeypatch, epoch=BASE_EPOCH, msg_id="wamid.STOPKNOWN")

    unknown = _message(text="STOP", msg_id="wamid.STOPUNKNOWN", epoch=BASE_EPOCH + 1800)
    unknown["timestamp"] = "not-a-number"
    await _ingest(session_maker, _payload(unknown), monkeypatch=monkeypatch)
    await _ingest(
        session_maker,
        _payload(_message(text="START", msg_id="wamid.STARTBETWEEN", epoch=BASE_EPOCH + 600)),
        monkeypatch=monkeypatch,
    )
    await _process_pending(session_maker, CaptureProvider())

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    assert row.stop_event_time_unknown_at is not None
    assert row.evidence.get("last_refused_start_reason") == REFUSED_START_BAN_TIME_UNKNOWN


async def test_the_order_of_those_three_messages_does_not_change_the_answer(session_maker, monkeypatch) -> None:
    """Every permutation, and the duplicates, reach the same refusal.

    Processing order is not something this project controls — Meta redelivers,
    the worker retries, a recovery pass re-reads — so the answer has to be the
    same whichever way round they arrive.
    """
    unknown = _message(text="STOP", msg_id="wamid.P-UNKNOWN", epoch=BASE_EPOCH + 1800)
    unknown["timestamp"] = "not-a-number"
    deliveries = [
        _payload(_message(text="START", msg_id="wamid.P-START", epoch=BASE_EPOCH + 600)),
        _payload(unknown),
        _payload(_message(text="STOP", msg_id="wamid.P-KNOWN", epoch=BASE_EPOCH)),
        # And again, in the other order, to prove a duplicate changes nothing.
        _payload(unknown),
        _payload(_message(text="START", msg_id="wamid.P-START2", epoch=BASE_EPOCH + 600)),
    ]
    await _seed_sender(session_maker)
    for payload in deliveries:
        await _ingest(session_maker, payload, monkeypatch=monkeypatch)
        await _process_pending(session_maker, CaptureProvider())

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    assert row.stop_event_time_unknown_at is not None


async def test_a_replayed_old_stop_does_not_undo_a_proven_opt_in(session_maker, monkeypatch) -> None:
    """STOP T1, a proven START T2, then T1 delivered again. T2 stands.

    The same payload arriving twice is ordinary — Meta retries a delivery it did
    not hear a 200 for — and the second copy must not walk the state back. The
    ledger keeps the opt-in and records that a superseded STOP was seen.
    """
    from altegio_bot.whatsapp_consent import NOOP_STOP_SUPERSEDED

    client_id = await _card(session_maker)
    stop = _message(text="STOP", msg_id="wamid.REPLAY-STOP", epoch=BASE_EPOCH)
    await _seed_sender(session_maker)
    await _ingest(session_maker, _payload(stop), monkeypatch=monkeypatch)
    await _process_pending(session_maker, CaptureProvider())

    await _ingest(
        session_maker,
        _payload(_message(text="START", msg_id="wamid.REPLAY-START", epoch=BASE_EPOCH + 600)),
        monkeypatch=monkeypatch,
    )
    await _process_pending(session_maker, CaptureProvider())
    assert (await _suppression(session_maker)).state == CONSENT_STATE_OPTED_IN

    # The same STOP again, under a different message id so it is a new delivery
    # of the same old event rather than a duplicate payload the ingress dedupes.
    await _ingest(
        session_maker,
        _payload(_message(text="STOP", msg_id="wamid.REPLAY-STOP-AGAIN", epoch=BASE_EPOCH)),
        monkeypatch=monkeypatch,
    )
    await _process_pending(session_maker, CaptureProvider())

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_IN
    assert row.evidence.get("superseded_stop_event_at") is not None
    receipts = [r for r in await _receipts(session_maker) if r.command == CONSENT_COMMAND_STOP]
    assert receipts[-1].resolution_reason == NOOP_STOP_SUPERSEDED
    client = await _client_row(session_maker, client_id)
    assert client.wa_opted_out is False, "the card must not be re-banned by a superseded replay"


async def test_a_genuinely_newer_stop_after_an_opt_in_still_applies(session_maker, monkeypatch) -> None:
    """The guard above is narrow on purpose: only a PROVABLY older STOP noops."""
    await _deliver_stop(session_maker, monkeypatch, epoch=BASE_EPOCH, msg_id="wamid.SEQ-STOP")
    await _ingest(
        session_maker,
        _payload(_message(text="START", msg_id="wamid.SEQ-START", epoch=BASE_EPOCH + 600)),
        monkeypatch=monkeypatch,
    )
    await _process_pending(session_maker, CaptureProvider())
    assert (await _suppression(session_maker)).state == CONSENT_STATE_OPTED_IN

    await _ingest(
        session_maker,
        _payload(_message(text="STOP", msg_id="wamid.SEQ-STOP2", epoch=BASE_EPOCH + 1200)),
        monkeypatch=monkeypatch,
    )
    await _process_pending(session_maker, CaptureProvider())

    row = await _suppression(session_maker)
    assert row.state == CONSENT_STATE_OPTED_OUT


async def test_a_stop_at_the_same_second_as_the_opt_in_still_applies(session_maker, monkeypatch) -> None:
    """A tie is not an order, and an unproven order falls to the refusal."""
    await _deliver_stop(session_maker, monkeypatch, epoch=BASE_EPOCH, msg_id="wamid.TIE-STOP")
    await _ingest(
        session_maker,
        _payload(_message(text="START", msg_id="wamid.TIE-START", epoch=BASE_EPOCH + 600)),
        monkeypatch=monkeypatch,
    )
    await _process_pending(session_maker, CaptureProvider())
    assert (await _suppression(session_maker)).state == CONSENT_STATE_OPTED_IN

    await _ingest(
        session_maker,
        _payload(_message(text="STOP", msg_id="wamid.TIE-STOP2", epoch=BASE_EPOCH + 600)),
        monkeypatch=monkeypatch,
    )
    await _process_pending(session_maker, CaptureProvider())

    assert (await _suppression(session_maker)).state == CONSENT_STATE_OPTED_OUT


# ===========================================================================
# Review fix 5 — the claim-time boundary, not just a fingerprint comparison
# ===========================================================================


async def test_a_pending_stop_between_the_guard_and_the_claim_refuses_the_claim(session_maker, monkeypatch) -> None:
    """The reproduction: the old re-check compared equal while a STOP waited.

    The guard is taken while nothing is recorded. A STOP then arrives at the
    ingress and is durable, but the worker has not applied it yet — which is
    exactly the state a fresh guard refuses with ``pending_inbound_unresolved``.
    The claim-time re-check has to refuse it too, and the fingerprint alone did
    not: it was computed over suppression rows, and no suppression row exists
    yet.
    """
    await _seed_sender(session_maker)
    async with session_maker() as session:
        guard = await final_send_guard(session, phone=ADDRESS)
    assert guard.allowed is True

    await _ingest(session_maker, _payload(_message(text="STOP", msg_id="wamid.PEND-CLAIM")), monkeypatch=monkeypatch)

    async with session_maker() as session:
        recheck = await recheck_consent_boundary(session, phone=ADDRESS, expected_fingerprint=guard.fingerprint)
    assert recheck.allowed is False
    assert recheck.reason == GUARD_PENDING_INBOUND
    # And the two answers agree, which is the property that was broken.
    async with session_maker() as session:
        assert (await final_send_guard(session, phone=ADDRESS)).reason == GUARD_PENDING_INBOUND


async def test_an_unresolved_ordinary_message_also_refuses_the_claim(session_maker, monkeypatch) -> None:
    """It may BE the refusal. Nothing has read it yet, so nothing may send."""
    await _seed_sender(session_maker)
    async with session_maker() as session:
        guard = await final_send_guard(session, phone=ADDRESS)

    await _ingest(session_maker, _payload(_message(text="hallo", msg_id="wamid.ORD-CLAIM")), monkeypatch=monkeypatch)

    async with session_maker() as session:
        held = await recheck_consent_boundary(session, phone=ADDRESS, expected_fingerprint=guard.fingerprint)
    assert held.allowed is False
    assert held.reason == GUARD_PENDING_INBOUND

    await _process_pending(session_maker, CaptureProvider())

    async with session_maker() as session:
        # Resolved, and it turned out not to be a refusal: the hold lifts and
        # the claim is allowed again. The fingerprint is a digest of the STATE,
        # not a history of it, so a message that changed nothing leaves it
        # unchanged — which is the honest answer, since a fresh guard says the
        # same thing.
        released = await recheck_consent_boundary(session, phone=ADDRESS, expected_fingerprint=guard.fingerprint)
        fresh = await final_send_guard(session, phone=ADDRESS)
    assert released.allowed is True
    assert fresh.allowed is True


async def test_a_receipt_cannot_commit_while_the_guard_holds_the_address(session_maker, monkeypatch) -> None:
    """The ingress and the guard serialise on the same address lock.

    This is the half a hash cannot provide. The guard holds its transaction
    open; a webhook delivery for the same address then tries to write its
    receipt and BLOCKS — asserted, because an implementation that only hashed
    the pending set would let the receipt land unordered and the claim would
    then be racing it. Once the guard's transaction ends the receipt commits,
    and the re-check sees it.
    """
    await _seed_sender(session_maker)
    guard_taken = asyncio.Event()
    release_guard = asyncio.Event()
    blocked: list[bool] = []
    fingerprints: list[str] = []

    async def _guard() -> None:
        async with session_maker() as session:
            async with session.begin():
                verdict = await final_send_guard(session, phone=ADDRESS)
                assert verdict.allowed is True
                fingerprints.append(verdict.fingerprint)
                guard_taken.set()
                await release_guard.wait()

    async def _ingress() -> None:
        await guard_taken.wait()
        task = asyncio.create_task(
            _ingest(
                session_maker,
                _payload(_message(text="СТОП", msg_id="wamid.RACE-RECEIPT")),
                monkeypatch=monkeypatch,
            )
        )
        done, _pending = await asyncio.wait({task}, timeout=1.0)
        blocked.append(not done)
        release_guard.set()
        await task

    await asyncio.gather(_guard(), _ingress())

    assert blocked == [True]
    receipts = await _receipts(session_maker)
    assert len(receipts) == 1 and receipts[0].resolved_at is None
    async with session_maker() as session:
        recheck = await recheck_consent_boundary(session, phone=ADDRESS, expected_fingerprint=fingerprints[0])
    assert recheck.allowed is False
    assert recheck.reason == GUARD_PENDING_INBOUND


async def test_a_refusal_on_another_number_of_the_same_identity_refuses_the_claim(session_maker, monkeypatch) -> None:
    """§46.4.2 at the send boundary, including the lock it is taken under."""
    customer = uuid_module.uuid4()
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=322579,
                    altegio_client_id=991020,
                    easyweek_customer_uuid=customer,
                    phone_e164=ADDRESS,
                    display_name="Before",
                    raw={},
                )
            )

    await _deliver_stop(session_maker, monkeypatch)
    async with session_maker() as session:
        async with session.begin():
            card = (await session.execute(select(Client).where(Client.easyweek_customer_uuid == customer))).scalar_one()
            card.phone_e164 = OTHER_ADDRESS

    async with session_maker() as session:
        guard = await final_send_guard(session, phone=OTHER_ADDRESS, easyweek_customer_uuid=customer)
    assert guard.allowed is False
    assert guard.reason == GUARD_SUPPRESSED
    # Found through the UUID bound to the refusal, not through the address set:
    # no card carries the old number any more, so nothing links it to this
    # person except the refusal itself. That is also the boundary of the lock —
    # an address nothing can name cannot be locked in advance, and a refusal
    # landing there is caught by the claim-time fingerprint instead.
    assert SOURCE_SUPPRESSION_UUID in guard.decision.sources


async def test_the_guard_locks_every_address_it_reads(session_maker, monkeypatch) -> None:
    """A STOP for a UUID-linked number cannot land while the guard is open."""
    customer = uuid_module.uuid4()
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=322579,
                    altegio_client_id=991021,
                    easyweek_customer_uuid=customer,
                    phone_e164=OTHER_ADDRESS,
                    display_name="Linked",
                    raw={},
                )
            )
    await _seed_sender(session_maker)
    guard_taken = asyncio.Event()
    release_guard = asyncio.Event()
    blocked: list[bool] = []

    async def _guard() -> None:
        async with session_maker() as session:
            async with session.begin():
                verdict = await final_send_guard(session, phone=ADDRESS, easyweek_customer_uuid=customer)
                assert verdict.allowed is True
                assert OTHER_ADDRESS in verdict.decision.matched_addresses
                guard_taken.set()
                await release_guard.wait()

    async def _ingress() -> None:
        await guard_taken.wait()
        task = asyncio.create_task(
            _ingest(
                session_maker,
                _payload(_message(text="STOP", sender=OTHER_RAW_FROM, msg_id="wamid.RACE-LINKED")),
                monkeypatch=monkeypatch,
            )
        )
        done, _pending = await asyncio.wait({task}, timeout=1.0)
        blocked.append(not done)
        release_guard.set()
        await task

    await asyncio.gather(_guard(), _ingress())
    assert blocked == [True]


# ===========================================================================
# Review fix 7 — one verdict per message, recorded where it was reached
# ===========================================================================


async def test_a_refused_sender_behind_an_allowed_one_cannot_lift_a_refusal(session_maker, monkeypatch) -> None:
    """allowed → disallowed in one delivery. The second START is not ours.

    The payload-level verdict used to accept the whole delivery because its
    FIRST change named an allowed route, so a START arriving on a route this
    deployment does not serve got a free attempt at somebody's refusal.
    """
    await _deliver_stop(session_maker, monkeypatch, epoch=BASE_EPOCH, msg_id="wamid.MIX-STOP")

    await _ingest(
        session_maker,
        _mixed_payload(
            (PNID, [_message(text="hallo", msg_id="wamid.MIX-ORD", epoch=BASE_EPOCH + 100)]),
            (
                "PNID-FOREIGN",
                [_message(text="START", msg_id="wamid.MIX-START", epoch=BASE_EPOCH + 600)],
            ),
        ),
        monkeypatch=monkeypatch,
    )
    await _process_pending(session_maker, CaptureProvider())

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    receipts = [r for r in await _receipts(session_maker) if r.whatsapp_message_id == "wamid.MIX-START"]
    assert receipts and receipts[0].route_allowed is False
    assert receipts[0].resolution == INBOUND_RESOLUTION_IGNORED_ROUTE
    assert receipts[0].resolved_at is not None


async def test_a_refusal_from_an_allowed_sender_behind_a_refused_one_still_counts(session_maker, monkeypatch) -> None:
    """disallowed → allowed. The STOP in the second change is a real refusal.

    This delivery used to be ignored whole, on the strength of its first
    change's route, and the refusal in it was recorded nowhere at all.
    """
    await _seed_sender(session_maker)
    response = await _ingest(
        session_maker,
        _mixed_payload(
            ("PNID-FOREIGN", [_message(text="hallo", sender=OTHER_RAW_FROM, msg_id="wamid.MIX2-ORD")]),
            (PNID, [_message(text="СТОП", msg_id="wamid.MIX2-STOP", epoch=BASE_EPOCH)]),
        ),
        monkeypatch=monkeypatch,
    )
    assert response.json()["ignored"] is False

    await _process_pending(session_maker, CaptureProvider())

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    # And the refused change's own message was neither acted on nor left
    # holding a send forever.
    foreign = [r for r in await _receipts(session_maker) if r.wa_address == OTHER_ADDRESS]
    assert foreign and foreign[0].route_allowed is False and foreign[0].resolved_at is not None
    async with session_maker() as session:
        assert (await final_send_guard(session, phone=OTHER_ADDRESS)).allowed is True


async def test_a_refused_stop_is_not_executed_and_a_refused_start_is_not_either(session_maker, monkeypatch) -> None:
    """Both commands, in both positions, judged by their own route."""
    await _seed_sender(session_maker)
    await _ingest(
        session_maker,
        _mixed_payload(
            ("PNID-FOREIGN", [_message(text="STOP", msg_id="wamid.MIX3-STOP", epoch=BASE_EPOCH)]),
            (PNID, [_message(text="hallo", msg_id="wamid.MIX3-ORD", epoch=BASE_EPOCH + 10)]),
        ),
        monkeypatch=monkeypatch,
    )
    await _process_pending(session_maker, CaptureProvider())
    assert await _suppression(session_maker) is None, "a refused route must not record a refusal"

    await _ingest(
        session_maker,
        _payload(_message(text="STOP", msg_id="wamid.MIX3-REAL", epoch=BASE_EPOCH + 20)),
        monkeypatch=monkeypatch,
    )
    await _process_pending(session_maker, CaptureProvider())
    assert (await _suppression(session_maker)).state == CONSENT_STATE_OPTED_OUT

    await _ingest(
        session_maker,
        _mixed_payload(
            ("PNID-FOREIGN", [_message(text="START", msg_id="wamid.MIX3-START", epoch=BASE_EPOCH + 600)]),
        ),
        monkeypatch=monkeypatch,
    )
    await _process_pending(session_maker, CaptureProvider())
    assert (await _suppression(session_maker)).state == CONSENT_STATE_OPTED_OUT


async def test_a_delivery_with_no_allowed_route_at_all_is_still_ignored(session_maker, monkeypatch) -> None:
    """The historical verdict is unchanged when every route is refused."""
    await _seed_sender(session_maker)
    response = await _ingest(
        session_maker,
        _mixed_payload(
            ("PNID-FOREIGN", [_message(text="STOP", msg_id="wamid.MIX4-A")]),
            ("PNID-FOREIGN-2", [_message(text="STOP", sender=OTHER_RAW_FROM, msg_id="wamid.MIX4-B")]),
        ),
        monkeypatch=monkeypatch,
    )
    assert response.json()["ignored"] is True

    async with session_maker() as session:
        event = (await session.scalars(select(WhatsAppEvent).order_by(WhatsAppEvent.id.desc()))).first()
    assert event.status == "ignored"
    receipts = await _receipts(session_maker)
    assert len(receipts) == 2
    assert all(r.route_allowed is False and r.resolved_at is not None for r in receipts)
    assert await _suppression(session_maker) is None
    async with session_maker() as session:
        assert (await final_send_guard(session, phone=ADDRESS)).allowed is True


async def test_the_receipt_records_which_route_the_message_arrived_on(session_maker, monkeypatch) -> None:
    """Provenance, as a digest, so the reconciliation can honour it later."""
    from altegio_bot.whatsapp_consent import sender_route_digest

    await _seed_sender(session_maker)
    await _ingest(session_maker, _payload(_message(text="STOP", msg_id="wamid.PROV")), monkeypatch=monkeypatch)

    receipts = await _receipts(session_maker)
    assert len(receipts) == 1
    assert receipts[0].route_allowed is True
    assert receipts[0].sender_route_digest == sender_route_digest(PNID)
    # A digest, never the identifier itself.
    assert PNID not in str(receipts[0].sender_route_digest)


async def test_the_live_worker_and_the_reconciliation_agree_about_a_refused_route(session_maker, monkeypatch) -> None:
    """One verdict, two readers. The projection adds nothing the worker refused.

    The delivery goes through the real ingress and the real worker, so the
    receipt carries the verdict that was actually reached; the historical
    reconciliation then runs over the same stored event with ``--apply`` and
    must reach the same answer. If it re-decided against today's settings this
    would be the test that fails, which is the point: the two code paths have
    different inputs available and only one recorded decision between them.
    """
    from altegio_bot.whatsapp_consent_projection import project

    await _seed_sender(session_maker)
    await _ingest(
        session_maker,
        _mixed_payload(
            ("PNID-FOREIGN", [_message(text="STOP", sender=OTHER_RAW_FROM, msg_id="wamid.AGREE-FOREIGN")]),
            (PNID, [_message(text="hallo", msg_id="wamid.AGREE-OK", epoch=BASE_EPOCH)]),
        ),
        monkeypatch=monkeypatch,
    )
    await _process_pending(session_maker, CaptureProvider())

    live = await _suppression(session_maker, OTHER_ADDRESS)
    assert live is None, "the worker must not act on a refused route"

    report = await project(session_maker, apply=True)

    assert report.messages_route_not_allowed >= 1
    assert await _suppression(session_maker, OTHER_ADDRESS) is None
    assert await _suppression(session_maker, ADDRESS) is None


def test_a_hostile_sender_route_still_produces_a_digest() -> None:
    """A value from a webhook body must never be able to raise in the ingress.

    This is a regression test, and the failure it pins was real: the receipt's
    route digest is computed inside the transaction that answers Meta, from a
    ``phone_number_id`` the sender controls. A lone surrogate in it raised
    ``UnicodeEncodeError`` there — a 500 on a payload that would fail exactly
    the same way on every retry, which is how an un-encodable field becomes a
    retry loop rather than an ignored event.
    """
    from altegio_bot.whatsapp_consent import receipt_key, recipient_route_digest, sender_route_digest

    hostile = "PNID-\ud800"
    digest = sender_route_digest(hostile)
    assert isinstance(digest, str) and len(digest) == 64
    assert digest != sender_route_digest("PNID-")
    # And the other two digests over sender-controlled values.
    assert recipient_route_digest(f"+4915177991001{chr(0xD800)}") is None, "an unnormalisable number has no handle"
    assert len(receipt_key("wa:\ud800", 0)) == 64
