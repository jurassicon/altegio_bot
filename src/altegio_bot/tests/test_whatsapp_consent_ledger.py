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
from sqlalchemy import update as sa_update

import altegio_bot.webhooks.whatsapp as wa_module
from altegio_bot.main import app
from altegio_bot.models.models import (
    CONSENT_COMMAND_NONE,
    CONSENT_COMMAND_START,
    CONSENT_COMMAND_STOP,
    CONSENT_SOURCE_INBOUND,
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
from altegio_bot.utils import utcnow
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

    from altegio_bot.whatsapp_consent import is_chatwoot_sourced

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
        assert is_chatwoot_sourced(event, payload) == worker_says, (dedupe_key, payload)


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

    async def _observe(session: Any, event: Any, provider: Any, **_kwargs: Any) -> None:
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
    # Reached through the refusal's own proven UUID link, and reported as such:
    # no card carries the old number any more, so nothing else connects it to
    # this person. Review fix 3 also brought it INTO the locked set — a refusal
    # bound to a proven UUID is itself a durable negative link, so the address it
    # names can be locked in advance after all, and the serialisation covers it
    # rather than leaving it to the claim-time fingerprint alone.
    assert SOURCE_SUPPRESSION_UUID in guard.decision.sources
    assert ADDRESS in guard.decision.matched_addresses


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


# ===========================================================================
# Round 2, review fix 1 — a rejected START leaves no usable mark
# ===========================================================================


async def _deliver(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    provider: WhatsAppProvider,
    *messages: dict[str, Any],
    signed: bool = True,
    phone_number_id: str = PNID,
) -> None:
    """Ingest one delivery and let the worker claim it, exactly as production does."""
    await _seed_sender(session_maker)
    await _ingest(
        session_maker,
        _payload(*messages, phone_number_id=phone_number_id),
        signed=signed,
        monkeypatch=monkeypatch,
    )
    await _process_pending(session_maker, provider)


async def _deliver_mixed(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    provider: WhatsAppProvider,
    *groups: tuple[str, list[dict[str, Any]]],
) -> None:
    await _seed_sender(session_maker)
    await _ingest(session_maker, _mixed_payload(*groups), monkeypatch=monkeypatch)
    await _process_pending(session_maker, provider)


async def _no_chatwoot(*_args: Any, **_kwargs: Any) -> None:
    return None


async def test_a_rejected_start_cannot_license_ignoring_a_later_stop(session_maker, monkeypatch) -> None:
    """The review's sequence, end to end, with every message really delivered.

    STOP 10:00, an UNSIGNED START 11:00 that is refused, the signed START 10:30
    that is honoured, then the signed STOP 10:45 — which must apply.

    It did not. The refusal's "am I a replay?" test compared the new STOP
    against ``last_start_event_at``, and that mark moved for every START before
    any of the proofs ran, so the rejected 11:00 attempt raised the bar to 11:00
    and a genuine refusal 15 minutes after the real opt-in was filed away as a
    replay of something that never happened. The send guard then allowed the
    send.
    """
    client_id = await _card(session_maker)
    provider = CaptureProvider()

    await _deliver(session_maker, monkeypatch, provider, _message(text="STOP", msg_id="wamid.R1S", epoch=BASE_EPOCH))
    await _deliver(
        session_maker,
        monkeypatch,
        provider,
        _message(text="START", msg_id="wamid.R1UNSIGNED", epoch=BASE_EPOCH + 3600),
        signed=False,
    )

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    # The rejected attempt is recorded as audit and nowhere else.
    assert row.last_start_event_at is None
    assert row.evidence.get("last_refused_start_reason") == REFUSED_START_UNVERIFIED
    assert row.evidence.get("last_refused_start_event_at") is not None

    await _deliver(
        session_maker, monkeypatch, provider, _message(text="START", msg_id="wamid.R1A", epoch=BASE_EPOCH + 1800)
    )
    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_IN

    await _deliver(
        session_maker, monkeypatch, provider, _message(text="STOP", msg_id="wamid.R1S2", epoch=BASE_EPOCH + 2700)
    )

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    card = await _client_row(session_maker, client_id)
    assert card.wa_opted_out is True
    assert _marketing_verdict(card) == "opted_out"
    async with session_maker() as session:
        guard = await final_send_guard(session, phone=ADDRESS)
    assert guard.allowed is False
    assert guard.reason == GUARD_SUPPRESSED


async def test_a_start_rejected_for_a_reused_identifier_also_raises_nothing(session_maker, monkeypatch) -> None:
    """The same property through a different refusal, so it is the RULE that holds.

    A reused Meta identifier is refused on authenticity grounds rather than on
    time, and its own timestamp is as late as it likes. If the boundary moved
    for it, a later honest STOP would be discarded exactly as before.
    """
    provider = CaptureProvider()
    # The ordinary message below is mirrored to Chatwoot, which is not what this
    # test is about and is not reachable from here.
    monkeypatch.setattr(worker_module, "_forward_text_to_chatwoot", _no_chatwoot)
    await _deliver(session_maker, monkeypatch, provider, _message(text="STOP", msg_id="wamid.R1C", epoch=BASE_EPOCH))
    # The same wamid, from somebody else: one identifier, two senders.
    await _deliver(
        session_maker,
        monkeypatch,
        provider,
        _message(text="hallo", sender=OTHER_RAW_FROM, msg_id="wamid.R1DUP", epoch=BASE_EPOCH + 60),
    )
    await _deliver(
        session_maker,
        monkeypatch,
        provider,
        _message(text="START", msg_id="wamid.R1DUP", epoch=BASE_EPOCH + 7200),
    )

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    assert row.evidence.get("last_refused_start_reason") == REFUSED_START_IDENTIFIER_CONFLICT
    assert row.last_start_event_at is None

    # And a real opt-in, then a newer STOP, still behave.
    await _deliver(
        session_maker, monkeypatch, provider, _message(text="START", msg_id="wamid.R1OK", epoch=BASE_EPOCH + 1800)
    )
    assert (await _suppression(session_maker)).state == CONSENT_STATE_OPTED_IN
    await _deliver(
        session_maker, monkeypatch, provider, _message(text="STOP", msg_id="wamid.R1S3", epoch=BASE_EPOCH + 2000)
    )
    assert (await _suppression(session_maker)).state == CONSENT_STATE_OPTED_OUT


@pytest.mark.parametrize(
    "stop_offset,expected",
    [
        (900, CONSENT_STATE_OPTED_IN),  # provably older than the opt-in: a replay
        (1800, CONSENT_STATE_OPTED_OUT),  # the same second: a tie refuses
        (2700, CONSENT_STATE_OPTED_OUT),  # newer: applies
    ],
)
async def test_a_stop_is_ordered_against_the_opt_in_in_force(
    session_maker, monkeypatch, stop_offset: int, expected: str
) -> None:
    """One table, three answers, all of them about ``decided_event_at``.

    The opt-in in force happened at 10:30. A STOP from 10:15 is a replay of
    something already answered; one from 10:30 is a tie, and a tie refuses; one
    from 10:45 wins. No rejected attempt is involved anywhere, which is the
    point — the boundary has to be right for the ordinary case too.
    """
    client_id = await _card(session_maker)
    provider = CaptureProvider()
    await _deliver(session_maker, monkeypatch, provider, _message(text="STOP", msg_id="wamid.R1T1", epoch=BASE_EPOCH))
    await _deliver(
        session_maker, monkeypatch, provider, _message(text="START", msg_id="wamid.R1T2", epoch=BASE_EPOCH + 1800)
    )
    await _deliver(
        session_maker,
        monkeypatch,
        provider,
        _message(text="STOP", msg_id="wamid.R1T3", epoch=BASE_EPOCH + stop_offset),
    )

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == expected

    card = await _client_row(session_maker, client_id)
    assert card.wa_opted_out is (expected == CONSENT_STATE_OPTED_OUT)
    assert _marketing_verdict(card) == ("opted_out" if expected == CONSENT_STATE_OPTED_OUT else None)
    async with session_maker() as session:
        guard = await final_send_guard(session, phone=ADDRESS)
    assert guard.allowed is (expected == CONSENT_STATE_OPTED_IN)
    assert guard.reason == (GUARD_ALLOWED if expected == CONSENT_STATE_OPTED_IN else GUARD_SUPPRESSED)


# ===========================================================================
# Round 2, review fix 2 — the card projection cannot be overtaken
# ===========================================================================


def _arm_competitor(session_maker, monkeypatch: pytest.MonkeyPatch, competitor) -> dict[str, Any]:
    """Start *competitor* the first time the projection reads the ledger.

    As a TASK, and then wait until PostgreSQL reports it QUEUED. That wait is
    the assertion: the projection holds this address's consent lock and this
    person's card rows for its whole transaction, so nothing can decide anything
    about them until it commits. Before that, the competitor ran to completion
    inside this window and the outcome was decided by commit order.

    The value handed back is the one from before the competitor was started, so
    the read is as stale as it can be made — and under the locks that no longer
    matters, which is the point.
    """
    real = worker_module.ledger_state
    state: dict[str, Any] = {"armed": True, "stale": None, "task": None}

    async def _patched(session: Any, *, phone: Any) -> Any:
        value = await real(session, phone=phone)
        if state["armed"]:
            state["armed"] = False
            state["stale"] = value
            state["task"] = asyncio.create_task(competitor())
            await _wait_until_a_transaction_is_queued(session_maker)
        return value

    monkeypatch.setattr(worker_module, "ledger_state", _patched)
    return state


async def test_a_competing_decision_cannot_land_inside_the_projection(session_maker, monkeypatch) -> None:
    """The window of review fix 1A, reproduced and shown closed.

    The projection reads the ledger, writes the card and commits. A newer STOP
    committing anywhere in between — a live delivery, or the historical
    reconciliation — used to decide the outcome by commit order alone: the card
    said sendable while the ledger refused, ``compute_excluded_reason`` allowed
    the send, and the acknowledgement confirmed a subscription. Re-reading could
    not close it, because the gap was between the last read and the COMMIT.

    Here the competing STOP is proven to be waiting while the projection is in
    flight, and the state afterwards is the one the ledger holds.
    """
    client_id = await _card(session_maker)
    provider = CaptureProvider()

    await _deliver(session_maker, monkeypatch, provider, _message(text="STOP", msg_id="wamid.R2A", epoch=BASE_EPOCH))
    await _deliver(
        session_maker, monkeypatch, provider, _message(text="START", msg_id="wamid.R2B", epoch=BASE_EPOCH + 1800)
    )
    assert (await _client_row(session_maker, client_id)).wa_opted_out is False

    async def _competing_stop() -> None:
        await _deliver(
            session_maker,
            monkeypatch,
            CaptureProvider(),
            _message(text="STOP", msg_id="wamid.R2RACE", epoch=BASE_EPOCH + 3600),
        )

    state = _arm_competitor(session_maker, monkeypatch, _competing_stop)
    late = CaptureProvider()
    await _deliver(
        session_maker, monkeypatch, late, _message(text="START", msg_id="wamid.R2C", epoch=BASE_EPOCH + 2000)
    )
    assert state["armed"] is False
    assert state["stale"].state == CONSENT_STATE_OPTED_IN
    await asyncio.wait_for(state["task"], timeout=60)

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    card = await _client_row(session_maker, client_id)
    assert card.wa_opted_out is True
    assert _marketing_verdict(card) == "opted_out"
    async with session_maker() as session:
        guard = await final_send_guard(session, phone=ADDRESS)
    assert guard.allowed is False
    assert guard.reason == GUARD_SUPPRESSED


async def test_the_historical_projection_also_queues_behind_it(session_maker, monkeypatch) -> None:
    """The competitor does not have to be a live message.

    ``project(apply=True)`` writes through the same ledger path and now takes
    the same two locks in the same order, so it queues behind the worker's
    projection instead of landing inside it — and it brings the card flag with
    its refusal, which is what keeps the two from disagreeing afterwards.
    """
    from altegio_bot.whatsapp_consent_projection import project

    client_id = await _card(session_maker)
    provider = CaptureProvider()
    await _deliver(session_maker, monkeypatch, provider, _message(text="STOP", msg_id="wamid.R2K", epoch=BASE_EPOCH))
    await _deliver(
        session_maker, monkeypatch, provider, _message(text="START", msg_id="wamid.R2L", epoch=BASE_EPOCH + 1800)
    )

    # A stored delivery nobody ever processed, carrying a refusal newer than the
    # opt-in. The projection is what turns it into a ledger decision.
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppEvent(
                    dedupe_key="wa:historical-stop-r2",
                    status="processed",
                    payload=_payload(_message(text="стоп", msg_id="wamid.R2HIST", epoch=BASE_EPOCH + 5400)),
                    received_at=datetime.now(timezone.utc),
                )
            )

    reports: list[Any] = []

    async def _run_projection() -> None:
        reports.append(await project(session_maker, apply=True))

    state = _arm_competitor(session_maker, monkeypatch, _run_projection)
    await _deliver(
        session_maker,
        monkeypatch,
        CaptureProvider(),
        _message(text="START", msg_id="wamid.R2M", epoch=BASE_EPOCH + 2000),
    )
    assert state["armed"] is False
    await asyncio.wait_for(state["task"], timeout=60)

    assert (await _suppression(session_maker)).state == CONSENT_STATE_OPTED_OUT
    card = await _client_row(session_maker, client_id)
    assert card.wa_opted_out is True
    assert _marketing_verdict(card) == "opted_out"
    assert reports and reports[0].cards_converged >= 1


async def test_the_reverse_schedule_also_settles_on_the_ledger(session_maker, monkeypatch) -> None:
    """The mirror image: a STOP's projection with a proven newer START behind it.

    Serialisation decides this one, and it decides it in order. The START cannot
    land inside the STOP's projection, so the STOP settles first and is
    acknowledged as what it was at that moment — a refusal. The START then
    takes the lock, lifts it, and the card follows. Two decisions, two true
    sentences, in the order they were applied; what cannot happen any more is
    one of them being committed over the other.
    """
    client_id = await _card(session_maker, opted_out=False)
    provider = CaptureProvider()
    await _deliver(session_maker, monkeypatch, provider, _message(text="STOP", msg_id="wamid.R2G", epoch=BASE_EPOCH))
    await _deliver(
        session_maker, monkeypatch, provider, _message(text="START", msg_id="wamid.R2H", epoch=BASE_EPOCH + 1800)
    )
    await _deliver(
        session_maker, monkeypatch, provider, _message(text="STOP", msg_id="wamid.R2I", epoch=BASE_EPOCH + 2000)
    )
    assert (await _suppression(session_maker)).state == CONSENT_STATE_OPTED_OUT

    async def _competing_start() -> None:
        await _deliver(
            session_maker,
            monkeypatch,
            CaptureProvider(),
            _message(text="START", msg_id="wamid.R2RACE3", epoch=BASE_EPOCH + 7200),
        )

    state = _arm_competitor(session_maker, monkeypatch, _competing_start)
    late = CaptureProvider()
    await _deliver(session_maker, monkeypatch, late, _message(text="STOP", msg_id="wamid.R2J", epoch=BASE_EPOCH + 2100))
    await asyncio.wait_for(state["task"], timeout=60)

    assert (await _suppression(session_maker)).state == CONSENT_STATE_OPTED_IN
    card = await _client_row(session_maker, client_id)
    assert card.wa_opted_out is False
    assert _marketing_verdict(card) is None
    # Acknowledged as the refusal it settled as, not as the opt-in that came
    # after it. The one thing ruled out is a sentence that contradicts the
    # record at the moment it is sent.
    assert len(late.sent) == 1
    assert "abgemeldet" in late.sent[0][2]
    async with session_maker() as session:
        guard = await final_send_guard(session, phone=ADDRESS)
    assert guard.allowed is True


async def test_a_permitted_new_start_still_lifts_the_refusal(session_maker, monkeypatch) -> None:
    """The negative half: none of the above may turn into a refusal that sticks."""
    client_id = await _card(session_maker)
    provider = CaptureProvider()
    await _deliver(session_maker, monkeypatch, provider, _message(text="STOP", msg_id="wamid.R2N", epoch=BASE_EPOCH))
    assert (await _client_row(session_maker, client_id)).wa_opted_out is True

    acked = CaptureProvider()
    await _deliver(
        session_maker, monkeypatch, acked, _message(text="START", msg_id="wamid.R2O", epoch=BASE_EPOCH + 1800)
    )

    assert (await _suppression(session_maker)).state == CONSENT_STATE_OPTED_IN
    card = await _client_row(session_maker, client_id)
    assert card.wa_opted_out is False
    assert _marketing_verdict(card) is None
    assert len(acked.sent) == 1
    assert "wieder angemeldet" in acked.sent[0][2]
    async with session_maker() as session:
        guard = await final_send_guard(session, phone=ADDRESS)
    assert guard.allowed is True


async def test_a_failed_acknowledgement_does_not_undo_the_refusal(session_maker, monkeypatch) -> None:
    """§46.4.7 — the projection is committed before the ack is even attempted."""

    class _BrokenProvider(WhatsAppProvider):
        async def send(self, sender_id: int, phone_e164: str, text: str, contact_name: str | None = None) -> str:
            raise RuntimeError("meta is down")

    client_id = await _card(session_maker)
    await _deliver(session_maker, monkeypatch, _BrokenProvider(), _message(text="STOP", msg_id="wamid.R3ACK"))

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    card = await _client_row(session_maker, client_id)
    assert card.wa_opted_out is True
    assert _marketing_verdict(card) == "opted_out"
    async with session_maker() as session:
        event = (await session.execute(select(WhatsAppEvent).order_by(WhatsAppEvent.id))).scalars().first()
    assert event.error is not None and "Ack send failed" in event.error


# ===========================================================================
# Round 2, review fix 5A — one order over the whole set of address locks
# ===========================================================================


async def _wait_until_a_transaction_is_queued(session_maker, *, timeout: float = 30.0) -> None:
    """Block until PostgreSQL shows a transaction waiting for a lock.

    A real wait rather than a sleep: the schedules here have to be exact, and
    "the other transaction is now queued" is a fact this database reports.
    ``pg_locks`` is the only way to observe it from outside, and every lock type
    counts — the orders under test mix transaction-scoped advisory locks with
    row locks on the card rows, and a wait for either one is a wait.
    """
    from sqlalchemy import text as sql_text

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        async with session_maker() as probe:
            waiting = await probe.scalar(sql_text("SELECT count(*) FROM pg_locks WHERE NOT granted"))
        if int(waiting or 0) >= 1:
            return
        assert loop.time() < deadline, "nothing ever queued on a lock"
        await asyncio.sleep(0.05)


async def test_the_worker_takes_the_whole_address_set_in_sorted_order(session_maker, monkeypatch) -> None:
    """The mutual block of review fix 5A, reproduced and shown closed.

    A delivery of [STOP B, STOP A] used to be locked in MESSAGE order, so the
    worker held B and waited for A while an ingress for the same two addresses —
    which has always locked them sorted — held A and waited for B. PostgreSQL
    broke that by aborting one of them, and the one it aborted here loses for
    good: ordinary inbound has no recovery pass, so the event stayed
    ``processing``, no suppression was written, and the receipts stayed
    unresolved.

    Here a third transaction holds A, the first address of the sorted order.
    With the fix the worker is queued on A while holding NOTHING, so the holder
    can go on to take B; without it the worker would already hold B and the two
    would deadlock.
    """
    from altegio_bot.whatsapp_consent import lock_consent_address

    await _seed_sender(session_maker)
    await _ingest(
        session_maker,
        _payload(
            _message(text="STOP", sender=OTHER_RAW_FROM, msg_id="wamid.R5B"),
            _message(text="STOP", sender=RAW_FROM, msg_id="wamid.R5A"),
        ),
        monkeypatch=monkeypatch,
    )
    [event_id] = await _event_ids(session_maker)
    assert ADDRESS < OTHER_ADDRESS, "the sorted order must differ from the message order"

    release = asyncio.Event()
    took_the_second = asyncio.Event()

    async def _holder() -> None:
        async with session_maker() as session:
            async with session.begin():
                await lock_consent_address(session, ADDRESS)
                await release.wait()
                # The whole question: can the holder of the FIRST address still
                # take the second one while the worker is in flight?
                await asyncio.wait_for(lock_consent_address(session, OTHER_ADDRESS), timeout=10)
                took_the_second.set()

    provider = CaptureProvider()
    holder = asyncio.create_task(_holder())
    await asyncio.sleep(0)
    # The whole production path, so a lost race shows up as a stranded event
    # rather than only as an exception: an aborted transaction here leaves the
    # row unprocessed, and ordinary inbound has no recovery pass to pick it up.
    worker = asyncio.create_task(_process(session_maker, provider, event_id))
    await _wait_until_a_transaction_is_queued(session_maker)
    release.set()
    await asyncio.wait_for(took_the_second.wait(), timeout=10)
    await asyncio.wait_for(holder, timeout=10)
    await asyncio.wait_for(worker, timeout=20)

    assert (await _suppression(session_maker, ADDRESS)) is not None
    assert (await _suppression(session_maker, OTHER_ADDRESS)) is not None
    assert [receipt.resolved_at is not None for receipt in await _receipts(session_maker)] == [True, True]
    async with session_maker() as session:
        event = await session.get(WhatsAppEvent, event_id)
    assert event.status == "processed"
    assert event.error is None


async def test_the_ingress_takes_the_same_order_as_the_worker(session_maker, monkeypatch) -> None:
    """The other half of the pair, so the two sides provably agree.

    The receipts are written before the 200, under the same address locks, and
    in the same sorted order. A holder of the first address therefore blocks the
    ingress while the ingress holds nothing, and can still take the second.
    """
    from altegio_bot.whatsapp_consent import lock_consent_address

    await _seed_sender(session_maker)
    release = asyncio.Event()
    took_the_second = asyncio.Event()

    async def _holder() -> None:
        async with session_maker() as session:
            async with session.begin():
                await lock_consent_address(session, ADDRESS)
                await release.wait()
                await asyncio.wait_for(lock_consent_address(session, OTHER_ADDRESS), timeout=10)
                took_the_second.set()

    holder = asyncio.create_task(_holder())
    await asyncio.sleep(0)
    ingress = asyncio.create_task(
        _ingest(
            session_maker,
            _payload(
                _message(text="STOP", sender=OTHER_RAW_FROM, msg_id="wamid.R5IB"),
                _message(text="STOP", sender=RAW_FROM, msg_id="wamid.R5IA"),
            ),
            monkeypatch=monkeypatch,
        )
    )
    await _wait_until_a_transaction_is_queued(session_maker)
    release.set()
    await asyncio.wait_for(took_the_second.wait(), timeout=10)
    await asyncio.wait_for(holder, timeout=10)
    response = await asyncio.wait_for(ingress, timeout=10)

    assert response.status_code == 200
    receipts = await _receipts(session_maker)
    assert sorted(receipt.wa_address for receipt in receipts) == [ADDRESS, OTHER_ADDRESS]


async def test_a_final_guard_and_the_worker_do_not_block_each_other(session_maker, monkeypatch) -> None:
    """The guard locks the same addresses in the same order, so neither waits twice."""
    from altegio_bot.whatsapp_consent import lock_consent_address

    await _seed_sender(session_maker)
    await _ingest(
        session_maker,
        _payload(
            _message(text="STOP", sender=OTHER_RAW_FROM, msg_id="wamid.R5GB"),
            _message(text="STOP", sender=RAW_FROM, msg_id="wamid.R5GA"),
        ),
        monkeypatch=monkeypatch,
    )
    [event_id] = await _event_ids(session_maker)

    release = asyncio.Event()
    took_the_second = asyncio.Event()

    async def _holder() -> None:
        async with session_maker() as session:
            async with session.begin():
                await lock_consent_address(session, ADDRESS)
                await release.wait()
                await asyncio.wait_for(lock_consent_address(session, OTHER_ADDRESS), timeout=10)
                took_the_second.set()

    async def _guard() -> Any:
        async with session_maker() as session:
            async with session.begin():
                return await final_send_guard(session, phone=OTHER_ADDRESS)

    holder = asyncio.create_task(_holder())
    await asyncio.sleep(0)
    worker = asyncio.create_task(apply_inbound_consent(session_maker, event_id=event_id))
    await _wait_until_a_transaction_is_queued(session_maker)
    release.set()
    await asyncio.wait_for(took_the_second.wait(), timeout=10)
    await asyncio.wait_for(holder, timeout=10)
    await asyncio.wait_for(worker, timeout=10)
    verdict = await asyncio.wait_for(_guard(), timeout=10)

    assert verdict.allowed is False
    assert verdict.reason == GUARD_SUPPRESSED


# ===========================================================================
# Round 2, review fix 6 — the verdict and the answer belong to one message
# ===========================================================================

OTHER_PNID = "PNID-REFUSED"


async def _seed_second_sender(session_maker) -> int:
    """A sender row for the REFUSED route, so using it would be possible.

    Without this row the refused route could never produce an outbound message
    anyway and the test would prove nothing: ``_pick_sender`` would find
    nothing and the handler would stop. The point is that the sender exists and
    is still not used.
    """
    async with session_maker() as session:
        async with session.begin():
            existing = await session.scalar(
                select(WhatsAppSender.id).where(WhatsAppSender.phone_number_id == OTHER_PNID)
            )
            if existing is not None:
                return int(existing)
            sender = WhatsAppSender(
                company_id=2,
                sender_code="refused",
                phone_number_id=OTHER_PNID,
                display_phone="+4930000001",
                is_active=True,
            )
            session.add(sender)
            await session.flush()
            return int(sender.id)


async def _allowed_sender_id(session_maker) -> int:
    await _seed_sender(session_maker)
    async with session_maker() as session:
        return int(await session.scalar(select(WhatsAppSender.id).where(WhatsAppSender.phone_number_id == PNID)))


async def _acknowledgements(session_maker) -> list[tuple[str, str]]:
    from altegio_bot.models.models import OutboxMessage

    async with session_maker() as session:
        rows = (
            await session.execute(
                select(OutboxMessage.template_code, OutboxMessage.phone_e164).order_by(OutboxMessage.id)
            )
        ).all()
    return [(str(code), str(phone)) for code, phone in rows]


@pytest.mark.parametrize("order", [("START", "STOP"), ("STOP", "START")])
async def test_one_address_saying_both_things_is_answered_by_the_settled_state(
    session_maker, monkeypatch, order: tuple[str, str]
) -> None:
    """Two commands from one person in one delivery, both permutations.

    The acknowledgement used to be taken from ``actions[0]``, so [START, STOP]
    left the ledger and the card refusing while the customer was told "you are
    subscribed again" — the one thing §46.4.7 rules out by name.

    It now describes the state that SETTLED, and the two permutations settle
    differently on purpose: the second message is a minute later than the first
    and both times are provable, so [START, STOP] ends in a refusal and
    [STOP, START] in a proven opt-in. Either way the sentence the customer gets
    is the one the record supports.
    """
    client_id = await _card(session_maker)
    provider = CaptureProvider()
    await _deliver(
        session_maker,
        monkeypatch,
        provider,
        _message(text=order[0], msg_id="wamid.R6A1", epoch=BASE_EPOCH),
        _message(text=order[1], msg_id="wamid.R6A2", epoch=BASE_EPOCH + 60),
    )

    refused = order[1] == "STOP"
    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == (CONSENT_STATE_OPTED_OUT if refused else CONSENT_STATE_OPTED_IN)
    card = await _client_row(session_maker, client_id)
    assert card.wa_opted_out is refused
    assert _marketing_verdict(card) == ("opted_out" if refused else None)
    assert len(provider.sent) == 1
    if refused:
        assert "abgemeldet" in provider.sent[0][2]
        assert "wieder angemeldet" not in provider.sent[0][2]
        assert await _acknowledgements(session_maker) == [("wa_cmd_stop", ADDRESS)]
    else:
        assert "wieder angemeldet" in provider.sent[0][2]
        assert await _acknowledgements(session_maker) == [("wa_cmd_start", ADDRESS)]


async def test_a_start_the_ledger_refuses_is_never_confirmed_even_beside_a_stop(session_maker, monkeypatch) -> None:
    """The prohibition itself, isolated from the ordering above.

    Both messages arrive in one delivery and the START is the LATER of the two,
    so only its own unprovable authenticity stands between it and an opt-in. It
    is refused, the refusal settles, and the sentence the customer gets is the
    refusal's — never the subscription's.
    """
    client_id = await _card(session_maker)
    provider = CaptureProvider()
    await _deliver(
        session_maker,
        monkeypatch,
        provider,
        _message(text="STOP", msg_id="wamid.R6A3", epoch=BASE_EPOCH),
        _message(text="START", msg_id="wamid.R6A4", epoch=BASE_EPOCH + 60),
        signed=False,
    )

    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    assert row.evidence.get("last_refused_start_reason") == REFUSED_START_UNVERIFIED
    assert (await _client_row(session_maker, client_id)).wa_opted_out is True
    assert len(provider.sent) == 1
    assert "abgemeldet" in provider.sent[0][2]
    assert await _acknowledgements(session_maker) == [("wa_cmd_stop", ADDRESS)]


@pytest.mark.parametrize("refused_first", [True, False])
async def test_a_refused_route_never_carries_the_answer(session_maker, monkeypatch, refused_first: bool) -> None:
    """One address, two routes: a START on the refused one and a STOP on ours.

    The receipts were already right — ``ignored_route`` for the START,
    ``applied`` for the STOP — and the worker filtered by ADDRESS, which has at
    least one accepted message and therefore passed whole. ``actions[0]`` was
    then the refused START, so the positive acknowledgement went out through
    the very sender the deployment had declined to accept.
    """
    monkeypatch.setattr(settings, "whatsapp_allowed_phone_number_ids", PNID)
    refused_sender_id = await _seed_second_sender(session_maker)
    allowed_sender_id = await _allowed_sender_id(session_maker)
    client_id = await _card(session_maker)
    provider = CaptureProvider()

    start = [_message(text="START", msg_id="wamid.R6B1", epoch=BASE_EPOCH + 3600)]
    stop = [_message(text="STOP", msg_id="wamid.R6B2", epoch=BASE_EPOCH)]
    groups = ((OTHER_PNID, start), (PNID, stop)) if refused_first else ((PNID, stop), (OTHER_PNID, start))
    await _deliver_mixed(session_maker, monkeypatch, provider, *groups)

    receipts = await _receipts(session_maker)
    assert sorted((receipt.command, bool(receipt.route_allowed)) for receipt in receipts) == [
        (CONSENT_COMMAND_START, False),
        (CONSENT_COMMAND_STOP, True),
    ]
    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    card = await _client_row(session_maker, client_id)
    assert card.wa_opted_out is True
    assert _marketing_verdict(card) == "opted_out"

    assert len(provider.sent) == 1
    sender_used, phone_used, text_used = provider.sent[0]
    assert sender_used == allowed_sender_id
    assert sender_used != refused_sender_id
    assert phone_used == ADDRESS
    assert "abgemeldet" in text_used
    assert await _acknowledgements(session_maker) == [("wa_cmd_stop", ADDRESS)]


async def test_a_refused_ordinary_message_is_not_mirrored_because_of_an_allowed_neighbour(
    session_maker, monkeypatch
) -> None:
    """An accepted STOP does not make the refused message next to it actionable."""
    monkeypatch.setattr(settings, "whatsapp_allowed_phone_number_ids", PNID)
    await _seed_second_sender(session_maker)
    forwarded: list[str] = []

    async def _spy(session: Any, event: Any, *, phone_e164: str, text: str, **_kwargs: Any) -> None:
        forwarded.append(text)

    monkeypatch.setattr(worker_module, "_forward_text_to_chatwoot", _spy)
    provider = CaptureProvider()
    await _deliver_mixed(
        session_maker,
        monkeypatch,
        provider,
        (OTHER_PNID, [_message(text="ich habe eine Frage", msg_id="wamid.R6C1")]),
        (PNID, [_message(text="STOP", msg_id="wamid.R6C2", epoch=BASE_EPOCH + 60)]),
    )

    assert forwarded == []
    row = await _suppression(session_maker)
    assert row is not None
    assert row.state == CONSENT_STATE_OPTED_OUT
    assert await _acknowledgements(session_maker) == [("wa_cmd_stop", ADDRESS)]


async def test_a_refused_promo_message_does_not_run_the_promo_funnel(session_maker, monkeypatch) -> None:
    """Same property for the promo branch, which has effects of its own."""
    monkeypatch.setattr(settings, "whatsapp_allowed_phone_number_ids", PNID)
    monkeypatch.setattr(settings, "promo_lead_funnel_enabled", True)
    await _seed_second_sender(session_maker)
    promo_calls: list[str] = []

    async def _spy(**kwargs: Any) -> None:
        promo_calls.append(str(kwargs.get("text")))

    monkeypatch.setattr(worker_module, "handle_promo_command", _spy)
    # A keyword the funnel really recognises, read from its own list: a word it
    # ignores would make this test pass for the wrong reason.
    promo_word = sorted(worker_module.PROMO_KEYWORDS)[0]
    assert worker_module._parse_command(promo_word) == "promo"
    provider = CaptureProvider()
    await _deliver_mixed(
        session_maker,
        monkeypatch,
        provider,
        (OTHER_PNID, [_message(text=promo_word, msg_id="wamid.R6D1")]),
        (PNID, [_message(text="STOP", msg_id="wamid.R6D2", epoch=BASE_EPOCH + 60)]),
    )

    assert promo_calls == []
    assert (await _suppression(session_maker)).state == CONSENT_STATE_OPTED_OUT


async def test_a_refused_stop_does_not_execute_because_an_allowed_message_follows_it(
    session_maker, monkeypatch
) -> None:
    """The refusing direction of the same rule, which must not be weakened.

    A STOP on a route this deployment declines is not executed, and an ordinary
    accepted message from the same person in the same delivery does not make it
    executable. The accepted message is still resolved, so nothing is left
    holding sends.
    """
    monkeypatch.setattr(settings, "whatsapp_allowed_phone_number_ids", PNID)
    await _seed_second_sender(session_maker)

    async def _quiet(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(worker_module, "_forward_text_to_chatwoot", _quiet)
    provider = CaptureProvider()
    await _deliver_mixed(
        session_maker,
        monkeypatch,
        provider,
        (OTHER_PNID, [_message(text="STOP", msg_id="wamid.R6E1")]),
        (PNID, [_message(text="danke", msg_id="wamid.R6E2", epoch=BASE_EPOCH + 60)]),
    )

    assert await _suppression(session_maker) is None
    assert provider.sent == []
    receipts = await _receipts(session_maker)
    assert [receipt.resolution for receipt in receipts] == [INBOUND_RESOLUTION_IGNORED_ROUTE, "noop"]
    assert all(receipt.resolved_at is not None for receipt in receipts)
    async with session_maker() as session:
        guard = await final_send_guard(session, phone=ADDRESS)
    assert guard.allowed is True


async def test_an_event_without_receipts_keeps_its_historical_handling(session_maker, monkeypatch) -> None:
    """No per-message provenance means no re-decision against today's routes.

    Every delivery stored before this phase existed is in this shape. Its
    commands are still applied and its acknowledgement still goes out, because
    re-judging it now would mean deciding an old message by configuration it
    never saw.
    """
    await _seed_sender(session_maker)
    client_id = await _card(session_maker)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppEvent(
                    dedupe_key="wa:no-receipts-r6",
                    status="received",
                    payload=_payload(_message(text="STOP", msg_id="wamid.R6F")),
                    received_at=datetime.now(timezone.utc),
                )
            )
    provider = CaptureProvider()
    await _process_pending(session_maker, provider)

    assert (await _suppression(session_maker)).state == CONSENT_STATE_OPTED_OUT
    assert (await _client_row(session_maker, client_id)).wa_opted_out is True
    assert len(provider.sent) == 1
    assert "abgemeldet" in provider.sent[0][2]


# ===========================================================================
# Round 3, review fix 1B — one order over the whole set of card rows
# ===========================================================================

THIRD_ADDRESS = "+4915177991003"
THIRD_RAW_FROM = "4915177991003"


async def _cards(session_maker, *addresses: str) -> dict[str, int]:
    """One Altegio card per address, each with its own numeric id."""
    created: dict[str, int] = {}
    async with session_maker() as session:
        async with session.begin():
            for index, address in enumerate(addresses):
                client = Client(
                    provider="altegio",
                    company_id=1,
                    altegio_client_id=993000 + index,
                    phone_e164=address,
                    display_name=f"Card {index}",
                    raw={},
                    wa_opted_out=False,
                )
                session.add(client)
                await session.flush()
                created[address] = int(client.id)
    return created


async def test_two_deliveries_naming_the_same_people_do_not_deadlock(session_maker, monkeypatch) -> None:
    """The mutual block of review fix 1B, reproduced and shown closed.

    Two deliveries, [STOP A, STOP B, STOP C] and [STOP B, STOP A]. The card
    writes used to happen in COMMAND order inside one transaction each, so the
    first held A and wanted B while the second held B and wanted A. PostgreSQL
    aborted one of them — and that is not a retryable delay here: ordinary
    inbound has no recovery pass, the event stayed ``processing``, and C (named
    by only one of the two deliveries) was left with the ledger refusing and the
    card saying sendable.

    The card rows are now locked for the whole set in one statement, in
    ascending id order, so the second projection queues behind the first. The
    order commands are APPLIED in is untouched.
    """
    await _seed_sender(session_maker)
    cards = await _cards(session_maker, ADDRESS, OTHER_ADDRESS, THIRD_ADDRESS)

    await _ingest(
        session_maker,
        _payload(
            _message(text="STOP", sender=RAW_FROM, msg_id="wamid.R3A1"),
            _message(text="STOP", sender=OTHER_RAW_FROM, msg_id="wamid.R3A2"),
            _message(text="STOP", sender=THIRD_RAW_FROM, msg_id="wamid.R3A3"),
        ),
        monkeypatch=monkeypatch,
    )
    await _ingest(
        session_maker,
        _payload(
            _message(text="STOP", sender=OTHER_RAW_FROM, msg_id="wamid.R3B1"),
            _message(text="STOP", sender=RAW_FROM, msg_id="wamid.R3B2"),
        ),
        monkeypatch=monkeypatch,
    )
    first_event, second_event = await _event_ids(session_maker)

    release = asyncio.Event()
    paused = asyncio.Event()
    real_set = worker_module._set_opt_out
    pending = {"armed": True}

    async def _patched(session: Any, **kwargs: Any) -> int:
        affected = await real_set(session, **kwargs)
        if pending["armed"] and kwargs.get("phone_e164") == ADDRESS:
            pending["armed"] = False
            paused.set()
            await release.wait()
        return affected

    monkeypatch.setattr(worker_module, "_set_opt_out", _patched)

    first = asyncio.create_task(_process(session_maker, CaptureProvider(), first_event))
    await asyncio.wait_for(paused.wait(), timeout=30)
    second = asyncio.create_task(_process(session_maker, CaptureProvider(), second_event))
    # The second projection must be WAITING, not running: without one order over
    # the card rows it would be holding B by now and the two would cross.
    await _wait_until_a_transaction_is_queued(session_maker)
    release.set()

    await asyncio.wait_for(first, timeout=60)
    await asyncio.wait_for(second, timeout=60)

    for address in (ADDRESS, OTHER_ADDRESS, THIRD_ADDRESS):
        row = await _suppression(session_maker, address)
        assert row is not None, address
        assert row.state == CONSENT_STATE_OPTED_OUT, address
        card = await _client_row(session_maker, cards[address])
        assert card.wa_opted_out is True, address
        assert _marketing_verdict(card) == "opted_out", address

    assert all(receipt.resolved_at is not None for receipt in await _receipts(session_maker))
    async with session_maker() as session:
        statuses = [
            str(value)
            for value in (await session.scalars(select(WhatsAppEvent.status).order_by(WhatsAppEvent.id))).all()
        ]
    assert statuses == ["processed", "processed"]


async def test_a_refusal_in_one_delivery_does_not_revive_cancelled_jobs(session_maker, monkeypatch) -> None:
    """A later START must not bring back what a refusal cancelled (§46.4.7)."""
    from altegio_bot.models.models import MessageJob, Record

    cards = await _cards(session_maker, ADDRESS)
    async with session_maker() as session:
        async with session.begin():
            record = Record(company_id=1, altegio_record_id=773000, client_id=cards[ADDRESS], raw={})
            session.add(record)
            await session.flush()
            session.add(
                MessageJob(
                    company_id=1,
                    record_id=int(record.id),
                    client_id=cards[ADDRESS],
                    job_type="review_3d",
                    status="queued",
                    run_at=datetime.now(timezone.utc) + timedelta(days=3),
                    dedupe_key="review_3d:773000",
                    payload={},
                )
            )

    provider = CaptureProvider()
    await _deliver(session_maker, monkeypatch, provider, _message(text="STOP", msg_id="wamid.R3J1"))
    async with session_maker() as session:
        assert (await session.scalars(select(MessageJob.status))).all() == ["canceled"]

    await _deliver(
        session_maker, monkeypatch, provider, _message(text="START", msg_id="wamid.R3J2", epoch=BASE_EPOCH + 3600)
    )

    assert (await _suppression(session_maker)).state == CONSENT_STATE_OPTED_IN
    assert (await _client_row(session_maker, cards[ADDRESS])).wa_opted_out is False
    async with session_maker() as session:
        assert (await session.scalars(select(MessageJob.status))).all() == ["canceled"]


# ===========================================================================
# A historical snapshot is answered only on its OWN number (review round 3)
# ===========================================================================

_SNAPSHOT_UUID = uuid_module.UUID("00000000-0000-4000-8000-00000000abcd")


async def _opted_out_snapshots(session_maker, *addresses: str) -> None:
    """One historical ``CampaignRecipient.is_opted_out`` row per address."""
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                provider=PROVIDER_EASYWEEK,
                campaign_code="snapshot-scope",
                mode="preview",
                company_ids=[322579],
                period_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
                period_end=datetime(2026, 1, 31, tzinfo=timezone.utc),
                status="completed",
            )
            session.add(run)
            await session.flush()
            for index, address in enumerate(addresses):
                session.add(
                    CampaignRecipient(
                        campaign_run_id=run.id,
                        provider=PROVIDER_EASYWEEK,
                        company_id=322579,
                        phone_e164=address,
                        display_name=f"Snapshot {index}",
                        status="skipped",
                        is_opted_out=True,
                    )
                )


async def _proven_opt_in_row(session_maker, *, address: str, decided_at: datetime) -> None:
    """A suppression row that now reads ``opted_in``, bound to the shared uuid.

    Written directly rather than through ``apply_consent_decision`` because the
    question under test is how a READER orders it against a snapshot, and the
    writer's own ordering rules are tested in their own section above.
    """
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppConsentSuppression(
                    wa_address=address,
                    state=CONSENT_STATE_OPTED_IN,
                    consent_version=2,
                    decision_command=CONSENT_COMMAND_START,
                    decision_source=CONSENT_SOURCE_INBOUND,
                    decision_authenticity=INBOUND_AUTHENTICITY_SIGNED,
                    decided_observed_at=decided_at,
                    decided_event_at=decided_at,
                    last_stop_observed_at=decided_at - timedelta(days=10),
                    last_stop_event_at=decided_at - timedelta(days=10),
                    last_start_observed_at=decided_at,
                    last_start_event_at=decided_at,
                    banned_easyweek_customer_uuid=_SNAPSHOT_UUID,
                    banned_uuid_recorded_at=decided_at - timedelta(days=10),
                )
            )


async def test_an_opt_in_on_another_number_does_not_answer_this_snapshot(session_maker) -> None:
    """§46.4.7 — a START lifts the refusal on the number it arrived from.

    The same person, the same uuid, two numbers. They resubscribed on one of
    them; the historical refusal recorded against the other was never
    withdrawn. The reader used to compare every snapshot against a single
    newest-opt-in figure for the person, so one number's resubscribe silently
    answered the other number's refusal.
    """
    await _opted_out_snapshots(session_maker, ADDRESS)
    await _proven_opt_in_row(session_maker, address=OTHER_ADDRESS, decided_at=utcnow() + timedelta(hours=1))
    async with session_maker() as session:
        decision = await consent_decision(session, phone=ADDRESS, easyweek_customer_uuid=_SNAPSHOT_UUID)
    assert decision.suppressed
    assert SOURCE_RECIPIENT_FLAG in decision.sources


async def test_an_opt_in_on_this_number_answers_its_own_snapshot(session_maker) -> None:
    """The control: on its own number a newer proven opt-in still answers it."""
    await _opted_out_snapshots(session_maker, ADDRESS)
    await _proven_opt_in_row(session_maker, address=ADDRESS, decided_at=utcnow() + timedelta(hours=1))
    async with session_maker() as session:
        decision = await consent_decision(session, phone=ADDRESS, easyweek_customer_uuid=_SNAPSHOT_UUID)
    assert not decision.suppressed


async def test_an_opt_in_older_than_its_own_snapshot_answers_nothing(session_maker) -> None:
    await _opted_out_snapshots(session_maker, ADDRESS)
    await _proven_opt_in_row(session_maker, address=ADDRESS, decided_at=utcnow() - timedelta(days=5))
    async with session_maker() as session:
        decision = await consent_decision(session, phone=ADDRESS, easyweek_customer_uuid=_SNAPSHOT_UUID)
    assert decision.suppressed
    assert SOURCE_RECIPIENT_FLAG in decision.sources


async def test_an_opt_in_at_the_same_moment_as_its_snapshot_answers_nothing(session_maker) -> None:
    """A tie is not an order, and an unordered pair falls to the refusal.

    The snapshot's own time cannot be absent — ``campaign_recipients.created_at``
    is NOT NULL — so the unprovable case this rule exists for is a tie, and the
    code's ``undated`` guard stays as defence in depth for any row a future
    schema or import might leave without one.
    """
    await _opted_out_snapshots(session_maker, ADDRESS)
    async with session_maker() as session:
        taken = (
            await session.scalars(select(CampaignRecipient.created_at).where(CampaignRecipient.phone_e164 == ADDRESS))
        ).one()
    await _proven_opt_in_row(session_maker, address=ADDRESS, decided_at=taken)
    async with session_maker() as session:
        decision = await consent_decision(session, phone=ADDRESS, easyweek_customer_uuid=_SNAPSHOT_UUID)
    assert decision.suppressed
    assert SOURCE_RECIPIENT_FLAG in decision.sources


async def test_a_busy_answered_number_cannot_hide_a_live_refusal_elsewhere(session_maker) -> None:
    """The read is per address, so no row limit can push a refusal out of sight.

    The earlier query took the newest fifty snapshots across every matched
    number, ordered by time. Sixty answered snapshots on one number therefore
    filled it completely and the live refusal on the other number was never
    read.
    """
    await _opted_out_snapshots(session_maker, *([OTHER_ADDRESS] * 60), ADDRESS)
    async with session_maker() as session:
        async with session.begin():
            # The refusing number's snapshot is the OLDEST, so a time-ordered
            # limit would reach it last.
            await session.execute(
                sa_update(CampaignRecipient)
                .where(CampaignRecipient.phone_e164 == ADDRESS)
                .values(created_at=utcnow() - timedelta(days=365))
            )
    await _proven_opt_in_row(session_maker, address=OTHER_ADDRESS, decided_at=utcnow() + timedelta(hours=1))
    async with session_maker() as session:
        decision = await consent_decision(session, phone=ADDRESS, easyweek_customer_uuid=_SNAPSHOT_UUID)
    assert decision.suppressed
    assert SOURCE_RECIPIENT_FLAG in decision.sources
