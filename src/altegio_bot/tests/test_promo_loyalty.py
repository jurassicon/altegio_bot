"""Tests: promo loyalty card issuance.

Covers:
23. promo_issue_loyalty_card_enabled=False — Altegio loyalty API NOT called.
24. promo_issue_loyalty_card_enabled=True, successful card issue:
    PromoLead gets all card fields, meta.loyalty_card_issued=True,
    reply contains card number, OutboxMessage created.
25. loyalty API failure — event.error set, meta.loyalty_card_issued=False,
    no card OutboxMessage, basic reply still sent.
26. missing promo_loyalty_card_type_id — fail closed, no live API call.
27. missing promo_discount_program_id — fail closed, no live API call.
28. company_id not in promo_location_id_by_company — fail closed.
29. existing tests not broken (import sanity check via promo_lead_handler).
"""

from __future__ import annotations

import contextlib
from datetime import datetime, timezone
from decimal import Decimal as D
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from altegio_bot.models.models import (
    OutboxMessage,
    PromoLead,
    WhatsAppEvent,
    WhatsAppSender,
)
from altegio_bot.promo_loyalty import AltegioLoyaltyError, LoyaltyCardResult
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.settings import settings
from altegio_bot.workers.whatsapp_inbox_worker import handle_event

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

PHONE_NUMBER_ID = "PNID_LOYALTY"
FROM_PHONE = "4916099887766"
PHONE_E164 = "+4916099887766"
CAMPAIGN = "welcome_discount"

_MOCK_CARD = LoyaltyCardResult(
    altegio_client_id=7001,
    loyalty_card_id="card_555",
    loyalty_card_number="0049160998877660",
    card_type_id="ct_001",
)


@pytest.fixture(autouse=True)
def _enable_promo_funnel():
    with patch.object(settings, "promo_lead_funnel_enabled", True):
        yield


class _CaptureProvider(WhatsAppProvider):
    wamid = "wamid.LOYALTY_TEST"

    def __init__(self) -> None:
        self.sent: list[tuple[int, str, str]] = []

    async def send(self, sender_id: int, phone_e164: str, text: str, contact_name: str | None = None) -> str:
        self.sent.append((sender_id, phone_e164, text))
        return self.wamid


class _FakeCW:
    async def log_incoming_message(self, phone: str, text: str, contact_name: str | None = None) -> None:
        pass

    async def aclose(self) -> None:
        pass


def _inbound_payload(phone_number_id: str, from_phone: str, text: str) -> dict[str, Any]:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {"phone_number_id": phone_number_id},
                            "messages": [
                                {
                                    "from": from_phone,
                                    "id": "wamid.INBOUND_LOYALTY",
                                    "timestamp": "1700000000",
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


async def _setup_sender(session, *, sender_id: int = 401, company_id: int = 1) -> None:
    session.add(
        WhatsAppSender(
            id=sender_id,
            company_id=company_id,
            sender_code="default",
            phone_number_id=PHONE_NUMBER_ID,
            display_phone="+49",
            is_active=True,
        )
    )
    await session.flush()


async def _fire_promo(
    session_maker,
    text: str = "aktion",
    *,
    sender_id: int = 401,
    company_id: int = 1,
    dedupe_suffix: str = "",
) -> tuple[_CaptureProvider, WhatsAppEvent]:
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            await _setup_sender(session, sender_id=sender_id, company_id=company_id)

            evt = WhatsAppEvent(
                dedupe_key=f"wa:loyalty-{text}-{sender_id}{dedupe_suffix}",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_inbound_payload(PHONE_NUMBER_ID, FROM_PHONE, text),
            )
            session.add(evt)
            await session.flush()

            with patch(
                "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                return_value=_FakeCW(),
            ):
                await handle_event(session, evt, provider)

    return provider, evt


# ---------------------------------------------------------------------------
# Helper: context manager patching all loyalty settings + mocked API
# ---------------------------------------------------------------------------


def _loyalty_settings_ctx(**overrides):
    """Patch all required loyalty settings for tests that enable card issuance."""
    defaults = {
        "promo_issue_loyalty_card_enabled": True,
        "promo_altegio_client_api_verified": True,
        "promo_loyalty_card_type_id": "ct_001",
        "promo_discount_program_id": "dp_001",
        "promo_location_id_by_company": '{"1": 9001}',
    }
    defaults.update(overrides)
    patches = [patch.object(settings, k, v) for k, v in defaults.items()]

    @contextlib.contextmanager
    def _stack():
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            yield

    return _stack()


# ---------------------------------------------------------------------------
# 23. promo_issue_loyalty_card_enabled=False — loyalty API not called
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_loyalty_card_disabled_no_api_call(session_maker) -> None:
    mock_issue = AsyncMock(side_effect=RuntimeError("should not be called"))

    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        provider, evt = await _fire_promo(session_maker, "aktion", dedupe_suffix="-23")

    mock_issue.assert_not_called()
    assert evt.error is None
    assert provider.sent, "Basic reply must be sent"

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))).scalar_one_or_none()

    assert lead is not None
    assert lead.loyalty_card_id is None
    assert lead.loyalty_card_number is None
    assert lead.altegio_client_id is None


# ---------------------------------------------------------------------------
# 24. Successful card issue — PromoLead fields, meta, reply, OutboxMessage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_loyalty_card_issue_success(session_maker) -> None:
    mock_issue = AsyncMock(return_value=_MOCK_CARD)

    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        with _loyalty_settings_ctx():
            provider, evt = await _fire_promo(session_maker, "aktion", dedupe_suffix="-24")

    mock_issue.assert_called_once()
    assert evt.error is None

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))).scalar_one_or_none()

    assert lead is not None
    assert lead.status == "issued"
    assert lead.loyalty_card_id == "card_555"
    assert lead.loyalty_card_number == "0049160998877660"
    assert lead.card_type_id == "ct_001"
    assert lead.discount_program_id == "dp_001"
    assert lead.location_id == 9001
    assert lead.altegio_client_id == 7001
    assert lead.meta is not None
    assert lead.meta.get("loyalty_card_issued") is True

    # Reply must contain card number
    assert provider.sent
    _sid, _phone, sent_text = provider.sent[0]
    assert "0049160998877660" in sent_text

    # OutboxMessage with card template created
    async with session_maker() as s:
        outbox = (
            await s.execute(select(OutboxMessage).where(OutboxMessage.template_code == "wa_promo_loyalty_card_issued"))
        ).scalar_one_or_none()

    assert outbox is not None
    assert outbox.status == "sent"
    assert outbox.phone_e164 == PHONE_E164


# ---------------------------------------------------------------------------
# 25. loyalty API failure — event.error, meta, no card OutboxMessage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_loyalty_card_api_failure(session_maker) -> None:
    mock_issue = AsyncMock(side_effect=AltegioLoyaltyError("Altegio 500"))

    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        with _loyalty_settings_ctx():
            provider, evt = await _fire_promo(session_maker, "aktion", dedupe_suffix="-25")

    assert evt.error is not None, "event.error must be set on card API failure"
    assert "Altegio 500" in (evt.error or "")

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))).scalar_one_or_none()

    assert lead is not None
    assert lead.loyalty_card_id is None
    meta = lead.meta or {}
    assert meta.get("loyalty_card_issued") is False
    assert "Altegio 500" in (meta.get("loyalty_card_error") or "")

    # Card-success OutboxMessage must NOT be created
    async with session_maker() as s:
        card_outbox = (
            await s.execute(select(OutboxMessage).where(OutboxMessage.template_code == "wa_promo_loyalty_card_issued"))
        ).scalar_one_or_none()

    assert card_outbox is None, "Card-success OutboxMessage must not be created on API failure"

    # Neutral manual-check reply sent — NOT the "Rabatt verknüpft" promise text
    assert provider.sent, "Neutral reply must be sent on card failure"
    sent_text = provider.sent[0][2]
    assert "0049160998877660" not in sent_text, "Card number must not appear in failure reply"
    assert "manuell" in sent_text, "Neutral manual-check reply expected on card failure"

    # Failure OutboxMessage created with failure template code
    async with session_maker() as s:
        fail_outbox = (
            await s.execute(
                select(OutboxMessage).where(OutboxMessage.template_code == "wa_promo_loyalty_card_issue_failed")
            )
        ).scalar_one_or_none()
    assert fail_outbox is not None, "Failure OutboxMessage must be created on card API failure"


# ---------------------------------------------------------------------------
# 26. missing promo_loyalty_card_type_id — fail closed, no API call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_card_type_id_fails_closed(session_maker) -> None:
    mock_issue = AsyncMock(side_effect=RuntimeError("must not be called"))

    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        with _loyalty_settings_ctx(promo_loyalty_card_type_id=""):
            provider, evt = await _fire_promo(session_maker, "aktion", dedupe_suffix="-26")

    mock_issue.assert_not_called()
    assert evt.error is not None, "event.error must be set when card_type_id missing"
    assert "promo_loyalty_card_type_id" in (evt.error or "")

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))).scalar_one_or_none()

    assert lead is not None
    meta = lead.meta or {}
    assert meta.get("loyalty_card_issued") is False


# ---------------------------------------------------------------------------
# 27. missing promo_discount_program_id — fail closed, no API call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_discount_program_id_fails_closed(session_maker) -> None:
    mock_issue = AsyncMock(side_effect=RuntimeError("must not be called"))

    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        with _loyalty_settings_ctx(promo_discount_program_id=""):
            provider, evt = await _fire_promo(session_maker, "aktion", dedupe_suffix="-27")

    mock_issue.assert_not_called()
    assert evt.error is not None, "event.error must be set when discount_program_id missing"
    assert "promo_discount_program_id" in (evt.error or "")

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))).scalar_one_or_none()

    meta = (lead.meta or {}) if lead else {}
    assert meta.get("loyalty_card_issued") is False


# ---------------------------------------------------------------------------
# 28. company_id not in location mapping — fail closed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_location_mapping_fails_closed(session_maker) -> None:
    mock_issue = AsyncMock(side_effect=RuntimeError("must not be called"))

    # company_id=1 is used in test, but mapping is empty → no location_id found
    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        with _loyalty_settings_ctx(promo_location_id_by_company="{}"):
            provider, evt = await _fire_promo(session_maker, "aktion", dedupe_suffix="-28")

    mock_issue.assert_not_called()
    assert evt.error is not None, "event.error must be set when location_id mapping missing"
    assert "location_id" in (evt.error or "")

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))).scalar_one_or_none()

    meta = (lead.meta or {}) if lead else {}
    assert meta.get("loyalty_card_issued") is False


# ---------------------------------------------------------------------------
# 29. Existing flow not broken when loyalty enabled but new lead is rejected_not_new
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rejected_not_new_skips_card_issuance(session_maker) -> None:
    """rejected_not_new leads must NOT trigger card issuance."""
    from altegio_bot.models.models import Record

    mock_issue = AsyncMock(side_effect=RuntimeError("must not be called"))

    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        with _loyalty_settings_ctx():
            provider = _CaptureProvider()

            async with session_maker() as session:
                async with session.begin():
                    await _setup_sender(session, sender_id=409)

                    session.add(
                        Record(
                            company_id=1,
                            altegio_record_id=9903,
                            client_id=1,
                            altegio_client_id=1,
                            is_deleted=False,
                            attendance=1,
                            raw={},
                        )
                    )

                    prior_phone = "10000000001"
                    evt = WhatsAppEvent(
                        dedupe_key="wa:loyalty-rejected-29",
                        status="received",
                        error=None,
                        query={},
                        headers={},
                        payload=_inbound_payload(PHONE_NUMBER_ID, prior_phone, "aktion"),
                    )
                    session.add(evt)
                    await session.flush()

                    with patch(
                        "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                        return_value=_FakeCW(),
                    ):
                        await handle_event(session, evt, provider)

    mock_issue.assert_not_called()
    assert evt.error is None

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == "+10000000001"))).scalar_one_or_none()

    assert lead is not None
    assert lead.status == "rejected_not_new"
    assert lead.loyalty_card_id is None


# ---------------------------------------------------------------------------
# 30. promo_altegio_client_api_verified=False — card issuance blocked
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_client_api_not_verified_blocks_issuance(session_maker) -> None:
    mock_issue = AsyncMock(side_effect=RuntimeError("must not be called"))

    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        with _loyalty_settings_ctx(promo_altegio_client_api_verified=False):
            provider, evt = await _fire_promo(session_maker, "aktion", dedupe_suffix="-30")

    mock_issue.assert_not_called()
    assert evt.error is not None, "event.error must be set when client API not verified"
    assert "promo_altegio_client_api_verified" in (evt.error or "")

    # Neutral reply sent (not the card-number reply, not the promise text)
    assert provider.sent, "Neutral reply must be sent"
    sent_text = provider.sent[0][2]
    assert "manuell" in sent_text, "Neutral manual-check reply expected"

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))).scalar_one_or_none()

    assert lead is not None
    meta = lead.meta or {}
    assert meta.get("loyalty_card_issued") is False


# ---------------------------------------------------------------------------
# 31. Card issued + WhatsApp send fails → card_message_pending=True, card data preserved
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_card_issued_send_failure_sets_pending(session_maker) -> None:
    mock_issue = AsyncMock(return_value=_MOCK_CARD)

    class _FailProvider(WhatsAppProvider):
        async def send(self, sender_id: int, phone_e164: str, text: str, contact_name: str | None = None) -> str:
            raise RuntimeError("network down")

    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        with _loyalty_settings_ctx():
            provider = _FailProvider()

            async with session_maker() as session:
                async with session.begin():
                    await _setup_sender(session, sender_id=431, company_id=1)

                    evt = WhatsAppEvent(
                        dedupe_key="wa:loyalty-send-fail-31",
                        status="received",
                        error=None,
                        query={},
                        headers={},
                        payload=_inbound_payload(PHONE_NUMBER_ID, FROM_PHONE, "aktion"),
                    )
                    session.add(evt)
                    await session.flush()

                    with patch(
                        "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                        return_value=_FakeCW(),
                    ):
                        await handle_event(session, evt, provider)

    mock_issue.assert_called_once()
    assert evt.error is not None, "event.error must be set on send failure"

    async with session_maker() as s:
        lead = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))).scalar_one_or_none()

    assert lead is not None
    assert lead.loyalty_card_id == "card_555", "Card data must be preserved despite send failure"
    assert lead.loyalty_card_number == "0049160998877660"
    meta = lead.meta or {}
    assert meta.get("card_message_pending") is True, "card_message_pending must be True"
    assert meta.get("reply_sent") is False


# ---------------------------------------------------------------------------
# 32. Retry after send failure (card_message_pending) → card-number reply resent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_sends_card_number_reply(session_maker) -> None:
    """On second secret-word trigger, if card already issued, resend card-number reply."""
    mock_issue = AsyncMock(side_effect=RuntimeError("must not be called on retry"))

    retry_phone = "4916011223344"
    retry_phone_e164 = f"+{retry_phone}"

    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        with _loyalty_settings_ctx():
            provider = _CaptureProvider()

            async with session_maker() as session:
                async with session.begin():
                    await _setup_sender(session, sender_id=432, company_id=1)

                    # Pre-existing issued lead with card already in DB
                    _utc = timezone.utc
                    existing_lead = PromoLead(
                        company_id=1,
                        phone_e164=retry_phone_e164,
                        campaign_name="welcome_discount",
                        secret_code="aktion",
                        discount_amount=D("15"),
                        discount_type="fixed",
                        status="issued",
                        loyalty_card_id="card_999",
                        loyalty_card_number="0049160112233440",
                        card_type_id="ct_001",
                        discount_program_id="dp_001",
                        location_id=9001,
                        issued_at=datetime(2026, 1, 1, tzinfo=_utc),
                        expires_at=datetime(2027, 1, 1, tzinfo=_utc),
                        meta={"card_message_pending": True, "reply_sent": False},
                    )
                    session.add(existing_lead)
                    await session.flush()

                    evt = WhatsAppEvent(
                        dedupe_key="wa:loyalty-retry-32",
                        status="received",
                        error=None,
                        query={},
                        headers={},
                        payload=_inbound_payload(PHONE_NUMBER_ID, retry_phone, "aktion"),
                    )
                    session.add(evt)
                    await session.flush()

                    with patch(
                        "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                        return_value=_FakeCW(),
                    ):
                        await handle_event(session, evt, provider)

    mock_issue.assert_not_called()
    assert evt.error is None

    assert provider.sent, "Reply must be sent on retry"
    sent_text = provider.sent[0][2]
    assert "0049160112233440" in sent_text, "Card number must appear in retry reply"


# ---------------------------------------------------------------------------
# 33. Repair path: existing issued lead without card → card issued on retry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_repair_path_issues_card_for_existing_lead(session_maker) -> None:
    """Existing issued lead with no card → card issued and card-number reply sent."""
    mock_issue = AsyncMock(return_value=_MOCK_CARD)

    repair_phone = "4916055667788"
    repair_phone_e164 = f"+{repair_phone}"

    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        with _loyalty_settings_ctx():
            provider = _CaptureProvider()

            async with session_maker() as session:
                async with session.begin():
                    await _setup_sender(session, sender_id=433, company_id=1)

                    _utc = timezone.utc
                    existing_lead = PromoLead(
                        company_id=1,
                        phone_e164=repair_phone_e164,
                        campaign_name="welcome_discount",
                        secret_code="aktion",
                        discount_amount=D("15"),
                        discount_type="fixed",
                        status="issued",
                        loyalty_card_id=None,
                        loyalty_card_number=None,
                        issued_at=datetime(2026, 1, 1, tzinfo=_utc),
                        expires_at=datetime(2027, 1, 1, tzinfo=_utc),
                    )
                    session.add(existing_lead)
                    await session.flush()

                    evt = WhatsAppEvent(
                        dedupe_key="wa:loyalty-repair-33",
                        status="received",
                        error=None,
                        query={},
                        headers={},
                        payload=_inbound_payload(PHONE_NUMBER_ID, repair_phone, "aktion"),
                    )
                    session.add(evt)
                    await session.flush()

                    with patch(
                        "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                        return_value=_FakeCW(),
                    ):
                        await handle_event(session, evt, provider)

    mock_issue.assert_called_once()
    assert evt.error is None

    async with session_maker() as s:
        stmt = select(PromoLead).where(PromoLead.phone_e164 == repair_phone_e164)
        lead = (await s.execute(stmt)).scalar_one_or_none()

    assert lead is not None
    assert lead.loyalty_card_id == "card_555", "Card ID must be saved on repair"
    assert lead.loyalty_card_number == "0049160998877660"
    meta = lead.meta or {}
    assert meta.get("loyalty_card_issued") is True

    assert provider.sent, "Card-number reply must be sent after repair"
    sent_text = provider.sent[0][2]
    assert "0049160998877660" in sent_text, "Card number must appear in repair reply"
