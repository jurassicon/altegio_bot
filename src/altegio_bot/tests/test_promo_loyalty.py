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
        "promo_loyalty_card_type_id": "ct_001",
        "promo_discount_program_id": "dp_001",
        "promo_location_id_by_company": '{"1": 9001}',
    }
    defaults.update(overrides)
    return [patch.object(settings, k, v) for k, v in defaults.items()]


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

    patches = _loyalty_settings_ctx()
    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        with patches[0], patches[1], patches[2], patches[3]:
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

    patches = _loyalty_settings_ctx()
    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        with patches[0], patches[1], patches[2], patches[3]:
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

    # Basic reply is still sent (without card number)
    assert provider.sent, "Basic promo reply must still be sent on card failure"
    sent_text = provider.sent[0][2]
    assert "0049160998877660" not in sent_text, "Card number must not appear in failure reply"


# ---------------------------------------------------------------------------
# 26. missing promo_loyalty_card_type_id — fail closed, no API call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_card_type_id_fails_closed(session_maker) -> None:
    mock_issue = AsyncMock(side_effect=RuntimeError("must not be called"))

    patches = _loyalty_settings_ctx(promo_loyalty_card_type_id="")
    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        with patches[0], patches[1], patches[2], patches[3]:
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

    patches = _loyalty_settings_ctx(promo_discount_program_id="")
    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        with patches[0], patches[1], patches[2], patches[3]:
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
    patches = _loyalty_settings_ctx(promo_location_id_by_company="{}")
    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        with patches[0], patches[1], patches[2], patches[3]:
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

    patches = _loyalty_settings_ctx()
    with patch("altegio_bot.workers.promo_lead_handler.issue_promo_loyalty_card", mock_issue):
        with patches[0], patches[1], patches[2], patches[3]:
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
