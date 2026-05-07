"""Tests: WhatsApp promo lead funnel.

Covers:
1.  Secret word creates PromoLead with status='issued'.
2.  Reply is free-form text (not a Meta template).
3.  OutboxMessage audit row created with correct fields.
4.  Existing active PromoLead does not create a duplicate.
5.  Existing active PromoLead sends 'already active' text.
6.  Expired PromoLead is marked 'expired'.
7.  Expiration: issued_plus_days mode.
8.  Expiration: calendar_month mode.
9.  STOP/START behaviour still works after promo funnel added.
10. Chatwoot-origin promo event does not create a PromoLead or send a reply.
11. All allowed PROMO_LEAD_STATUSES are defined in the model.
12. rejected_not_new: local prior-visit check creates rejected lead (attendance=1).
13. PromoLead stores company_id from sender.
14. Send failure persists PromoLead with meta.reply_sent=False, no OutboxMessage.
15. Prior visit with attendance=0 and visit_attendance=1 → rejected_not_new.
16. calendar_month customer display shows the last valid day, not the boundary.
17. promo_lead_funnel_enabled=False sends safe info reply, creates no PromoLead.
18. promo_lead_funnel_enabled=True creates PromoLead on secret word.
19. Concurrent race: savepoint detects duplicate, sends already-active reply.
20. Send failure after lead persist marks meta.reply_sent=False, no OutboxMessage.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest
from sqlalchemy import select

from altegio_bot.models.models import (
    PROMO_LEAD_STATUSES,
    Client,
    OutboxMessage,
    PromoLead,
    Record,
    WhatsAppEvent,
    WhatsAppSender,
)
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.settings import settings
from altegio_bot.workers.promo_lead_handler import compute_expires_at
from altegio_bot.workers.whatsapp_inbox_worker import handle_event

# ---------------------------------------------------------------------------
# Autouse fixture: enable funnel for all tests in this module
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _enable_promo_funnel():
    """All tests here exercise the full funnel (promo_lead_funnel_enabled=True)."""
    with patch.object(settings, "promo_lead_funnel_enabled", True):
        yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PHONE_NUMBER_ID = "PNID_PROMO_LEAD"
FROM_PHONE = "4916000000099"
PHONE_E164 = "+4916000000099"
CAMPAIGN = "welcome_discount"  # must match settings default


class _CaptureProvider(WhatsAppProvider):
    wamid = "wamid.PROMO_LEAD_TEST"

    def __init__(self) -> None:
        self.sent: list[tuple[int, str, str]] = []

    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
        contact_name: str | None = None,
    ) -> str:
        self.sent.append((sender_id, phone_e164, text))
        return self.wamid


class _FakeCW:
    async def log_incoming_message(self, phone: str, text: str, contact_name: str | None = None) -> None:
        pass

    async def aclose(self) -> None:
        pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


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
                                    "id": "wamid.INBOUND",
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


async def _setup_sender(session, *, sender_id: int = 301, company_id: int = 1) -> None:
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


async def _fire_promo(session_maker, text: str = "aktion") -> tuple[_CaptureProvider, WhatsAppEvent]:
    """Run handle_event for a promo inbound message. Returns (provider, event)."""
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            await _setup_sender(session)

            evt = WhatsAppEvent(
                dedupe_key=f"wa:promo-lead-{text}-{id(text)}",
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
# 1. Secret word creates PromoLead with status='issued'
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_secret_word_creates_promo_lead_issued(session_maker) -> None:
    await _fire_promo(session_maker, "aktion")

    async with session_maker() as s:
        result = await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))
        lead = result.scalar_one_or_none()

    assert lead is not None, "PromoLead must be created on first promo command"
    assert lead.status == "issued"
    assert lead.campaign_name == CAMPAIGN
    assert lead.phone_e164 == PHONE_E164
    assert lead.discount_amount == Decimal("15")
    assert lead.expires_at > lead.issued_at


# ---------------------------------------------------------------------------
# 2. Reply is free-form text (not a Meta template)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_promo_reply_is_free_form_text(session_maker) -> None:
    provider, evt = await _fire_promo(session_maker, "AKTION!")

    assert provider.sent, "provider.send() must be called — not send_template()"
    _sid, sent_phone, sent_text = provider.sent[0]
    assert sent_phone == PHONE_E164
    # Sent via free-form channel: confirm no template marker in the text
    assert "Rabatt" in sent_text
    assert "Neukunden" in sent_text
    assert evt.error is None


# ---------------------------------------------------------------------------
# 3. OutboxMessage audit row created with correct fields
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_outbox_audit_row_created(session_maker) -> None:
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            await _setup_sender(session, sender_id=302)

            evt = WhatsAppEvent(
                dedupe_key="wa:promo-audit-3",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_inbound_payload(PHONE_NUMBER_ID, FROM_PHONE, "angebot"),
            )
            session.add(evt)
            await session.flush()
            event_id = int(evt.id)

            with patch(
                "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                return_value=_FakeCW(),
            ):
                await handle_event(session, evt, provider)

    async with session_maker() as s:
        result = await s.execute(select(OutboxMessage).where(OutboxMessage.template_code == "wa_promo_lead_issued"))
        outbox = result.scalar_one_or_none()

    assert outbox is not None
    assert outbox.template_code == "wa_promo_lead_issued"
    assert outbox.message_source == "bot"
    assert outbox.status == "sent"
    assert outbox.provider_message_id == _CaptureProvider.wamid
    assert outbox.phone_e164 == PHONE_E164
    assert outbox.language == "de"

    meta = outbox.meta or {}
    assert meta.get("source") == "promo_lead"
    assert meta.get("command") == "promo"
    assert meta.get("campaign_name") == CAMPAIGN
    assert meta.get("whatsapp_event_id") == event_id


# ---------------------------------------------------------------------------
# 4. Existing active PromoLead does not create a duplicate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_duplicate_lead_for_active_promo(session_maker) -> None:
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            await _setup_sender(session, sender_id=308)

            evt1 = WhatsAppEvent(
                dedupe_key="wa:promo-nodup-1",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_inbound_payload(PHONE_NUMBER_ID, FROM_PHONE, "aktion"),
            )
            session.add(evt1)
            await session.flush()

            with patch(
                "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                return_value=_FakeCW(),
            ):
                await handle_event(session, evt1, provider)

            evt2 = WhatsAppEvent(
                dedupe_key="wa:promo-nodup-2",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_inbound_payload(PHONE_NUMBER_ID, FROM_PHONE, "rabatt"),
            )
            session.add(evt2)
            await session.flush()

            with patch(
                "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                return_value=_FakeCW(),
            ):
                await handle_event(session, evt2, provider)

    async with session_maker() as s:
        result = await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))
        leads = result.scalars().all()

    assert len(leads) == 1, "Only one PromoLead must exist for an active promo"
    assert leads[0].status == "issued"
    assert len(provider.sent) == 2
    assert "bereits aktiv" in provider.sent[1][2]


# ---------------------------------------------------------------------------
# 5. Existing active PromoLead sends 'already active' text
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_already_active_lead_sends_already_issued_text(session_maker) -> None:
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            await _setup_sender(session, sender_id=303)

            now = _utcnow()
            session.add(
                PromoLead(
                    company_id=1,
                    phone_e164=PHONE_E164,
                    campaign_name=CAMPAIGN,
                    secret_code="aktion",
                    discount_amount=Decimal("15"),
                    discount_type="fixed",
                    status="issued",
                    issued_at=now,
                    expires_at=now + timedelta(days=30),
                )
            )

            evt = WhatsAppEvent(
                dedupe_key="wa:promo-already-5",
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

    assert provider.sent
    _sid, sent_phone, sent_text = provider.sent[0]
    assert "bereits aktiv" in sent_text
    assert evt.error is None


# ---------------------------------------------------------------------------
# 6. Expired PromoLead is marked 'expired'
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_expired_lead_is_marked_expired(session_maker) -> None:
    provider = _CaptureProvider()
    past = _utcnow() - timedelta(days=1)

    async with session_maker() as session:
        async with session.begin():
            await _setup_sender(session, sender_id=304)

            session.add(
                PromoLead(
                    company_id=1,
                    phone_e164=PHONE_E164,
                    campaign_name=CAMPAIGN,
                    secret_code="aktion",
                    discount_amount=Decimal("15"),
                    discount_type="fixed",
                    status="issued",
                    issued_at=past - timedelta(days=30),
                    expires_at=past,  # already in the past
                )
            )

            evt = WhatsAppEvent(
                dedupe_key="wa:promo-expired-6",
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

    async with session_maker() as s:
        result = await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))
        lead = result.scalar_one_or_none()

    assert lead is not None
    assert lead.status == "expired"
    assert provider.sent
    _sid, _phone, sent_text = provider.sent[0]
    assert "abgelaufen" in sent_text


# ---------------------------------------------------------------------------
# 7. Expiration: issued_plus_days mode
# ---------------------------------------------------------------------------


def test_compute_expires_at_issued_plus_days() -> None:
    issued = datetime(2026, 5, 7, 10, 0, 0, tzinfo=timezone.utc)
    expires = compute_expires_at(issued, "issued_plus_days", 30)
    assert expires == issued + timedelta(days=30)


def test_compute_expires_at_issued_plus_days_zero() -> None:
    issued = datetime(2026, 5, 7, 10, 0, 0, tzinfo=timezone.utc)
    expires = compute_expires_at(issued, "issued_plus_days", 0)
    assert expires == issued


# ---------------------------------------------------------------------------
# 8. Expiration: calendar_month mode
# ---------------------------------------------------------------------------


def test_compute_expires_at_calendar_month_mid_month() -> None:
    issued = datetime(2026, 5, 7, 10, 0, 0, tzinfo=timezone.utc)
    expires = compute_expires_at(issued, "calendar_month", 30)
    assert expires == datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_compute_expires_at_calendar_month_december() -> None:
    issued = datetime(2026, 12, 15, 10, 0, 0, tzinfo=timezone.utc)
    expires = compute_expires_at(issued, "calendar_month", 30)
    assert expires == datetime(2027, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_compute_expires_at_calendar_month_first_day() -> None:
    issued = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    expires = compute_expires_at(issued, "calendar_month", 30)
    assert expires == datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# 9. STOP/START behaviour unchanged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_still_sets_opt_out_after_promo_funnel(session_maker) -> None:
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=305,
                    company_id=1,
                    sender_code="default",
                    phone_number_id=PHONE_NUMBER_ID,
                    display_phone="+49",
                    is_active=True,
                )
            )

            stop_payload = _inbound_payload(PHONE_NUMBER_ID, "10000000001", "STOP")
            evt = WhatsAppEvent(
                dedupe_key="wa:stop-after-promo-9",
                status="received",
                error=None,
                query={},
                headers={},
                payload=stop_payload,
            )
            session.add(evt)
            await session.flush()
            await handle_event(session, evt, provider)

    async with session_maker() as s:
        c = await s.get(Client, 1)
    assert c is not None
    assert c.wa_opted_out is True

    assert provider.sent
    _sid, _phone, sent_text = provider.sent[0]
    assert "abgemeldet" in sent_text.lower()


# ---------------------------------------------------------------------------
# 10. Chatwoot-origin promo event creates no lead and no reply
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chatwoot_origin_promo_no_lead_no_reply(session_maker) -> None:
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=306,
                    company_id=1,
                    sender_code="default",
                    phone_number_id=PHONE_NUMBER_ID,
                    display_phone="+49",
                    is_active=True,
                )
            )

            # dedupe_key starting with 'chatwoot:' triggers _is_chatwoot_origin
            evt = WhatsAppEvent(
                dedupe_key="chatwoot:promo-loop-guard-10",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_inbound_payload(PHONE_NUMBER_ID, FROM_PHONE, "aktion"),
            )
            session.add(evt)
            await session.flush()
            await handle_event(session, evt, provider)

    assert not provider.sent, "No reply must be sent for Chatwoot-origin promo event"
    assert evt.error is None

    async with session_maker() as s:
        result = await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))
        lead = result.scalar_one_or_none()

    assert lead is None, "No PromoLead must be created for Chatwoot-origin event"


# ---------------------------------------------------------------------------
# 11. All PROMO_LEAD_STATUSES are defined
# ---------------------------------------------------------------------------


def test_promo_lead_statuses_complete() -> None:
    required = {
        "issued",
        "booked",
        "applied",
        "used",
        "expired",
        "cancelled",
        "rejected_not_new",
        "rejected_service_not_allowed",
        "apply_failed",
    }
    assert required == PROMO_LEAD_STATUSES


# ---------------------------------------------------------------------------
# 12. rejected_not_new: attendance=1 creates rejected lead
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rejected_not_new_when_client_has_prior_visit(session_maker) -> None:
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            await _setup_sender(session, sender_id=307)

            # Client id=1 has phone "+10000000001" (seeded).
            # Add a Record for that client that shows attendance=1 (visited).
            session.add(
                Record(
                    company_id=1,
                    altegio_record_id=9901,
                    client_id=1,
                    altegio_client_id=1,
                    is_deleted=False,
                    attendance=1,
                    raw={},
                )
            )

            prior_visit_phone = "10000000001"
            prior_visit_e164 = "+10000000001"
            evt = WhatsAppEvent(
                dedupe_key="wa:promo-rejected-12",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_inbound_payload(PHONE_NUMBER_ID, prior_visit_phone, "aktion"),
            )
            session.add(evt)
            await session.flush()

            with patch(
                "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                return_value=_FakeCW(),
            ):
                await handle_event(session, evt, provider)

    async with session_maker() as s:
        result = await s.execute(select(PromoLead).where(PromoLead.phone_e164 == prior_visit_e164))
        lead = result.scalar_one_or_none()

    assert lead is not None
    assert lead.status == "rejected_not_new"
    assert lead.reject_reason == "has_prior_visits"

    assert provider.sent
    _sid, _phone, sent_text = provider.sent[0]
    assert "Neukunden" in sent_text


# ---------------------------------------------------------------------------
# 13. PromoLead stores company_id from sender
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_promo_lead_stores_company_id(session_maker) -> None:
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            await _setup_sender(session, sender_id=309, company_id=42)

            evt = WhatsAppEvent(
                dedupe_key="wa:promo-company-13",
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

    async with session_maker() as s:
        result = await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))
        lead = result.scalar_one_or_none()

    assert lead is not None
    assert lead.company_id == 42


# ---------------------------------------------------------------------------
# 14. Send failure persists PromoLead with meta.reply_sent=False, no OutboxMessage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_failure_marks_lead_reply_not_sent(session_maker) -> None:
    class _FailProvider(WhatsAppProvider):
        async def send(self, sender_id, phone_e164, text, contact_name=None) -> str:
            raise RuntimeError("network error")

    provider = _FailProvider()

    async with session_maker() as session:
        async with session.begin():
            await _setup_sender(session, sender_id=310)

            evt = WhatsAppEvent(
                dedupe_key="wa:promo-sendfail-14",
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

    assert evt.error is not None, "event.error must be set on send failure"

    async with session_maker() as s:
        leads = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))).scalars().all()
        outbox = (
            await s.execute(select(OutboxMessage).where(OutboxMessage.template_code == "wa_promo_lead_issued"))
        ).scalar_one_or_none()

    assert len(leads) == 1, "PromoLead must be persisted even when send fails"
    lead = leads[0]
    assert lead.meta is not None
    assert lead.meta.get("reply_sent") is False, "meta.reply_sent must be False on send failure"
    assert lead.meta.get("reply_send_error") is not None, "meta.reply_send_error must capture the error"
    assert outbox is None, "No OutboxMessage must be created when send fails"


# ---------------------------------------------------------------------------
# 15. Prior visit with attendance=0 and visit_attendance=1 → rejected_not_new
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rejected_not_new_when_visit_attendance_is_one(session_maker) -> None:
    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            await _setup_sender(session, sender_id=311)

            # attendance=0 but visit_attendance=1 — must also be treated as prior visit.
            session.add(
                Record(
                    company_id=1,
                    altegio_record_id=9902,
                    client_id=1,
                    altegio_client_id=1,
                    is_deleted=False,
                    attendance=0,
                    visit_attendance=1,
                    raw={},
                )
            )

            prior_phone = "10000000001"
            prior_e164 = "+10000000001"
            evt = WhatsAppEvent(
                dedupe_key="wa:promo-visit-att-15",
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

    async with session_maker() as s:
        result = await s.execute(select(PromoLead).where(PromoLead.phone_e164 == prior_e164))
        lead = result.scalar_one_or_none()

    assert lead is not None
    assert lead.status == "rejected_not_new", "attendance=0 + visit_attendance=1 must still trigger rejected_not_new"


# ---------------------------------------------------------------------------
# 16. calendar_month display shows the last valid day (not the boundary)
# ---------------------------------------------------------------------------


def test_calendar_month_display_shows_last_valid_day() -> None:
    from altegio_bot.workers.promo_lead_handler import build_reply_issued

    # Technical boundary: 2026-06-01 00:00:00 UTC
    expires_at = datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
    reply = build_reply_issued(
        expires_at=expires_at,
        booking_url="https://example.com/",
        discount_amount=Decimal("15"),
        discount_type="fixed",
    )
    assert "31.05.2026" in reply, "Customer text must show last valid day, not boundary date"
    assert "01.06.2026" not in reply, "Boundary date must not appear in customer text"


def test_issued_plus_days_display_shows_exact_date() -> None:
    from altegio_bot.workers.promo_lead_handler import build_reply_issued

    # issued_plus_days: expires at 15:00 UTC, not midnight — no day subtraction.
    expires_at = datetime(2026, 6, 6, 15, 0, 0, tzinfo=timezone.utc)
    reply = build_reply_issued(
        expires_at=expires_at,
        booking_url="https://example.com/",
        discount_amount=Decimal("15"),
        discount_type="fixed",
    )
    assert "06.06.2026" in reply


# ---------------------------------------------------------------------------
# 17. promo_lead_funnel_enabled=False → safe info reply, no PromoLead
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_funnel_disabled_sends_info_reply_no_lead(session_maker) -> None:
    provider = _CaptureProvider()

    # Override the autouse fixture: disable funnel explicitly for this test.
    with patch.object(settings, "promo_lead_funnel_enabled", False):
        async with session_maker() as session:
            async with session.begin():
                await _setup_sender(session, sender_id=312)

                evt = WhatsAppEvent(
                    dedupe_key="wa:promo-disabled-17",
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

    assert provider.sent, "Info reply must be sent even when funnel is disabled"
    _sid, sent_phone, sent_text = provider.sent[0]
    assert sent_phone == PHONE_E164
    assert sent_text, "Sent text must be non-empty"
    # Must NOT make the discount-is-linked promise
    assert "verknüpft" not in sent_text
    assert evt.error is None

    async with session_maker() as s:
        leads = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))).scalars().all()
        outbox = (
            await s.execute(select(OutboxMessage).where(OutboxMessage.template_code == "wa_promo_info"))
        ).scalar_one_or_none()

    assert not leads, "No PromoLead must be created when funnel is disabled"
    assert outbox is not None, "OutboxMessage with template_code=wa_promo_info must be created"
    assert outbox.message_source == "bot"
    assert outbox.meta.get("source") == "promo_lead"


# ---------------------------------------------------------------------------
# 18. promo_lead_funnel_enabled=True → PromoLead created on secret word
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_funnel_enabled_creates_promo_lead(session_maker) -> None:
    # The autouse fixture already enables the funnel; this test is explicit.
    provider, evt = await _fire_promo(session_maker, "rabatt")

    async with session_maker() as s:
        result = await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))
        lead = result.scalar_one_or_none()

    assert lead is not None, "PromoLead must be created when funnel is enabled"
    assert lead.status == "issued"
    assert evt.error is None


# ---------------------------------------------------------------------------
# 19. Concurrent race: savepoint detects duplicate, sends already-active reply
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_race_condition_savepoint_recovers_to_already_active(session_maker) -> None:
    """Simulate a concurrent winner: _find_any_lead returns None on first call,
    but the savepoint insert hits UniqueConstraint (pre-committed winner).
    The code must recover: rollback savepoint, re-read winner, send already-active
    reply, commit outer transaction cleanly.
    """
    import altegio_bot.workers.promo_lead_handler as _handler

    now = _utcnow()

    # Step 1: Pre-commit the winner lead and the WhatsAppSender.
    async with session_maker() as pre:
        async with pre.begin():
            pre.add(
                WhatsAppSender(
                    id=314,
                    company_id=1,
                    sender_code="default",
                    phone_number_id=PHONE_NUMBER_ID,
                    display_phone="+49",
                    is_active=True,
                )
            )
            pre.add(
                PromoLead(
                    company_id=1,
                    phone_e164=PHONE_E164,
                    campaign_name=CAMPAIGN,
                    secret_code="aktion",
                    discount_amount=Decimal("15"),
                    discount_type="fixed",
                    status="issued",
                    issued_at=now,
                    expires_at=now + timedelta(days=30),
                )
            )

    # Step 2: Patch _find_any_lead to return None on first call (race window),
    #         then call the real implementation on subsequent calls.
    original_find = _handler._find_any_lead
    call_count = 0

    async def _patched_find(sess, phone, campaign):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return None
        return await original_find(sess, phone, campaign)

    provider = _CaptureProvider()

    async with session_maker() as session:
        async with session.begin():
            evt = WhatsAppEvent(
                dedupe_key="wa:promo-race-19",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_inbound_payload(PHONE_NUMBER_ID, FROM_PHONE, "aktion"),
            )
            session.add(evt)
            await session.flush()

            with patch("altegio_bot.workers.promo_lead_handler._find_any_lead", _patched_find):
                with patch(
                    "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                    return_value=_FakeCW(),
                ):
                    await handle_event(session, evt, provider)

    # Outer transaction must commit cleanly — no error.
    assert evt.error is None, "Outer transaction must commit after race recovery"

    # Exactly one lead must exist (the pre-committed winner).
    async with session_maker() as s:
        leads = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))).scalars().all()
    assert len(leads) == 1, "Only the pre-committed winner must exist"

    # Already-active reply must have been sent.
    assert len(provider.sent) == 1
    assert "bereits aktiv" in provider.sent[0][2], "Already-active reply must be sent after race recovery"

    # OutboxMessage audit row must be created for the reply that was actually sent.
    async with session_maker() as s:
        outboxes = (await s.execute(select(OutboxMessage))).scalars().all()
    assert any(
        o.template_code == "wa_promo_lead_already_issued" for o in outboxes
    ), "OutboxMessage for already-issued reply must be created"


# ---------------------------------------------------------------------------
# 20. Send failure after lead persist: meta.reply_sent=False, no OutboxMessage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_failure_after_persist_marks_meta(session_maker) -> None:
    """Lead is persisted via savepoint, then send fails.
    PromoLead must remain with meta.reply_sent=False and the error string.
    No OutboxMessage must be created.
    """

    class _FailProvider(WhatsAppProvider):
        async def send(self, sender_id, phone_e164, text, contact_name=None) -> str:
            raise RuntimeError("downstream timeout")

    provider = _FailProvider()

    async with session_maker() as session:
        async with session.begin():
            await _setup_sender(session, sender_id=315)

            evt = WhatsAppEvent(
                dedupe_key="wa:promo-sendpost-20",
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

    assert evt.error is not None

    async with session_maker() as s:
        leads = (await s.execute(select(PromoLead).where(PromoLead.phone_e164 == PHONE_E164))).scalars().all()
        outboxes = (await s.execute(select(OutboxMessage))).scalars().all()

    assert len(leads) == 1, "PromoLead must be persisted despite send failure"
    meta = leads[0].meta or {}
    assert meta.get("reply_sent") is False
    assert "downstream timeout" in (meta.get("reply_send_error") or "")
    assert not outboxes, "No OutboxMessage must be created when send fails"
