from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy import select

from altegio_bot.models.models import (
    Client,
    MessageJob,
    Record,
    WhatsAppEvent,
    WhatsAppSender,
)
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.workers.whatsapp_inbox_worker import handle_event


class CaptureProvider(WhatsAppProvider):
    def __init__(self) -> None:
        self.sent: list[tuple[int, str, str]] = []

    async def send(self, sender_id: int, phone_e164: str, text: str) -> str:
        self.sent.append((sender_id, phone_e164, text))
        return 'msg-1'


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _payload(phone_number_id: str, from_phone: str, text: str) -> dict[str, Any]:
    return {
        'object': 'whatsapp_business_account',
        'entry': [
            {
                'id': 'WABA',
                'changes': [
                    {
                        'field': 'messages',
                        'value': {
                            'metadata': {'phone_number_id': phone_number_id},
                            'messages': [
                                {
                                    'from': from_phone,
                                    'id': 'wamid.TEST',
                                    'timestamp': '1700000000',
                                    'type': 'text',
                                    'text': {'body': text},
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


@pytest.mark.asyncio
async def test_stop_sets_opt_out_global_and_cancels_jobs(session_maker) -> None:
    provider = CaptureProvider()
    now = _utcnow()
    phone = '+10000000010'

    async with session_maker() as session:
        async with session.begin():
            c10 = await session.get(Client, 10)
            assert c10 is not None
            c10.phone_e164 = phone

            c20 = Client(
                id=20,
                company_id=2,
                altegio_client_id=20,
                display_name='Client 20',
                phone_e164=phone,
                raw={},
            )
            session.add(c20)

            r1 = Record(
                company_id=1,
                altegio_record_id=1001,
                client_id=10,
                altegio_client_id=10,
                raw={},
            )
            r2 = Record(
                company_id=2,
                altegio_record_id=1002,
                client_id=20,
                altegio_client_id=20,
                raw={},
            )
            session.add_all([r1, r2])
            await session.flush()

            session.add(
                WhatsAppSender(
                    id=1,
                    company_id=1,
                    sender_code='default',
                    phone_number_id='PNID',
                    display_phone='+49',
                    is_active=True,
                )
            )

            session.add_all(
                [
                    MessageJob(
                        company_id=1,
                        record_id=r1.id,
                        client_id=10,
                        job_type='review_3d',
                        run_at=now,
                        status='queued',
                        dedupe_key='t1',
                        payload={},
                    ),
                    MessageJob(
                        company_id=2,
                        record_id=r2.id,
                        client_id=20,
                        job_type='repeat_10d',
                        run_at=now,
                        status='queued',
                        dedupe_key='t2',
                        payload={},
                    ),
                ]
            )

            evt = WhatsAppEvent(
                dedupe_key='wa:test-1',
                status='received',
                error=None,
                query={},
                headers={},
                payload=_payload('PNID', '10000000010', 'STOP'),
            )
            session.add(evt)
            await session.flush()

            await handle_event(session, evt, provider)

        c10_after = await session.get(Client, 10)
        c20_after = await session.get(Client, 20)

        assert c10_after is not None
        assert c10_after.wa_opted_out is True

        assert c20_after is not None
        assert c20_after.wa_opted_out is True

        res = await session.execute(
            select(MessageJob).order_by(MessageJob.id.asc())
        )
        jobs = list(res.scalars().all())
        assert [j.status for j in jobs] == ['canceled', 'canceled']

        assert provider.sent
        sender_id, sent_phone, text = provider.sent[0]
        assert sender_id == 1
        assert sent_phone == phone
        assert 'abgemeldet' in text.lower()


@pytest.mark.asyncio
async def test_start_clears_opt_out(session_maker) -> None:
    provider = CaptureProvider()
    phone = '+10000000010'

    async with session_maker() as session:
        async with session.begin():
            c = await session.get(Client, 10)
            assert c is not None
            c.phone_e164 = phone
            c.wa_opted_out = True
            c.wa_opt_out_reason = 'wa:stop'

            session.add(
                WhatsAppSender(
                    id=1,
                    company_id=1,
                    sender_code='default',
                    phone_number_id='PNID',
                    display_phone='+49',
                    is_active=True,
                )
            )

            evt = WhatsAppEvent(
                dedupe_key='wa:test-2',
                status='received',
                error=None,
                query={},
                headers={},
                payload=_payload('PNID', '10000000010', 'START'),
            )
            session.add(evt)
            await session.flush()

            await handle_event(session, evt, provider)

        c2 = await session.get(Client, 10)
        assert c2 is not None
        assert c2.wa_opted_out is False
        assert c2.wa_opted_out_at is None
        assert c2.wa_opt_out_reason is None