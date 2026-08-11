from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.models.models import PROVIDER_EASYWEEK, EasyWeekEvent, MessageJob
from altegio_bot.ops import router as ops_router
from altegio_bot.settings import settings

pytestmark = pytest.mark.asyncio


async def test_ops_easyweek_has_one_status_row_per_registry_location(
    session_maker: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    first_id, second_id = 999701, 999702
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "durlach": {
                    "location_id": first_id,
                    "location_uuid": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeee1",
                    "meta_template_prefix": "du",
                    "booking_page_url": "https://booking.example.invalid/durlach",
                },
                "rastatt": {
                    "location_id": second_id,
                    "location_uuid": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeee2",
                    "meta_template_prefix": "ra",
                    "booking_page_url": "https://booking.example.invalid/rastatt",
                },
            }
        ),
        raising=False,
    )
    monkeypatch.setattr(ops_router, "SessionLocal", session_maker)

    async with session_maker() as session:
        async with session.begin():
            session.add_all(
                [
                    EasyWeekEvent(status="captured", payload={"location_id": first_id}),
                    EasyWeekEvent(status="failed", payload={"location_id": first_id}),
                    EasyWeekEvent(status="processed", payload={"location_id": second_id}),
                    MessageJob(
                        provider=PROVIDER_EASYWEEK,
                        company_id=first_id,
                        job_type="record_created",
                        run_at=datetime.now(timezone.utc),
                        status="queued",
                        dedupe_key="ops-easyweek-first",
                        payload={},
                    ),
                    MessageJob(
                        provider=PROVIDER_EASYWEEK,
                        company_id=second_id,
                        job_type="record_updated",
                        run_at=datetime.now(timezone.utc),
                        status="done",
                        dedupe_key="ops-easyweek-second",
                        payload={},
                    ),
                ]
            )

    html = await ops_router.ops_easyweek()
    assert str(first_id) in html and "durlach" in html
    assert "captured=1" in html and "failed=1" in html and "queued=1" in html
    assert str(second_id) in html and "rastatt" in html
    assert "processed=1" in html and "done=1" in html
