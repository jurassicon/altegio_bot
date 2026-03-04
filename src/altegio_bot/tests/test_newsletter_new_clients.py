"""Tests for the new-client filtering logic in the monthly newsletter script."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from altegio_bot.models.models import Client, Record
from altegio_bot.scripts.run_newsletter_new_clients_monthly import (
    _fetch_new_clients,
    _format_card_text,
    _prev_month_range,
)

PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 2, 1, tzinfo=timezone.utc)

COMPANY = 758285


# ---------------------------------------------------------------------------
# Unit tests that don't need a database
# ---------------------------------------------------------------------------


def test_prev_month_range_february() -> None:
    ref = datetime(2026, 2, 15, tzinfo=timezone.utc)
    start, end = _prev_month_range(ref)
    assert start == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert end == datetime(2026, 2, 1, tzinfo=timezone.utc)


def test_prev_month_range_january_wraps_to_december() -> None:
    ref = datetime(2026, 1, 5, tzinfo=timezone.utc)
    start, end = _prev_month_range(ref)
    assert start == datetime(2025, 12, 1, tzinfo=timezone.utc)
    assert end == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_format_card_text() -> None:
    assert _format_card_text("0074454347287392") == "Kundenkarte #0074454347287392"


# ---------------------------------------------------------------------------
# Database-backed tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_new_client_with_one_unarrived_record(session_maker) -> None:
    """Client with exactly 1 record in period and attendance != 1 qualifies."""
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=2001,
                display_name="New Client",
                phone_e164="+491234560001",
                raw={},
            )
            session.add(client)
            await session.flush()

            record = Record(
                company_id=COMPANY,
                altegio_record_id=3001,
                client_id=client.id,
                starts_at=PERIOD_START + timedelta(days=5),
                attendance=0,
                visit_attendance=0,
            )
            session.add(record)

        result = await _fetch_new_clients(
            session,
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    assert any(c.altegio_client_id == 2001 for c in result)


@pytest.mark.asyncio
async def test_arrived_client_excluded(session_maker) -> None:
    """Client whose record has attendance == 1 must NOT qualify."""
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=2002,
                display_name="Arrived Client",
                phone_e164="+491234560002",
                raw={},
            )
            session.add(client)
            await session.flush()

            record = Record(
                company_id=COMPANY,
                altegio_record_id=3002,
                client_id=client.id,
                starts_at=PERIOD_START + timedelta(days=5),
                attendance=1,
            )
            session.add(record)

        result = await _fetch_new_clients(
            session,
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    assert not any(c.altegio_client_id == 2002 for c in result)


@pytest.mark.asyncio
async def test_multiple_records_client_excluded(session_maker) -> None:
    """Client with > 1 records in period must NOT qualify."""
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=2003,
                display_name="Returning Client",
                phone_e164="+491234560003",
                raw={},
            )
            session.add(client)
            await session.flush()

            for i, altegio_id in enumerate([3003, 3004]):
                session.add(
                    Record(
                        company_id=COMPANY,
                        altegio_record_id=altegio_id,
                        client_id=client.id,
                        starts_at=PERIOD_START + timedelta(days=i + 1),
                        attendance=0,
                    )
                )

        result = await _fetch_new_clients(
            session,
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    assert not any(c.altegio_client_id == 2003 for c in result)


@pytest.mark.asyncio
async def test_opted_out_client_excluded(session_maker) -> None:
    """Client with wa_opted_out=True must NOT qualify."""
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=2004,
                display_name="Opted Out",
                phone_e164="+491234560004",
                raw={},
                wa_opted_out=True,
            )
            session.add(client)
            await session.flush()

            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=3005,
                    client_id=client.id,
                    starts_at=PERIOD_START + timedelta(days=2),
                    attendance=0,
                )
            )

        result = await _fetch_new_clients(
            session,
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    assert not any(c.altegio_client_id == 2004 for c in result)


@pytest.mark.asyncio
async def test_record_outside_period_not_counted(session_maker) -> None:
    """A record outside the period should not affect qualification."""
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=2005,
                display_name="Old Client",
                phone_e164="+491234560005",
                raw={},
            )
            session.add(client)
            await session.flush()

            # Record INSIDE period — qualifies
            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=3006,
                    client_id=client.id,
                    starts_at=PERIOD_START + timedelta(days=3),
                    attendance=0,
                )
            )
            # Record OUTSIDE period (previous month)
            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=3007,
                    client_id=client.id,
                    starts_at=PERIOD_START - timedelta(days=30),
                    attendance=0,
                )
            )

        result = await _fetch_new_clients(
            session,
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    # The extra record is outside the period, so client still qualifies
    assert any(c.altegio_client_id == 2005 for c in result)
