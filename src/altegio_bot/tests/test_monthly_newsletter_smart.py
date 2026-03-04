"""Tests for run_monthly_newsletter_smart helper functions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from altegio_bot.models.models import Client, Record
from altegio_bot.scripts.run_monthly_newsletter_smart import (
    CandidateInfo,
    _compute_candidates,
    _compute_summary,
    _format_table,
    _parse_company_ids,
    _parse_period,
)

PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 2, 1, tzinfo=timezone.utc)
COMPANY = 758285


# ---------------------------------------------------------------------------
# _parse_period
# ---------------------------------------------------------------------------


def test_parse_period_month() -> None:
    start, end = _parse_period("2026-01", None, None)
    assert start == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert end == datetime(2026, 2, 1, tzinfo=timezone.utc)


def test_parse_period_month_december() -> None:
    start, end = _parse_period("2025-12", None, None)
    assert start == datetime(2025, 12, 1, tzinfo=timezone.utc)
    assert end == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_parse_period_from_to() -> None:
    start, end = _parse_period(None, "2026-01-01", "2026-01-31")
    assert start == datetime(2026, 1, 1, tzinfo=timezone.utc)
    # --to is inclusive, so end = Jan 31 + 1 day = Feb 1
    assert end == datetime(2026, 2, 1, tzinfo=timezone.utc)


def test_parse_period_month_overrides_from_to() -> None:
    start, end = _parse_period("2026-02", "2026-01-01", "2026-01-31")
    assert start == datetime(2026, 2, 1, tzinfo=timezone.utc)
    assert end == datetime(2026, 3, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# _parse_company_ids
# ---------------------------------------------------------------------------


def test_parse_company_ids_single() -> None:
    assert _parse_company_ids("758285") == [758285]


def test_parse_company_ids_multi() -> None:
    assert _parse_company_ids("758285,1271200") == [758285, 1271200]


def test_parse_company_ids_all() -> None:
    assert _parse_company_ids("all") is None


def test_parse_company_ids_all_upper() -> None:
    assert _parse_company_ids("ALL") is None


# ---------------------------------------------------------------------------
# _compute_summary
# ---------------------------------------------------------------------------


def _make_candidate(**kwargs: object) -> CandidateInfo:
    defaults: dict = dict(
        company_id=1,
        client_id=1,
        altegio_client_id=1,
        display_name="Test",
        phone_e164="+491234560001",
        total_records_in_period=1,
        arrived_records_in_period=0,
        is_opted_out=False,
        is_eligible=True,
        excluded_reason=None,
    )
    defaults.update(kwargs)
    return CandidateInfo(**defaults)  # type: ignore[arg-type]


def test_compute_summary_all_eligible() -> None:
    candidates = [_make_candidate(client_id=i, altegio_client_id=i) for i in range(3)]
    s = _compute_summary(candidates)
    assert s["total_clients_seen"] == 3
    assert s["candidates_count"] == 3
    assert s["excluded_opted_out"] == 0


def test_compute_summary_with_exclusions() -> None:
    candidates = [
        _make_candidate(
            client_id=1,
            altegio_client_id=1,
            is_eligible=False,
            excluded_reason="opted_out",
            is_opted_out=True,
        ),
        _make_candidate(
            client_id=2,
            altegio_client_id=2,
            is_eligible=False,
            excluded_reason="has_arrived",
        ),
        _make_candidate(
            client_id=3,
            altegio_client_id=3,
            is_eligible=False,
            excluded_reason="more_than_one_record",
        ),
        _make_candidate(
            client_id=4,
            altegio_client_id=4,
            is_eligible=False,
            excluded_reason="no_phone",
            phone_e164=None,
        ),
        _make_candidate(client_id=5, altegio_client_id=5),
    ]
    s = _compute_summary(candidates)
    assert s["total_clients_seen"] == 5
    assert s["candidates_count"] == 1
    assert s["excluded_opted_out"] == 1
    assert s["excluded_has_arrived"] == 1
    assert s["excluded_more_than_one_record"] == 1
    assert s["excluded_no_phone"] == 1


# ---------------------------------------------------------------------------
# _format_table
# ---------------------------------------------------------------------------


def test_format_table_empty() -> None:
    result = _format_table([])
    assert "no clients" in result


def test_format_table_contains_phone() -> None:
    c = _make_candidate(phone_e164="+381638400431")
    result = _format_table([c])
    assert "+381638400431" in result


def test_format_table_shows_excluded_reason() -> None:
    c = _make_candidate(
        is_eligible=False,
        excluded_reason="opted_out",
        is_opted_out=True,
    )
    result = _format_table([c])
    assert "opted_out" in result


# ---------------------------------------------------------------------------
# _compute_candidates (requires DB via session_maker fixture)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compute_candidates_eligible_client(session_maker) -> None:
    """Client with exactly 1 unarrived record qualifies."""
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=8001,
                display_name="Smart Test Client",
                phone_e164="+491110000001",
                raw={},
            )
            session.add(client)
            await session.flush()

            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=9001,
                    client_id=client.id,
                    starts_at=PERIOD_START + timedelta(days=5),
                    attendance=0,
                    visit_attendance=0,
                )
            )

        result = await _compute_candidates(
            session,
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    eligible = [c for c in result if c.altegio_client_id == 8001]
    assert len(eligible) == 1
    assert eligible[0].is_eligible is True
    assert eligible[0].total_records_in_period == 1
    assert eligible[0].arrived_records_in_period == 0


@pytest.mark.asyncio
async def test_compute_candidates_arrived_excluded(session_maker) -> None:
    """Client with attended record is excluded (has_arrived)."""
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=8002,
                display_name="Arrived Client",
                phone_e164="+491110000002",
                raw={},
            )
            session.add(client)
            await session.flush()

            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=9002,
                    client_id=client.id,
                    starts_at=PERIOD_START + timedelta(days=3),
                    attendance=1,
                )
            )

        result = await _compute_candidates(
            session,
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    excluded = [c for c in result if c.altegio_client_id == 8002]
    assert len(excluded) == 1
    assert excluded[0].is_eligible is False
    assert excluded[0].excluded_reason == "has_arrived"
    assert excluded[0].arrived_records_in_period == 1


@pytest.mark.asyncio
async def test_compute_candidates_multi_record_excluded(session_maker) -> None:
    """Client with > 1 records is excluded (more_than_one_record)."""
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=8003,
                display_name="Regular Client",
                phone_e164="+491110000003",
                raw={},
            )
            session.add(client)
            await session.flush()

            for i, rid in enumerate([9003, 9004]):
                session.add(
                    Record(
                        company_id=COMPANY,
                        altegio_record_id=rid,
                        client_id=client.id,
                        starts_at=PERIOD_START + timedelta(days=i + 1),
                        attendance=0,
                    )
                )

        result = await _compute_candidates(
            session,
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    excluded = [c for c in result if c.altegio_client_id == 8003]
    assert len(excluded) == 1
    assert excluded[0].is_eligible is False
    assert excluded[0].excluded_reason == "more_than_one_record"
    assert excluded[0].total_records_in_period == 2


@pytest.mark.asyncio
async def test_compute_candidates_opted_out_excluded(session_maker) -> None:
    """Client with wa_opted_out=True is excluded."""
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=8004,
                display_name="Opted Out",
                phone_e164="+491110000004",
                raw={},
                wa_opted_out=True,
            )
            session.add(client)
            await session.flush()

            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=9005,
                    client_id=client.id,
                    starts_at=PERIOD_START + timedelta(days=2),
                    attendance=0,
                )
            )

        result = await _compute_candidates(
            session,
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    excluded = [c for c in result if c.altegio_client_id == 8004]
    assert len(excluded) == 1
    assert excluded[0].is_eligible is False
    assert excluded[0].excluded_reason == "opted_out"


@pytest.mark.asyncio
async def test_compute_candidates_visit_attendance_excluded(
    session_maker,
) -> None:
    """Client with visit_attendance==1 is also counted as arrived."""
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=8005,
                display_name="Visit Attended",
                phone_e164="+491110000005",
                raw={},
            )
            session.add(client)
            await session.flush()

            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=9006,
                    client_id=client.id,
                    starts_at=PERIOD_START + timedelta(days=4),
                    attendance=0,
                    visit_attendance=1,
                )
            )

        result = await _compute_candidates(
            session,
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    excluded = [c for c in result if c.altegio_client_id == 8005]
    assert len(excluded) == 1
    assert excluded[0].is_eligible is False
    assert excluded[0].excluded_reason == "has_arrived"


@pytest.mark.asyncio
async def test_compute_candidates_no_phone_excluded(session_maker) -> None:
    """Eligible client without phone is excluded (no_phone)."""
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=8006,
                display_name="No Phone",
                phone_e164=None,
                raw={},
            )
            session.add(client)
            await session.flush()

            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=9007,
                    client_id=client.id,
                    starts_at=PERIOD_START + timedelta(days=1),
                    attendance=0,
                )
            )

        result = await _compute_candidates(
            session,
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    excluded = [c for c in result if c.altegio_client_id == 8006]
    assert len(excluded) == 1
    assert excluded[0].is_eligible is False
    assert excluded[0].excluded_reason == "no_phone"


@pytest.mark.asyncio
async def test_compute_candidates_outside_period_not_counted(
    session_maker,
) -> None:
    """Records outside the period do not affect eligibility."""
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=8007,
                display_name="Outside Period",
                phone_e164="+491110000007",
                raw={},
            )
            session.add(client)
            await session.flush()

            # One record INSIDE period
            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=9008,
                    client_id=client.id,
                    starts_at=PERIOD_START + timedelta(days=3),
                    attendance=0,
                )
            )
            # One record OUTSIDE period (before)
            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=9009,
                    client_id=client.id,
                    starts_at=PERIOD_START - timedelta(days=10),
                    attendance=1,  # arrived, but outside period
                )
            )

        result = await _compute_candidates(
            session,
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    cands = [c for c in result if c.altegio_client_id == 8007]
    assert len(cands) == 1
    # arrived record is outside period, so client qualifies
    assert cands[0].is_eligible is True
    assert cands[0].total_records_in_period == 1
    assert cands[0].arrived_records_in_period == 0
