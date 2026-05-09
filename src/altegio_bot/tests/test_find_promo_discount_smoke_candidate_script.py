"""Tests: find_promo_discount_smoke_candidate CLI script.

Covers:
1. no_candidates: no data in DB → exit 0, output contains "No promo discount smoke candidates found".
2. finds_candidate: lead + client + record seeded → candidate printed with dry-run command.
3. company_filter: --company-id filters out candidates from another company.
4. phone_filter: --phone filters to matching phone only.
5. expired_lead_ignored: expired PromoLead not returned.
6. deleted_record_ignored: soft-deleted Record excluded; lead without any valid record → no candidate.
7. missing_fields_ignored: leads missing loyalty_card_id / location_id / discount_program_id excluded.
8. read_only_guarantee: after script run, PromoLead status/meta and Record are unchanged.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from altegio_bot.models.models import Client, PromoLead, Record, RecordService
from altegio_bot.scripts.find_promo_discount_smoke_candidate import _build_parser, _run

_UTC = timezone.utc
_FUTURE = datetime(2099, 1, 1, tzinfo=_UTC)
_PAST = datetime(2020, 1, 1, tzinfo=_UTC)
_PHONE = "+4916099887766"
_PHONE2 = "+4916099887700"
_COMPANY = 1
_COMPANY2 = 2
_LOCATION = 9001
_CARD_ID = "555"
_PROGRAM_ID = "dp_001"
_ALTEGIO_RECORD_ID = 77701


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


async def _seed_client(
    session,
    *,
    client_id: int = 200,
    phone: str = _PHONE,
    company_id: int = _COMPANY,
) -> Client:
    c = Client(
        id=client_id,
        company_id=company_id,
        altegio_client_id=client_id,
        phone_e164=phone,
        display_name="Smoke Test",
        raw={},
    )
    session.add(c)
    await session.flush()
    return c


async def _seed_record(
    session,
    *,
    record_id: int = 300,
    altegio_record_id: int = _ALTEGIO_RECORD_ID,
    client_id: int = 200,
    company_id: int = _COMPANY,
    is_deleted: bool = False,
) -> Record:
    r = Record(
        id=record_id,
        company_id=company_id,
        altegio_record_id=altegio_record_id,
        client_id=client_id,
        altegio_client_id=client_id,
        is_deleted=is_deleted,
        raw={},
    )
    session.add(r)
    await session.flush()
    return r


async def _seed_service(
    session,
    *,
    record_id: int = 300,
    service_id: int = 12345,
    title: str = "Haircut",
) -> None:
    session.add(
        RecordService(
            record_id=record_id,
            service_id=service_id,
            title=title,
            raw={},
        )
    )
    await session.flush()


def _make_lead(
    *,
    phone: str = _PHONE,
    company_id: int = _COMPANY,
    status: str = "issued",
    expires_at: datetime = _FUTURE,
    loyalty_card_id: str | None = _CARD_ID,
    location_id: int | None = _LOCATION,
    discount_program_id: str | None = _PROGRAM_ID,
    meta: dict | None = None,
) -> PromoLead:
    return PromoLead(
        company_id=company_id,
        phone_e164=phone,
        campaign_name="welcome_discount",
        secret_code="aktion",
        discount_amount=Decimal("15"),
        discount_type="fixed",
        status=status,
        issued_at=datetime(2026, 1, 1, tzinfo=_UTC),
        expires_at=expires_at,
        loyalty_card_id=loyalty_card_id,
        location_id=location_id,
        discount_program_id=discount_program_id,
        meta=meta if meta is not None else {"loyalty_card_issued": True},
    )


def _parse(*extra: str):
    return _build_parser().parse_args(list(extra))


# ---------------------------------------------------------------------------
# 1. No candidates → exit 0, informative message
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_candidates(session_maker, capsys) -> None:
    args = _parse()
    exit_code = await _run(args, session_factory=session_maker)
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "No promo discount smoke candidates found" in out


# ---------------------------------------------------------------------------
# 2. Finds candidate → dry-run command contains correct IDs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_finds_candidate(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

    args = _parse()
    exit_code = await _run(args, session_factory=session_maker)
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "DRY-RUN COMMAND" in out
    assert f"--location-id {_LOCATION}" in out
    assert f"--card-id {_CARD_ID}" in out
    assert f"--program-id {_PROGRAM_ID}" in out
    assert f"--record-id {_ALTEGIO_RECORD_ID}" in out
    assert "REAL APPLY COMMAND" in out
    assert "DO NOT RUN UNTIL YOU VERIFIED THE IDS" in out


# ---------------------------------------------------------------------------
# 3. Company filter: --company-id excludes candidates from another company
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_company_filter(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session)
            lead = _make_lead(company_id=_COMPANY)
            session.add(lead)
            await session.flush()

    # Request only company 2 — the seeded lead belongs to company 1
    args = _parse("--company-id", str(_COMPANY2))
    exit_code = await _run(args, session_factory=session_maker)
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "No promo discount smoke candidates found" in out


# ---------------------------------------------------------------------------
# 4. Phone filter: --phone includes only matching phone
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_phone_filter(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            # Seed two clients/leads — only _PHONE should match
            await _seed_client(session, client_id=200, phone=_PHONE)
            await _seed_record(session, record_id=300, client_id=200, altegio_record_id=77701)
            lead1 = _make_lead(phone=_PHONE)
            session.add(lead1)

            await _seed_client(session, client_id=201, phone=_PHONE2)
            await _seed_record(session, record_id=301, client_id=201, altegio_record_id=77702)
            lead2 = _make_lead(phone=_PHONE2)
            session.add(lead2)
            await session.flush()

    args = _parse("--phone", _PHONE)
    exit_code = await _run(args, session_factory=session_maker)
    assert exit_code == 0

    out = capsys.readouterr().out
    assert _PHONE in out
    assert _PHONE2 not in out
    assert "77701" in out
    assert "77702" not in out


# ---------------------------------------------------------------------------
# 5. Expired lead is ignored
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_expired_lead_ignored(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session)
            lead = _make_lead(expires_at=_PAST)
            session.add(lead)
            await session.flush()

    args = _parse()
    exit_code = await _run(args, session_factory=session_maker)
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "No promo discount smoke candidates found" in out


# ---------------------------------------------------------------------------
# 6. Deleted record is ignored → lead with only a deleted record is excluded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deleted_record_ignored(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session, is_deleted=True)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

    args = _parse()
    exit_code = await _run(args, session_factory=session_maker)
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "No promo discount smoke candidates found" in out


# ---------------------------------------------------------------------------
# 7. Leads missing required fields are excluded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_loyalty_card_id_excluded(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session)
            lead = _make_lead(loyalty_card_id=None)
            session.add(lead)
            await session.flush()

    args = _parse()
    exit_code = await _run(args, session_factory=session_maker)
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "No promo discount smoke candidates found" in out


@pytest.mark.asyncio
async def test_missing_location_id_excluded(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session)
            lead = _make_lead(location_id=None)
            session.add(lead)
            await session.flush()

    args = _parse()
    exit_code = await _run(args, session_factory=session_maker)
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "No promo discount smoke candidates found" in out


@pytest.mark.asyncio
async def test_missing_discount_program_id_excluded(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session)
            lead = _make_lead(discount_program_id=None)
            session.add(lead)
            await session.flush()

    args = _parse()
    exit_code = await _run(args, session_factory=session_maker)
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "No promo discount smoke candidates found" in out


# ---------------------------------------------------------------------------
# 8. Read-only guarantee: PromoLead and Record unchanged after script run
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_only_guarantee(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()
            lead_id = lead.id

    args = _parse()
    await _run(args, session_factory=session_maker)

    async with session_maker() as session:
        async with session.begin():
            lead_after = (await session.execute(select(PromoLead).where(PromoLead.id == lead_id))).scalar_one()
            record_after = (await session.execute(select(Record).where(Record.id == 300))).scalar_one()

    assert lead_after.status == "issued"
    assert lead_after.meta == {"loyalty_card_issued": True}
    assert record_after.is_deleted is False
