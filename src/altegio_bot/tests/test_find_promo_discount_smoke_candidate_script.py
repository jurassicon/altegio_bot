"""Tests: find_promo_discount_smoke_candidate CLI script.

Covers:
1.  no_candidates: no data → exit 0, "No promo discount smoke candidates found".
2.  finds_candidate: valid lead+record → dry-run command printed, IDs correct.
3.  output_never_contains_yes_apply: --yes-apply absent from all output.
4.  company_filter: --company-id filters out wrong company.
5.  phone_filter: --phone includes only matching phone.
6.  expired_lead_ignored: expired PromoLead excluded.
7.  deleted_record_ignored: soft-deleted Record excluded.
8.  missing_loyalty_card_id_excluded: lead without loyalty_card_id skipped.
9.  missing_location_id_excluded: lead without location_id skipped.
10. missing_discount_program_id_excluded: lead without discount_program_id skipped.
11. read_only_guarantee: PromoLead and Record unchanged after script run.
12. allowed_service_match_yes: service in allowlist → allowed_service_match=yes.
13. allowed_service_match_no: service not in allowlist → allowed_service_match=no + warning.
14. allowed_service_match_not_configured: empty allowlist → allowed_service_match=not_configured.
15. pagination_finds_candidate_past_empty_leads: valid candidate found even when preceding
    leads (newer created_at) have no matching Record.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest
from sqlalchemy import select

from altegio_bot.models.models import Client, PromoLead, Record, RecordService
from altegio_bot.scripts.find_promo_discount_smoke_candidate import _build_parser, _run
from altegio_bot.settings import settings

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
_ALLOWED_SERVICE = 12345
_OTHER_SERVICE = 99999


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
    service_id: int = _ALLOWED_SERVICE,
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
    created_at: datetime | None = None,
) -> PromoLead:
    kwargs: dict = dict(
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
    if created_at is not None:
        kwargs["created_at"] = created_at
    return PromoLead(**kwargs)


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
# 2. Finds candidate → dry-run command printed, real command is NOT printed
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
    assert "REAL APPLY COMMAND is intentionally not printed" in out
    assert "Record.created_at is not available" in out


# ---------------------------------------------------------------------------
# 3. Output never contains --yes-apply
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_output_never_contains_yes_apply(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session)
            lead = _make_lead()
            session.add(lead)
            await session.flush()

    args = _parse()
    await _run(args, session_factory=session_maker)
    out = capsys.readouterr().out
    assert "--yes-apply" not in out


# ---------------------------------------------------------------------------
# 4. Company filter: --company-id excludes candidates from another company
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

    args = _parse("--company-id", str(_COMPANY2))
    exit_code = await _run(args, session_factory=session_maker)
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "No promo discount smoke candidates found" in out


# ---------------------------------------------------------------------------
# 5. Phone filter: --phone includes only matching phone
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_phone_filter(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
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
# 6. Expired lead is ignored
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
# 7. Deleted record is ignored
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
# 8–10. Leads missing required fields are excluded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_loyalty_card_id_excluded(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session)
            session.add(_make_lead(loyalty_card_id=None))
            await session.flush()

    capsys.readouterr()  # clear previous output
    exit_code = await _run(_parse(), session_factory=session_maker)
    assert exit_code == 0
    assert "No promo discount smoke candidates found" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_missing_location_id_excluded(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session)
            session.add(_make_lead(location_id=None))
            await session.flush()

    capsys.readouterr()  # clear previous output
    exit_code = await _run(_parse(), session_factory=session_maker)
    assert exit_code == 0
    assert "No promo discount smoke candidates found" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_missing_discount_program_id_excluded(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session)
            session.add(_make_lead(discount_program_id=None))
            await session.flush()

    capsys.readouterr()  # clear previous output
    exit_code = await _run(_parse(), session_factory=session_maker)
    assert exit_code == 0
    assert "No promo discount smoke candidates found" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# 11. Read-only guarantee
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

    await _run(_parse(), session_factory=session_maker)

    async with session_maker() as session:
        async with session.begin():
            lead_after = (await session.execute(select(PromoLead).where(PromoLead.id == lead_id))).scalar_one()
            record_after = (await session.execute(select(Record).where(Record.id == 300))).scalar_one()

    assert lead_after.status == "issued"
    assert lead_after.meta == {"loyalty_card_issued": True}
    assert record_after.is_deleted is False


# ---------------------------------------------------------------------------
# 12. Service allowlist: match=yes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_allowed_service_match_yes(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session)
            await _seed_service(session, service_id=_ALLOWED_SERVICE)
            session.add(_make_lead())
            await session.flush()

    with patch.object(settings, "promo_allowed_service_ids", str(_ALLOWED_SERVICE)):
        exit_code = await _run(_parse(), session_factory=session_maker)

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "allowed_service_match=yes" in out


# ---------------------------------------------------------------------------
# 13. Service allowlist: match=no
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_allowed_service_match_no(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session)
            await _seed_service(session, service_id=_OTHER_SERVICE)
            session.add(_make_lead())
            await session.flush()

    with patch.object(settings, "promo_allowed_service_ids", str(_ALLOWED_SERVICE)):
        exit_code = await _run(_parse(), session_factory=session_maker)

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "allowed_service_match=no" in out
    assert "WARNING: This record has no services from PROMO_ALLOWED_SERVICE_IDS" in out


# ---------------------------------------------------------------------------
# 14. Service allowlist: not_configured (empty setting)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_allowed_service_match_not_configured(session_maker, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_client(session)
            await _seed_record(session)
            await _seed_service(session, service_id=_ALLOWED_SERVICE)
            session.add(_make_lead())
            await session.flush()

    with patch.object(settings, "promo_allowed_service_ids", ""):
        exit_code = await _run(_parse(), session_factory=session_maker)

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "allowed_service_match=not_configured" in out
    assert "PROMO_ALLOWED_SERVICE_IDS is empty" in out


# ---------------------------------------------------------------------------
# 15. Pagination: valid candidate found past empty leads
#
# Seeds N_EMPTY leads (with newer created_at) that have no matching Client/Record.
# One valid lead (with older created_at) has a Client and Record.
# With --limit 1 the original limit*3=3 fetch would miss the valid lead if
# N_EMPTY >= 3.  The paginated scan finds it regardless.
# ---------------------------------------------------------------------------

_EMPTY_PHONES = [
    "+4900000000001",
    "+4900000000002",
    "+4900000000003",
    "+4900000000004",
]
_NEWER_TS = datetime(2026, 5, 9, 12, 0, 0, tzinfo=_UTC)
_OLDER_TS = datetime(2026, 5, 1, 12, 0, 0, tzinfo=_UTC)


@pytest.mark.asyncio
async def test_pagination_finds_candidate_past_empty_leads(session_maker, capsys) -> None:
    """Valid candidate is found even when N_EMPTY >= limit*3 leads precede it."""
    async with session_maker() as session:
        async with session.begin():
            # Leads with no Client/Record — will be scanned and skipped
            for phone in _EMPTY_PHONES:
                session.add(_make_lead(phone=phone, created_at=_NEWER_TS))

            # Valid candidate: older created_at so it comes after the empty leads
            # in ORDER BY created_at DESC
            await _seed_client(session)
            await _seed_record(session)
            session.add(_make_lead(created_at=_OLDER_TS))
            await session.flush()

    args = _parse("--limit", "1")
    exit_code = await _run(args, session_factory=session_maker)
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "DRY-RUN COMMAND" in out
    assert f"--location-id {_LOCATION}" in out
    assert f"--record-id {_ALTEGIO_RECORD_ID}" in out
